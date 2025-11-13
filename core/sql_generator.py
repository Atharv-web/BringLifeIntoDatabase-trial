import sqlparse, re
from typing import Dict,Optional,Any
import logging

logger = logging.getLogger(__name__)


class SQLGenerator:
    """
    Generates safe, deterministic SQL queries using pattern-based templates.
    Ensures agents never hallucinate risky or invalid SQL.
    """

    ALLOWED_COMMANDS = {"SELECT", "INSERT", "UPDATE", "CREATE INDEX", "VACUUM", "ANALYZE"}
    FORBIDDEN_KEYWORDS = ["DROP","TRUNCATE","DELETE","GRANT","REVOKE","SHUTDOWN","ALTER"]

    ALLOWED_HYPERTABLES = {
        'schema_metadata',
        'query_performance',
        'index_analytics',
        'table_statistics',
        'semantic_relationships',
        'system_health',
        'data_quality_metrics',
        'agent_actions'
    }

    def __init__(self):
        # Predefined templates for frequent patterns
        self.templates: Dict[str, str] = {
            # postgres source database queries
            "slow_queries": """
                SELECT query, mean_exec_time, calls, queryid
                FROM pg_stat_statements
                WHERE mean_exec_time > $1
                ORDER BY mean_exec_time DESC
                LIMIT $2;
            """,

            "table_stats": """
            SELECT schemaname, relname, AS table_name, n_live_tup AS live_rows, n_dead_tup AS dead_rows, last_vacuum, last_autovacuum, last_analyze
            FROM pg_stat_user_tables WHERE schemaname = $1;
            """,

            "index_usage": """
                SELECT schemaname, relname AS table_name, idx_scan AS index_scans, indexrelname AS index_name, idx_tup_read, idx_tup_fetch
                FROM pg_stat_user_indexes WHERE schemaname = $1 ORDER BY idx_scan DESC;
            """,
            
            "system_health": """ SELECT COUNT(*) FILTER (WHERE state = 'active') AS active_connections,
            COUNT(*) FILTER (WHERE state = 'idle') as idle_connections,
            COUNT(*) FILTER (WHERE wait_event IS NOT NULL') as waiting_queries
            FROM pg_stat_activity;
            """,

            "table_sizes":"""SELECT schemaname, tablename,
            pg_total_relation_size(schemanme|| '.' || tablename) as total_bytes,
            pg_relation_size(schemaname|| '.' ||tablename) as table_bytes,
            pg_indexes_size(schemaname|| '.' ||tablename) as index_bytes
            FROM pg_tables WHERE schemaname= $1;""",

            "check_index_exists":"""SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname =$1 AND tablename = $2 AND indexname = $3);""",

            "get_table_columns":"""SELECT column_name, data_type, is_nullable FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position;""",

            "get_foreign_keys":"""SELECT 
            tc.constraint_name,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name,
            FROM information_schema.table_constraints as tc
            JOIN information_schema.key_column_usage as kcu
            ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage as ccu
            ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_name = 'FOREIGN KEY'
            AND tc.table_schema = $1
            AND tc.table_name = $2;""",

            "create_index": """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS {index_name}
                ON {schema}.{table_name}({columns});
            """,

            "vacuum_table": """VACUUM ANALYZE {schema}.{table_name};""",

            "analyze_table":"""ANALYZE {schema}.{table_name};""",

            # TIMESCALE DB INSERTS

            "insert_query_performance":"""
            INSERT INTO _agentic.query_performance (
            executed_at, db_id, query_hash, query_text, execution_time_ms, rows_returned, calls, user_name, application_name, error_occured) 
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10);""",
            
            "insert_system_health":"""
            INSERT INTO _agentic.system_health (
            timestamp, db_id, cpu_usage, memory_usage, active_connections, idle_connections, waiting_queries) 
            VALUES ($1,$2,$3,$4,$5,$6,$7);""",
            
            "insert_table_statistics":"""
            INSERT INTO _agentic.table_statistics (
            recorded_at, db_id, table_name, schema_name, total_rows, live_rows, dead_rows, table_size_bytes, index_size_bytes, last_vacuum, last_analyze, seq_scans, index_scans) 
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13);""",
            
            "insert_index_analytics":"""
            INSERT INTO _agentic.index_analytics (measured_at, db_id, table_name, index_name, index_type, columns, size_bytes, scans, tuples_read, tuples_fetched, effectiveness_score)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11);""",
            
            "insert_semantic_relationships":"""
            INSERT INTO _agentic.semantic_relationshps (executed_at, db_id, agent_name, action_type, action_details, sql_executed, success, impact_score)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8);""",

            "insert_agent_action": """
                INSERT INTO _agentic.agent_actions (
                executed_at, db_id, agent_name, action_type, action_details, sql_executed,
                success, impact_score, performance_delta, rollback_available, rollback_sql
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8);""",

            "insert_data_quality":"""
            INSERT INTO _agentic.data_quality_metrics(
            measured_at, db_id, table_name,column_name,null_count,null_percentage,distant_count,cardinality_ratio, anomaly_score)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9);"""
        }

    def generate(self, template_name: str, **kwargs) -> str:
        """Generate SQL from a predefined template."""
        if template_name not in self.templates:
            raise ValueError(f"Unknown SQL template: {template_name}")

        template =  self.templates[template_name]
        
        if '$' in template:
            return template.strip()

        for key,value in kwargs.items():
            if key in ['table_name','column_name','columns','index_name','schema']:
                if not self._is_valid_identifier(str(value)):
                    raise ValueError(f"Invalid SQL Identifier for {key}: {value}")

        sql = template.format(**kwargs)
        
        if not self.is_safe(sql):
            raise ValueError("Unsafe SQL detected. Operation blocked.")
        return sql.strip()

    def is_safe(self, sql: str) -> bool:
        """Validate SQL syntax and ensure no dangerous commands are used."""
        if not sql or not sql.strip():
            return False
        
        parsed = sqlparse.parse(sql)
        if not parsed:
            return False

        if len(parsed)>1:
            logger.warning("Multiple SQL statements detected kiddo")
            return False

        statement = parsed[0]
        first_token = statement.token_first(skip_cm=True,skip_ws=True)
        if not first_token:
            return False

        command = first_token.value.upper()

        if not any(cmd in command for cmd in self.ALLOWED_COMMANDS):
            logger.warning(f"Command not allowed :{command}")
            return False
        
        sql_upper = sql.upper()
        
        for keyword in self.FORBIDDEN_KEYWORDS:
            if keyword in sql_upper:
                logger.warning(f"Forbidden keyword detected: {keyword}")
                return False

        if 'INSERT' in command or 'UPDATE' in command:
            table = self._extract_table_name(sql)
            if table and not self._is_allowed_hypertable(table):
                logger.warning(f"Table not in whitelist: {table}")
                return False
        
        return True
    
    def _is_valid_identifier(self,name:str) -> bool:
        """Validate SQL identifier (table/column/index name)."""

        if not name:
            return False
        
        pattern -= r'^[a-zA-Z_][a-zA-Z0-9_\.]*$'

        if not re.match(pattern,name):
            return False
        
        if len(name)> 63:
            return False
        
        return True
        
    def _extract_table_name(self,sql:str) -> Optional[str]:
        """"""
        match = re.search(r'INSERT\s+INTO\s+([^\s(]+)', sql, re.IGNORECASE)
        if match:
            table = match.group(1)
            if '.' in table:
                _, table = table.split('.',1)
            return table
        
        match = re.search(r'UPDATE\s+([^\s]+)', sql, re.IGNORECASE)
        if match:
            table = match.group(1)
            if '.' in table:
                _, table = table.split('.',1)
            return table
        
        return None

    def _is_allowed_hypertable(self, table_name: str) -> bool:
        """Check if table is in the allowed hypertables list."""
        return table_name in self.ALLOWED_HYPERTABLES

    def get_parameterized_query(self, template_name: str) -> str:
        """Get a parameterized query template (for use with execute)."""
        if template_name not in self.templates:
            raise ValueError(f"Unknown template: {template_name}")
        return self.templates[template_name].strip()

    def build_select(self, table: str, columns: str = "*", where: str = None, limit: int = None, schema:str = "public") -> str:
        """Builds a safe SELECT statement."""
        if not self._is_valid_identifier(table):
            raise ValueError(f"Invalid Table name: {table}")

        if not self._is_valid_identifier(schema):
            raise ValueError(f"Invalid Schema name: {schema}")
        
        sql = f"SELECT {columns} FROM {schema}.{table}"
        
        if where:
            sql += f" WHERE {where}"
        
        if limit:
            sql += f" LIMIT {limit}"

        sql += ";"
        
        if not self.is_safe(sql):
            raise ValueError("Generated Sql query failed safety check.")
        return sql
