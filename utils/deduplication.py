import hashlib, json, logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from core.database import Database

logger = logging.getLogger(__name__)

class DeduplicationEngine:
    """Prevents duplication of data insertion in timescale DB.
    Uses fingerprinting and time-bucketing for efficient deduplication."""

    def __init__(self,db:Database):
        self.db=db
        self.bucket_minutes = 5,
        self.cache ={}
        self.cache_ttl = 3600

    def generate_fingerprint(self, data: Dict[str, Any], bucket_time: bool = True) -> str:
        """Generate unique fingerprint for data."""

        key_parts = []
        key_parts.append(str(data.get('db_id','')))
        key_parts.append(str(data.get('table_name','')))
        key_parts.append(str(data.get('event_type', '')))

        timestamp = data.get('timestamp') or data.get('executed_at') or data.get('recorded_at') or data.get('measured_at')
        if timestamp:
            if bucket_time:
                bucketed = self._bucket_timestamp(timestamp)
                key_parts.append(bucketed)
            else:
                key_parts.append(str(timestamp))

        if data.get('query_hash'):
            key_parts.append(str(data['query_hash']))

        if data.get('index_name'):
            key_parts.append(str(data['index_name']))

        if data.get('column_name'):
            key_parts.append(str(data['column_name']))

    # create a hash
        key_string = ":".join(key_parts)
        fingerprint = hashlib.sha256(key_string.encode('utf-8')).hexdigest()

        logger.debug(f"Generated fingerprint: {fingerprint[:16]}... from {key_string[:100]}")
        return fingerprint

    def _bucket_timestamp(self, timestamp) -> str:
        """Rounding timestamp to the nearest bucket interval"""
        if isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except:
                timestamp = datetime.utcnow()

        minutes = (timestamp.minute // self.bucket_minutes) *self.bucket_minutes
        bucketed = timestamp.replace(minute=minutes, second = 0, microsecond =0)

        return bucketed.isoformat()

    async def alread_exists(self,fingerprint: str, hypertable:str, lookback_hours: int=1) -> bool:
        """checks if fingerprint exists in hypertable with lookback time
        returns True if fingerprint exists else False"""
        cache_key = f"{hypertable}:{fingerprint}"
        if cache_key in self.cache:
            cache_time,exists = self.cache[cache_key]
            if (datetime.utcnow() - cache_time).seconds < self.cache_ttl:
                logger.debug(f"Cache hit for fingerprint: {fingerprint[:16]}...")
                return exists

        time_column = self._get_time_column(hypertable)

        query = f"""SELECT EXISTS (
        SELECT 1 FROM _agentic.{hypertable} WHERE fingerprint =$1 AND {time_column}> NOW() - INTERVAL '{lookback_hours}' hours);"""
        
        try:
            exists = await self.db.fetchval_ts_db(query,fingerprint)
            self.cache[cache_key] = (datetime.utcnow(), exists)

            logger.debug(f"Fingerprint {'exists' if exists else 'not found'}: {fingerprint[:16]}..")
            return exists
        except Exception as e:
            logger.error(f"Error checking fingerprint existence: {e}")
            return False
        
    def _get_time_column(self, hypertable: str) -> str:
        """Get the primary time column name for a hypertable."""
        time_columns ={
            'schema_metadata': 'captured_at',
            'query_performance': 'executed_at',
            'index_analytics': 'measured_at',
            'table_statistics': 'recorded_at',
            'semantic_relationships': 'discovered_at',
            'system_health': 'timestamp',
            'data_quality_metrics': 'measured_at',
            'agent_actions': 'executed_at'
        }

        return time_columns.get(hypertable,'timestamp')

    async def get_last_sync_time(self,db_id:str, hypertable: str) ->Optional[datetime]:
        """Get timestamp of last successful sync for a specific database and hypertable"""
        time_column = self._get_time_column(hypertable)
        query = f"""SELECT MAX({time_column}) FROM _agentic.{hypertable} WHERE db_id = $1;"""

        try:
            last_time = await self.db.fetchval_ts_db(query,db_id)
            if last_time:
                logger.info(f"Last sync for {hypertable}: {last_time}")
            return last_time

        except Exception as e:
            logger.error(f"Error getting last sync time: {e}")
            return None
    
    async def should_insert(self,data:Dict[str,Any],hypertable: str, lookback_hours: int =1) -> tuple[bool,str]:
        """see if data should be inserted or not.."""
        fingerprint = self.generate_fingerprint(data)
        exists = await self.alread_exists(fingerprint, hypertable,lookback_hours)
        return (not exists, fingerprint)

    async def mark_inserted(self,fingerprint:str, hypertable:str):
        """mark the fingerprint inserted in cache and call this after successfull insertion."""

        cache_key = f"{hypertable}:{fingerprint}"
        self.cache[cache_key] = (datetime.utcnow(),True)
        logger.debug(f"Marked as insertd: {fingerprint[:16]}...")

    def clear_cache(self):
        "Clearing in-memory fingerprint cache"
        self.cache.clear()
        logger.info("deduplication cache got cleared...")

    def set_bucket_interval(self,minutes: str):
        """set the interval in minutes"""
        if minutes<1 or minutes> 60:
            raise ValueError("Bucket interval must be between 1 and 60 minutes")
        self.bucket_minutes = minutes
        logger.info(f"Time bucket interval is set to {minutes} mins...")

    async def cleanup_old_cache(self):
        now = datetime.utcnow()
        expired = []
        for key, (timestamp,_) in self.cache.items():
            if (now -timestamp).seconds > self.cache_ttl:
                expired.append(key)

        for key in expired :
            del self.cache[key]

        if expired:
            logger.info(f"Cleaed up {len(expired)} expired cache entries")