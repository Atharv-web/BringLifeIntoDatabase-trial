import logging,sys, json
from pathlib import Path
from datetime import datetime
from typing import Optional

from asyncpg import TransactionIntegrityConstraintViolationError

class CustomFormatter(logging.Formatter):
    """Custom formatter for structured outputs."""

    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m'        # Reset
    }

    def format(self,record):
        levelname= record.levelname
        if levelname in self.COLORS:
            record.levelname = f"{self.COLORS[levelname]}{levelname}{self.COLORS['RESET']}"

        record.asctime = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')
        return super().format(record)

    
class AgentLogger:
    """Centralized logging for the database system.
    Logs to both console and fike with different formats."""

    _instance =None

    def __new__(cls):
        if cls._instance is None:
            cls._instance =super().__new__(cls)
            cls._instance._initialized = False

            return cls._instance
    
    def __init__(self):
        if self._initialized:
            return

        self._initialized = True
        self.loggers = {}

    def setup_logger(self, name:str, log_file:Optional[str]= None,level: str = "INFO",console :bool = True) -> logging.Logger:
        """setup a logger for a specific component"""

        if name in self.loggers:
            return self.loggers[name]
        
        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, level.upper()))
        logger.propagate = False

        logger.handlers.clear()

        if console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(getattr(logging, level.upper()))
            console_format = CustomFormatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s')
            
            console_handler.setFormatter(console_format)
            logger.addHandler(console_handler)

        if log_file:
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)

            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.DEBUG)
            
            file_format = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
            file_handler.setFormatter(file_format)
            logger.addHandler(file_handler)

        self.loggers[name] = logger
        return logger

    def get_logger (self, name: str) -> logging.Logger:
        """Get an existing logger or create a new one."""
        if name not in self.loggers:
            return self.setup_logger(name)
        return self.loggers[name]

class StructuredLogger:
    """
    Structured JSON logging for agent actions and events.
    Used for audit trails and analysis.
    """ 
    def __init__(self,log_file:str):
        self.log_file = Path(log_file)
        self.log_file.parent.mkdir(parents=True, exist_ok=True)

    def log_event(self,event_type: str, data: dict):
        """Log a structured event as JSON.."""
        log_entry = {"timestamp":datetime.utcnow().isoformat(),"event_type": event_type,"data":data}
        try:
            with open(self.log_file, 'a') as f:
                f.write(json.dumps(log_entry) + '\n')
        except Exception as e:
            print(f"Failed to write structured log: {e}")

    def log_agent_action(self, agent_name: str, action: str, details: dict,success: bool):
        """Log an agent action + outcome"""

        self.log_event('agent_action',{
            'agent':agent_name,
            'action':action,
            'details': details,
            'success':success
        })

    def log_query_execution(self, query: str, execution_time_ms: float, success: bool, error: Optional[str] = None):
        """Log sql query execution.."""
        self.log_event('query_execution',{
            'query':query[:200],
            'execution_time_ms': execution_time_ms,
            'success':success,
            'error':error
        })

    def log_recommendation(self, agent_name: str, recommendation_type: str, details: dict, confidence: float):
        """Log agent recommendations.."""
        self.log_event('recommendation',{
            'agent':agent_name,
            'type':recommendation_type,
            'details':details,
            'confidence': confidence,
        })

_agent_logger = AgentLogger()

def get_logger(name:str, log_file:Optional[str] = None, level:str = 'INFO'):
    """Convenience function to get logger"""
    return _agent_logger.setup_logger(name,log_file,level)

def get_structured_logger(log_file:str="logs/structured.jsonl") -> StructuredLogger:
    """get structured logger for audit trails."""
    return StructuredLogger(log_file)