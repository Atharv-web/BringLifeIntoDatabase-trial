import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
load_dotenv()

class Settings:
    """Configuration settings for the database..."""

    def __init__(self):
        # Source Database (Postgres)
        self.SOURCE_DB_HOST = os.getenv('SOURCE_DB_HOST', 'localhost')
        self.SOURCE_DB_PORT = int(os.getenv('SOURCE_DB_PORT', 5432))
        self.SOURCE_DB_NAME = os.getenv('SOURCE_DB_NAME')
        self.SOURCE_DB_USER = os.getenv('SOURCE_DB_USER')
        self.SOURCE_DB_PASSWORD = os.getenv('SOURCE_DB_PASSWORD')
        
        # TimescaleDB (Meta Database)
        self.META_DB_HOST = os.getenv('META_DB_HOST', 'localhost')
        self.META_DB_PORT = int(os.getenv('META_DB_PORT', 5433))
        self.META_DB_NAME = os.getenv('META_DB_NAME', 'agentic_meta')
        self.META_DB_USER = os.getenv('META_DB_USER')
        self.META_DB_PASSWORD = os.getenv('META_DB_PASSWORD')
        
        # Database ID
        self.DB_ID = os.getenv('DB_ID')
        
        # Agent Configuration
        self.MONITORING_ENABLED = os.getenv('MONITORING_ENABLED', 'true').lower() == 'true'
        self.MONITORING_FREQUENCY = int(os.getenv('MONITORING_FREQUENCY', 60))
        
        self.PERFORMANCE_ENABLED = os.getenv('PERFORMANCE_ENABLED', 'true').lower() == 'true'
        self.PERFORMANCE_SLOW_THRESHOLD_MS = int(os.getenv('PERFORMANCE_SLOW_THRESHOLD_MS', 500))
        
        self.INDEXING_ENABLED = os.getenv('INDEXING_ENABLED', 'true').lower() == 'true'
        self.INDEXING_FREQUENCY = int(os.getenv('INDEXING_FREQUENCY', 3600))
        
        self.SEMANTIC_ENABLED = os.getenv('SEMANTIC_ENABLED', 'true').lower() == 'true'
        self.SEMANTIC_FREQUENCY = int(os.getenv('SEMANTIC_FREQUENCY', 86400))
        
        # AI Configuration
        self.OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
        self.AI_MODEL = os.getenv('AI_MODEL', 'gpt-4')
        self.AI_ENABLED = bool(self.OPENAI_API_KEY)
        
        # Logging
        self.LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
        self.LOG_FILE = os.getenv('LOG_FILE', 'logs/agent.log')
        self.STRUCTURED_LOG_FILE = os.getenv('STRUCTURED_LOG_FILE', 'logs/structured.jsonl')
        
        # Deduplication
        self.DEDUP_BUCKET_MINUTES = int(os.getenv('DEDUP_BUCKET_MINUTES', 5))
        self.DEDUP_LOOKBACK_HOURS = int(os.getenv('DEDUP_LOOKBACK_HOURS', 1))
        
        # Event Channels
        self.CHANNEL_MONITORING = 'monitoring_events'
        self.CHANNEL_PERFORMANCE = 'performance_events'
        self.CHANNEL_SEMANTIC = 'semantic_events'
        self.CHANNEL_APPROVAL = 'approval_events'
        
        # Validate required settings
        self._validate()        

    def _validate(self):
        """validate required configuration."""
        required = [
            ('SOURCE_DB_NAME', self.SOURCE_DB_NAME),
            ('SOURCE_DB_USER', self.SOURCE_DB_USER),
            ('SOURCE_DB_PASSWORD', self.SOURCE_DB_PASSWORD),
            ('META_DB_USER', self.META_DB_USER),
            ('META_DB_PASSWORD', self.META_DB_PASSWORD),
            ('DB_ID', self.DB_ID)
        ]

        missing = [name for name, value in required if not value]

        if missing:
            raise ValueError(f"Missing required env variables:{', '.join(missing)}")

    @property
    def POSTGRES_URL(self) -> str:
        return (
            f"postgresql://{self.SOURCE_DB_USER}:{self.SOURCE_DB_PASSWORD}"
            f"@{self.SOURCE_DB_HOST}:{self.SOURCE_DB_PORT}/{self.SOURCE_DB_NAME}"
        )

    @property
    def TIMESCALE_URL(self) -> str:
        return (
            f"postgresql://{self.META_DB_USER}:{self.META_DB_PASSWORD}"
            f"@{self.META_DB_HOST}:{self.META_DB_PORT}/{self.META_DB_NAME}"
        )

    def get_agent_config(self, agent_name: str) -> dict:
        """Get configuration for a specific agent."""
        configs = {
            'monitoring': {
                'enabled': self.MONITORING_ENABLED,
                'frequency': self.MONITORING_FREQUENCY,
                'channel': self.CHANNEL_MONITORING
            },
            'performance': {
                'enabled': self.PERFORMANCE_ENABLED,
                'slow_threshold_ms': self.PERFORMANCE_SLOW_THRESHOLD_MS,
                'channel': self.CHANNEL_PERFORMANCE
            },
            'indexing': {
                'enabled': self.INDEXING_ENABLED,
                'frequency': self.INDEXING_FREQUENCY,
                'channel': self.CHANNEL_PERFORMANCE
            },
            'semantic': {
                'enabled': self.SEMANTIC_ENABLED,
                'frequency': self.SEMANTIC_FREQUENCY,
                'channel': self.CHANNEL_SEMANTIC
            }
        }
        return configs.get(agent_name.lower(), {})

    def ensure_log_directory(self):
        """create log directory if it doesnt exist."""
        log_dir = Path(self.LOG_FILE).parent
        log_dir.mkdir(parents=True,exist_ok=True)

        structured_log_dir = Path(self.STRUCTURED_LOG_FILE)
        structured_log_dir.mkdir(parents= True, exist_ok = True)

settings = Settings()
POSTGRES_URL = settings.POSTGRES_URL
TIMESCALE_URL = settings.TIMESCALE_URL
DB_ID = settings.DB_ID
