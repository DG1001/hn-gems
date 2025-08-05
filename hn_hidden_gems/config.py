import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Base configuration."""
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key-change-in-production'
    DATABASE_URL = os.environ.get('DATABASE_URL') or f'sqlite:///{os.path.join(os.path.dirname(os.path.dirname(__file__)), "instance", "hn_hidden_gems.db")}'
    
    # API Keys
    OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
    
    # Redis
    REDIS_URL = os.environ.get('REDIS_URL') or 'redis://localhost:6379/0'
    
    # Email Settings
    SMTP_SERVER = os.environ.get('SMTP_SERVER')
    SMTP_PORT = int(os.environ.get('SMTP_PORT', 587))
    SMTP_USERNAME = os.environ.get('SMTP_USERNAME')
    SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD')
    SMTP_FROM = os.environ.get('SMTP_FROM')
    
    # Webhooks
    DISCORD_WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL')
    SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK_URL')
    
    # Application Settings
    KARMA_THRESHOLD = int(os.environ.get('KARMA_THRESHOLD', 100))
    MIN_INTEREST_SCORE = float(os.environ.get('MIN_INTEREST_SCORE', 0.3))
    POLL_INTERVAL_SECONDS = int(os.environ.get('POLL_INTERVAL_SECONDS', 60))
    MAX_POSTS_PER_POLL = int(os.environ.get('MAX_POSTS_PER_POLL', 100))
    
    # Background Collection Settings
    POST_COLLECTION_INTERVAL_MINUTES = int(os.environ.get('POST_COLLECTION_INTERVAL_MINUTES', 5))
    POST_COLLECTION_ENABLED = POST_COLLECTION_INTERVAL_MINUTES > 0
    POST_COLLECTION_BATCH_SIZE = int(os.environ.get('POST_COLLECTION_BATCH_SIZE', 25))
    POST_COLLECTION_MAX_STORIES = int(os.environ.get('POST_COLLECTION_MAX_STORIES', 500))
    
    # Logging
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    LOG_FILE = os.environ.get('LOG_FILE', 'logs/app.log')
    
    # Hacker News API
    HN_API_BASE = "https://hacker-news.firebaseio.com/v0"

class DevelopmentConfig(Config):
    """Development configuration."""
    DEBUG = True
    TESTING = False

class ProductionConfig(Config):
    """Production configuration."""
    DEBUG = False
    TESTING = False

class TestingConfig(Config):
    """Testing configuration."""
    DEBUG = True
    TESTING = True
    DATABASE_URL = 'sqlite:///:memory:'

config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}