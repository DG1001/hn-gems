#!/usr/bin/env python3
"""Initialize the database with tables and indexes."""

import os
import sys
from datetime import datetime

# Add parent directory to path to import our modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask  
from hn_hidden_gems.config import config
from hn_hidden_gems.models.database import init_db
from hn_hidden_gems.utils.logger import setup_logger

logger = setup_logger(__name__)

def create_app():
    """Create Flask app for database initialization."""
    app = Flask(__name__)
    
    # Load configuration
    config_name = os.environ.get('FLASK_ENV', 'development')
    app.config.from_object(config[config_name])
    
    # Set SQLAlchemy database URI
    app.config['SQLALCHEMY_DATABASE_URI'] = app.config['DATABASE_URL']
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    
    return app

def main():
    """Initialize database."""
    logger.info("Starting database initialization...")
    
    app = create_app()
    
    try:
        with app.app_context():
            # Initialize database
            init_db(app)
            logger.info("Database initialized successfully!")
            
            # Print database location
            db_url = app.config['SQLALCHEMY_DATABASE_URI']
            if db_url.startswith('sqlite:///'):
                db_path = db_url.replace('sqlite:///', '')
                if not db_path.startswith('/'):
                    db_path = os.path.abspath(db_path)
                logger.info(f"SQLite database created at: {db_path}")
            else:
                logger.info(f"Database URL: {db_url}")
            
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()