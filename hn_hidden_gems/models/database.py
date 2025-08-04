from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import event
from sqlalchemy.engine import Engine
import sqlite3

db = SQLAlchemy()

@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    """Enable foreign key constraints and optimize SQLite performance."""
    if isinstance(dbapi_connection, sqlite3.Connection):
        cursor = dbapi_connection.cursor()
        # Enable foreign key constraints
        cursor.execute("PRAGMA foreign_keys=ON")
        # Optimize performance
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA cache_size=1000")
        cursor.execute("PRAGMA temp_store=MEMORY")
        cursor.close()

def init_db(app):
    """Initialize database with Flask app."""
    db.init_app(app)
    
    with app.app_context():
        # Create all tables
        db.create_all()
        
        # Create indexes
        create_indexes()

def create_indexes():
    """Create database indexes for better performance."""
    # Posts table indexes
    db.engine.execute('CREATE INDEX IF NOT EXISTS idx_posts_hn_id ON posts(hn_id)')
    db.engine.execute('CREATE INDEX IF NOT EXISTS idx_posts_author_karma ON posts(author_karma)')
    db.engine.execute('CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at)')
    db.engine.execute('CREATE INDEX IF NOT EXISTS idx_posts_is_hidden_gem ON posts(is_hidden_gem)')
    db.engine.execute('CREATE INDEX IF NOT EXISTS idx_posts_author_created_at ON posts(author, created_at)')
    
    # Quality scores table indexes
    db.engine.execute('CREATE INDEX IF NOT EXISTS idx_quality_scores_post_id ON quality_scores(post_id)')
    db.engine.execute('CREATE INDEX IF NOT EXISTS idx_quality_scores_overall_interest ON quality_scores(overall_interest)')
    db.engine.execute('CREATE INDEX IF NOT EXISTS idx_quality_scores_spam_likelihood ON quality_scores(spam_likelihood)')
    
    # Hall of fame table indexes
    db.engine.execute('CREATE INDEX IF NOT EXISTS idx_hall_of_fame_post_id ON hall_of_fame(post_id)')
    db.engine.execute('CREATE INDEX IF NOT EXISTS idx_hall_of_fame_discovered_at ON hall_of_fame(discovered_at)')
    db.engine.execute('CREATE INDEX IF NOT EXISTS idx_hall_of_fame_success_at ON hall_of_fame(success_at)')
    
    # Users table indexes
    db.engine.execute('CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)')
    db.engine.execute('CREATE INDEX IF NOT EXISTS idx_users_karma ON users(karma)')
    db.engine.execute('CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at)')