#!/usr/bin/env python3
"""
HN Hidden Gems Finder - Main Flask Application

A tool that discovers high-quality Hacker News posts from low-karma accounts
that would otherwise be overlooked.
"""

import os
import sys
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template
from werkzeug.exceptions import HTTPException

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from hn_hidden_gems.config import config
from hn_hidden_gems.models import db, init_db
from hn_hidden_gems.web.routes import main as main_bp, api
from hn_hidden_gems.utils.logger import setup_logger

logger = setup_logger(__name__)

def create_app(config_name=None):
    """Create and configure the Flask application."""
    app = Flask(__name__)
    
    # Load configuration
    if config_name is None:
        config_name = os.environ.get('FLASK_ENV', 'development')
    
    app.config.from_object(config[config_name])
    
    # Set SQLAlchemy database URI
    app.config['SQLALCHEMY_DATABASE_URI'] = app.config['DATABASE_URL']
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    
    # Initialize database
    db.init_app(app)
    
    # Register blueprints
    app.register_blueprint(main_bp)
    app.register_blueprint(api)
    
    # Create tables if they don't exist
    with app.app_context():
        try:
            db.create_all()
            logger.info("Database tables created/verified")
        except Exception as e:
            logger.error(f"Failed to create database tables: {e}")
    
    # Error handlers
    @app.errorhandler(404)
    def not_found(error):
        if request.path.startswith('/api/'):
            return jsonify({'error': 'Endpoint not found'}), 404
        return render_template('404.html'), 404
    
    @app.errorhandler(500)
    def internal_error(error):
        db.session.rollback()
        if request.path.startswith('/api/'):
            return jsonify({'error': 'Internal server error'}), 500
        return render_template('500.html'), 500
    
    @app.errorhandler(HTTPException)
    def handle_http_exception(error):
        if request.path.startswith('/api/'):
            return jsonify({'error': error.description}), error.code
        return error
    
    # Request hooks
    @app.before_request
    def before_request():
        # Log API requests
        if request.path.startswith('/api/'):
            logger.debug(f"API Request: {request.method} {request.path}")
    
    @app.after_request
    def after_request(response):
        # Add CORS headers for API requests
        if request.path.startswith('/api/'):
            response.headers['Access-Control-Allow-Origin'] = '*'
            response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
            response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
            # Prevent caching of API responses
            response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
            response.headers['Pragma'] = 'no-cache'
            response.headers['Expires'] = '0'
        
        # Add security headers
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'DENY'
        response.headers['X-XSS-Protection'] = '1; mode=block'
        
        return response
    
    # Context processors
    @app.context_processor
    def inject_template_vars():
        return {
            'now': datetime.utcnow,
            'app_version': '0.1.0'
        }
    
    # CLI commands
    @app.cli.command()
    def init_db_cli():
        """Initialize database with tables and indexes."""
        init_db(app)
        logger.info("Database initialized successfully")
    
    @app.cli.command()
    def test_apis():
        """Test API connections."""
        from hn_hidden_gems.api.hn_api import HackerNewsAPI
        from hn_hidden_gems.api.algolia_api import AlgoliaHNAPI
        
        # Test HN API
        hn_api = HackerNewsAPI()
        try:
            stories = hn_api.get_story_ids('new', 5)
            logger.info(f"HN API test successful: retrieved {len(stories)} story IDs")
        except Exception as e:
            logger.error(f"HN API test failed: {e}")
        
        # Test Algolia API
        algolia_api = AlgoliaHNAPI()
        try:
            posts = algolia_api.get_low_karma_posts(50, 1)
            logger.info(f"Algolia API test successful: retrieved {len(posts)} posts")
        except Exception as e:
            logger.error(f"Algolia API test failed: {e}")
    
    @app.cli.command()
    def analyze_sample():
        """Analyze a sample of recent posts."""
        from hn_hidden_gems.api.hn_api import HackerNewsAPI
        from hn_hidden_gems.analyzer.quality_analyzer import QualityAnalyzer
        from hn_hidden_gems.models import Post, User, QualityScore
        
        hn_api = HackerNewsAPI()
        analyzer = QualityAnalyzer()
        
        try:
            # Get recent posts (more for better sample)
            posts = hn_api.get_posts_with_metadata('new', 50)
            logger.info(f"Retrieved {len(posts)} posts for analysis")
            
            for post_data in posts:
                try:
                    # Create or update user
                    user = User.find_or_create(post_data['by'], {
                        'karma': post_data.get('author_karma', 0),
                        'created': post_data.get('account_age_days', 0)
                    })
                    
                    # Create or update post
                    post = Post.find_by_hn_id(post_data['id'])
                    if not post:
                        post = Post(
                            hn_id=post_data['id'],
                            title=post_data.get('title', ''),
                            url=post_data.get('url'),
                            text=post_data.get('text'),
                            author=post_data['by'],
                            author_karma=post_data.get('author_karma', 0),
                            account_age_days=post_data.get('account_age_days', 0),
                            score=post_data.get('score', 0),
                            descendants=post_data.get('descendants', 0),
                            hn_created_at=datetime.fromtimestamp(post_data.get('time', 0))
                        )
                        db.session.add(post)
                    
                    # Analyze quality
                    quality_scores = analyzer.analyze_post_quality(post_data)
                    
                    # Create or update quality score
                    if not post.quality_score:
                        post.quality_score = QualityScore(post=post)
                        db.session.add(post.quality_score)
                    
                    post.quality_score.update_scores(quality_scores)
                    
                    # Determine if it's a hidden gem (more realistic thresholds)
                    is_gem = (
                        post.author_karma < 100 and  # Increased karma threshold
                        quality_scores['overall_interest'] >= 0.3 and  # Lowered interest threshold
                        quality_scores['spam_likelihood'] < 0.4  # Slightly more lenient spam threshold
                    )
                    post.is_hidden_gem = is_gem
                    post.is_spam = quality_scores['spam_likelihood'] >= 0.7
                    
                    logger.info(f"Analyzed post {post.hn_id}: gem={is_gem}, score={quality_scores['overall_interest']:.2f}")
                    
                except Exception as e:
                    logger.error(f"Error analyzing post {post_data.get('id', 'unknown')}: {e}")
                    continue
            
            db.session.commit()
            logger.info("Sample analysis completed successfully")
            
        except Exception as e:
            logger.error(f"Sample analysis failed: {e}")
            db.session.rollback()
    
    logger.info(f"Flask app created with config: {config_name}")
    return app

def main():
    """Run the application."""
    app = create_app()
    
    # Get configuration
    host = os.environ.get('HOST', '127.0.0.1')
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'True').lower() == 'true'
    
    logger.info(f"Starting HN Hidden Gems server on {host}:{port} (debug={debug})")
    
    try:
        app.run(host=host, port=port, debug=debug)
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()