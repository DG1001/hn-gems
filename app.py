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
from hn_hidden_gems.scheduler import scheduler

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
    
    # Initialize scheduler
    scheduler.init_app(app)
    app.scheduler = scheduler
    
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
            'app_version': '0.1.0',
            'github_url': 'https://github.com/DG1001/hn-gems'
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
        
        # Test HN API
        hn_api = HackerNewsAPI()
        try:
            stories = hn_api.get_story_ids('new', 5)
            logger.info(f"✅ HN API test successful: retrieved {len(stories)} story IDs")
        except Exception as e:
            logger.error(f"❌ HN API test failed: {e}")
        
        # Test quality analyzer
        try:
            from hn_hidden_gems.analyzer.quality_analyzer import QualityAnalyzer
            analyzer = QualityAnalyzer()
            logger.info("✅ Quality analyzer initialized successfully")
        except Exception as e:
            logger.error(f"❌ Quality analyzer test failed: {e}")
    
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
    
    @app.cli.command()
    @app.cli.command('fetch-target')
    def fetch_target_post():
        """Fetch specific target post and surrounding area."""
        from hn_hidden_gems.api.hn_api import HackerNewsAPI
        from hn_hidden_gems.analyzer.quality_analyzer import QualityAnalyzer
        from hn_hidden_gems.models import Post, User, QualityScore
        
        hn_api = HackerNewsAPI()
        analyzer = QualityAnalyzer()
        
        target_id = 44782782  # User's post
        range_size = 500  # Check 500 posts around the target
        
        try:
            posts_processed = 0
            gems_found = 0
            
            # Process posts around the target ID
            start_id = target_id + range_size // 2
            end_id = target_id - range_size // 2
            
            logger.info(f"Fetching posts from {end_id} to {start_id} (targeting {target_id})")
            
            for hn_id in range(start_id, end_id, -1):
                try:
                    # Check if we already have this post
                    if Post.find_by_hn_id(hn_id):
                        continue
                    
                    # Fetch post from HN API
                    post_data = hn_api.get_item(hn_id)
                    if not post_data or post_data.get('type') != 'story':
                        continue
                    
                    if not post_data.get('title'):
                        continue
                    
                    # Get author karma
                    author_data = hn_api.get_user(post_data['by']) if post_data.get('by') else {}
                    author_karma = author_data.get('karma', 0) if author_data else 0
                    
                    # Create user
                    user = User.find_or_create(post_data['by'], {
                        'karma': author_karma,
                        'created': author_data.get('created', 0)
                    })
                    
                    # Create post
                    post = Post(
                        hn_id=hn_id,
                        title=post_data.get('title', ''),
                        url=post_data.get('url'),
                        text=post_data.get('text'),
                        author=post_data['by'],
                        author_karma=author_karma,
                        account_age_days=0,
                        score=post_data.get('score', 0),
                        descendants=post_data.get('descendants', 0),
                        hn_created_at=datetime.fromtimestamp(post_data.get('time', 0))
                    )
                    db.session.add(post)
                    
                    # Analyze quality
                    quality_scores = analyzer.analyze_post_quality({
                        **post_data,
                        'author_karma': author_karma
                    })
                    
                    # Create quality score
                    quality_score = QualityScore(post=post)
                    quality_score.update_scores(quality_scores)
                    db.session.add(quality_score)
                    
                    # Determine if it's a hidden gem
                    is_gem = (
                        author_karma < 100 and
                        quality_scores['overall_interest'] >= 0.3 and
                        quality_scores['spam_likelihood'] < 0.4
                    )
                    post.is_hidden_gem = is_gem
                    post.is_spam = quality_scores['spam_likelihood'] >= 0.7
                    
                    if is_gem:
                        gems_found += 1
                        logger.info(f"Found gem {hn_id}: {post_data.get('title', '')[:50]}... (score: {quality_scores['overall_interest']:.2f})")
                    
                    if hn_id == target_id:
                        logger.info(f"🎯 FOUND TARGET POST {target_id}: {post_data.get('title', '')}")
                        logger.info(f"   Author: {post_data['by']} (karma: {author_karma})")
                        logger.info(f"   Quality score: {quality_scores['overall_interest']:.2f}")
                        logger.info(f"   Is gem: {is_gem}")
                    
                    posts_processed += 1
                    
                    # Commit every 25 posts
                    if posts_processed % 25 == 0:
                        db.session.commit()
                        logger.info(f"Processed {posts_processed} posts, found {gems_found} gems")
                    
                except Exception as e:
                    logger.error(f"Error processing post {hn_id}: {e}")
                    continue
            
            db.session.commit()
            logger.info(f"Target fetch completed: {posts_processed} posts processed, {gems_found} gems found")
            
        except Exception as e:
            logger.error(f"Target fetch failed: {e}")
            db.session.rollback()
    
    @app.cli.command()
    def monitor_gems():
        """Monitor discovered gems for success and update Hall of Fame."""
        from hn_hidden_gems.api.hn_api import HackerNewsAPI
        from hn_hidden_gems.models import Post, HallOfFame
        
        hn_api = HackerNewsAPI()
        
        try:
            # Get all hidden gems that aren't spam
            gems = Post.query.filter(
                Post.is_hidden_gem == True,
                Post.is_spam == False
            ).all()
            
            logger.info(f"Monitoring {len(gems)} discovered gems for success...")
            
            new_successes = 0
            updated_entries = 0
            
            for gem in gems:
                try:
                    # Get current HN score
                    current_data = hn_api.get_item(gem.hn_id)
                    if not current_data:
                        continue
                    
                    current_score = current_data.get('score', 0)
                    current_descendants = current_data.get('descendants', 0)
                    
                    # Update post with current metrics
                    gem.score = current_score
                    gem.descendants = current_descendants
                    
                    # Check if this gem already has a Hall of Fame entry
                    hof_entry = HallOfFame.query.filter_by(post_id=gem.id).first()
                    
                    if hof_entry:
                        # Update existing entry
                        hof_entry.update_success_metrics(current_score)
                        updated_entries += 1
                        logger.info(f"Updated HoF entry for {gem.hn_id}: {current_score} points")
                    
                    elif current_score >= 100:  # New success threshold reached
                        # Calculate discovery age
                        if gem.hn_created_at and gem.created_at:
                            discovery_age_hours = (gem.created_at - gem.hn_created_at).total_seconds() / 3600
                        else:
                            discovery_age_hours = None
                        
                        # Create new Hall of Fame entry
                        hof_entry = HallOfFame.create_entry(
                            post=gem,
                            quality_score=gem.quality_score,
                            hn_age_hours=discovery_age_hours
                        )
                        
                        # Update with current success metrics
                        hof_entry.update_success_metrics(current_score)
                        
                        new_successes += 1
                        logger.info(f"🏆 NEW SUCCESS: {gem.title[:50]}... reached {current_score} points!")
                        logger.info(f"   HN ID: {gem.hn_id}")
                        logger.info(f"   Author: {gem.author} (karma: {gem.author_karma})")
                        logger.info(f"   Discovery score: {gem.quality_score.overall_interest:.2f}")
                
                except Exception as e:
                    logger.error(f"Error monitoring gem {gem.hn_id}: {e}")
                    continue
            
            db.session.commit()
            
            logger.info(f"Gem monitoring completed:")
            logger.info(f"  - New successes added to Hall of Fame: {new_successes}")
            logger.info(f"  - Existing entries updated: {updated_entries}")
            logger.info(f"  - Total gems monitored: {len(gems)}")
            
        except Exception as e:
            logger.error(f"Gem monitoring failed: {e}")
            db.session.rollback()
    
    @app.cli.command()
    def create_sample_hof():
        """Create sample Hall of Fame entries for testing."""
        from hn_hidden_gems.models import Post, HallOfFame
        from datetime import datetime, timedelta
        
        try:
            # Get some of our best gems to promote to Hall of Fame
            top_gems = Post.query.join(Post.quality_score).filter(
                Post.is_hidden_gem == True,
                Post.is_spam == False
            ).order_by(Post.quality_score.has(overall_interest=0.6)).limit(3).all()
            
            if not top_gems:
                logger.info("No gems found to create sample Hall of Fame entries")
                return
            
            created_count = 0
            
            for i, gem in enumerate(top_gems):
                # Check if already in Hall of Fame
                existing = HallOfFame.query.filter_by(post_id=gem.id).first()
                if existing:
                    continue
                
                # Create fake success scenario
                fake_discovery_score = max(10, gem.score or 10)  # Simulate low initial score
                fake_success_score = fake_discovery_score + (120 + i * 50)  # Simulate growth
                
                # Create discovery time (simulate we found it early)
                discovery_time = gem.hn_created_at + timedelta(hours=2 + i)
                success_time = discovery_time + timedelta(hours=6 + i * 2)
                
                # Create Hall of Fame entry
                hof_entry = HallOfFame(
                    post_id=gem.id,
                    discovered_at=discovery_time,
                    discovery_score=gem.quality_score.overall_interest,
                    discovery_hn_score=fake_discovery_score,
                    discovery_karma=gem.author_karma,
                    success_at=success_time,
                    success_hn_score=fake_success_score,
                    peak_hn_score=fake_success_score + 20,
                    success_threshold=100,
                    success_verified=True,
                    lead_time_hours=(success_time - discovery_time).total_seconds() / 3600,
                    hn_age_at_discovery_hours=2 + i
                )
                
                # Set success type based on score
                if fake_success_score >= 500:
                    hof_entry.success_type = 'viral'
                elif fake_success_score >= 200:
                    hof_entry.success_type = 'front_page'
                else:
                    hof_entry.success_type = 'top_100'
                
                # Update gem's current score to match success
                gem.score = fake_success_score
                
                db.session.add(hof_entry)
                created_count += 1
                
                logger.info(f"Created sample HoF entry: {gem.title[:50]}...")
                logger.info(f"  Discovery: {fake_discovery_score} → Success: {fake_success_score} points")
                logger.info(f"  Lead time: {hof_entry.lead_time_hours:.1f} hours")
            
            db.session.commit()
            logger.info(f"Created {created_count} sample Hall of Fame entries")
            
        except Exception as e:
            logger.error(f"Failed to create sample Hall of Fame entries: {e}")
            db.session.rollback()
    
    @app.cli.command()
    def start_collector():
        """Start the post collection background service."""
        collection_interval = int(os.environ.get('POST_COLLECTION_INTERVAL_MINUTES', 5))
        
        if collection_interval <= 0:
            logger.info("Post collection is disabled (POST_COLLECTION_INTERVAL_MINUTES <= 0)")
            return
        
        hof_interval = int(os.environ.get('HALL_OF_FAME_INTERVAL_HOURS', 6))
        logger.info(f"Starting background services:")
        logger.info(f"  - Post collection: {collection_interval} minute intervals")
        logger.info(f"  - Hall of Fame monitoring: {hof_interval} hour intervals")
        
        if scheduler.start():
            logger.info("✅ Background services started successfully")
            logger.info("Both post collection and Hall of Fame monitoring will run in the background")
            logger.info("Use 'flask collection-status' to check status")
        else:
            logger.error("❌ Failed to start background services")
    
    @app.cli.command()
    def stop_collector():
        """Stop the background services (post collection and Hall of Fame monitoring)."""
        if scheduler.stop():
            logger.info("✅ Background services stopped")
        else:
            logger.info("❌ Services were not running")
    
    @app.cli.command()
    def collect_now():
        """Manually trigger post collection now."""
        minutes_back = int(input("How many minutes back to collect? (default: 60): ") or 60)
        
        logger.info(f"Starting manual collection for last {minutes_back} minutes...")
        
        try:
            scheduler.collect_now(minutes_back)
            logger.info("✅ Collection started in background")
            logger.info("Use 'flask collection-status' to check progress")
        except Exception as e:
            logger.error(f"❌ Failed to start collection: {e}")
    
    @app.cli.command()
    def collection_status():
        """Get status of the post collection service."""
        try:
            status = scheduler.get_status()
            
            logger.info("=== Post Collection Service Status ===")
            logger.info(f"Enabled: {status['enabled']}")
            logger.info(f"Running: {status['running']}")
            logger.info(f"Post collection interval: {status['interval_minutes']} minutes")
            logger.info(f"Hall of Fame monitoring enabled: {status['hof_enabled']}")
            logger.info(f"Hall of Fame monitoring interval: {status['hof_interval_hours']} hours")
            
            if status['jobs']:
                logger.info("Scheduled Jobs:")
                for job in status['jobs']:
                    logger.info(f"  - {job['name']}")
                    logger.info(f"    Next run: {job['next_run'] or 'Not scheduled'}")
            
            stats = status['stats']
            logger.info(f"\nStatistics:")
            logger.info(f"  Status: {stats['status']}")
            logger.info(f"  Total runs: {stats['total_runs']}")
            logger.info(f"  Last run: {stats['last_run'] or 'Never'}")
            logger.info(f"  Last duration: {stats['last_duration']:.1f}s" if stats['last_duration'] else "  Last duration: N/A")
            logger.info(f"  Posts collected (last run): {stats['posts_collected']}")
            logger.info(f"  Gems found (last run): {stats['gems_found']}")
            logger.info(f"  Errors (last run): {stats['errors']}")
                
        except Exception as e:
            logger.error(f"Failed to get collection status: {e}")
    
    @app.cli.command() 
    def config_collection():
        """Configure post collection and Hall of Fame monitoring settings."""
        current_interval = int(os.environ.get('POST_COLLECTION_INTERVAL_MINUTES', 5))
        hof_interval = int(os.environ.get('HALL_OF_FAME_INTERVAL_HOURS', 6))
        
        logger.info("=== Background Services Configuration ===")
        logger.info(f"Post collection interval: {current_interval} minutes")
        logger.info(f"Post collection status: {'Enabled' if current_interval > 0 else 'Disabled'}")
        logger.info(f"Hall of Fame monitoring interval: {hof_interval} hours")
        logger.info(f"Hall of Fame monitoring status: {'Enabled' if hof_interval > 0 else 'Disabled'}")
        logger.info("")
        logger.info("To change settings, set environment variables:")
        logger.info("# Post Collection")
        logger.info("POST_COLLECTION_INTERVAL_MINUTES=5    # Minutes between collections (0 to disable)")
        logger.info("POST_COLLECTION_BATCH_SIZE=25         # Posts to commit per batch")
        logger.info("POST_COLLECTION_MAX_STORIES=500       # Max story IDs to fetch per run")
        logger.info("")
        logger.info("# Hall of Fame Monitoring")
        logger.info("HALL_OF_FAME_INTERVAL_HOURS=6         # Hours between HoF checks (0 to disable)")
        logger.info("")
        logger.info("# Quality Thresholds")
        logger.info("KARMA_THRESHOLD=100                   # Max author karma for gems")
        logger.info("MIN_INTEREST_SCORE=0.3               # Min quality score for gems")
        logger.info("")
        logger.info("Example:")
        logger.info("export POST_COLLECTION_INTERVAL_MINUTES=10")
        logger.info("export HALL_OF_FAME_INTERVAL_HOURS=4")
        logger.info("python app.py  # Restart the Flask app to apply changes")
    
    @app.cli.command()
    def fetch_historical():
        """Fetch historical posts from the last 2 days using HN item IDs."""
        from hn_hidden_gems.api.hn_api import HackerNewsAPI
        from hn_hidden_gems.analyzer.quality_analyzer import QualityAnalyzer
        from hn_hidden_gems.models import Post, User, QualityScore
        
        hn_api = HackerNewsAPI()
        analyzer = QualityAnalyzer()
        
        try:
            # Get current database range to know where to start
            existing_min = Post.query.with_entities(Post.hn_id).order_by(Post.hn_id.asc()).first()
            if existing_min:
                start_id = existing_min[0] - 1
                logger.info(f"Starting backward from HN ID {start_id}")
            else:
                # If no posts exist, start from a recent ID
                start_id = 44795000
                logger.info(f"No existing posts, starting from {start_id}")
            
            # Fetch posts going backwards to cover last 2 days
            # Approximately 2000-4000 posts per day on HN
            target_posts = 6000  # Should cover ~2 days
            batch_size = 100
            
            posts_processed = 0
            gems_found = 0
            
            # Go backwards through HN item IDs
            for batch_start in range(start_id, start_id - target_posts, -batch_size):
                batch_end = max(batch_start - batch_size, start_id - target_posts)
                logger.info(f"Processing batch: {batch_end} to {batch_start}")
                
                # Fetch posts in this ID range
                for hn_id in range(batch_start, batch_end, -1):
                    try:
                        # Check if we already have this post
                        if Post.find_by_hn_id(hn_id):
                            continue
                        
                        # Fetch post from HN API
                        post_data = hn_api.get_item(hn_id)
                        if not post_data or post_data.get('type') != 'story':
                            continue
                        
                        if not post_data.get('title'):
                            continue
                        
                        # Get author karma
                        author_data = hn_api.get_user(post_data['by']) if post_data.get('by') else {}
                        author_karma = author_data.get('karma', 0) if author_data else 0
                        
                        # Create user
                        user = User.find_or_create(post_data['by'], {
                            'karma': author_karma,
                            'created': author_data.get('created', 0)
                        })
                        
                        # Create post
                        post = Post(
                            hn_id=hn_id,
                            title=post_data.get('title', ''),
                            url=post_data.get('url'),
                            text=post_data.get('text'),
                            author=post_data['by'],
                            author_karma=author_karma,
                            account_age_days=0,  # We'll calculate this if needed
                            score=post_data.get('score', 0),
                            descendants=post_data.get('descendants', 0),
                            hn_created_at=datetime.fromtimestamp(post_data.get('time', 0))
                        )
                        db.session.add(post)
                        
                        # Analyze quality
                        quality_scores = analyzer.analyze_post_quality({
                            **post_data,
                            'author_karma': author_karma
                        })
                        
                        # Create quality score
                        quality_score = QualityScore(post=post)
                        quality_score.update_scores(quality_scores)
                        db.session.add(quality_score)
                        
                        # Determine if it's a hidden gem
                        is_gem = (
                            author_karma < 100 and
                            quality_scores['overall_interest'] >= 0.3 and
                            quality_scores['spam_likelihood'] < 0.4
                        )
                        post.is_hidden_gem = is_gem
                        post.is_spam = quality_scores['spam_likelihood'] >= 0.7
                        
                        if is_gem:
                            gems_found += 1
                            logger.info(f"Found gem {hn_id}: {post_data.get('title', '')[:50]}... (score: {quality_scores['overall_interest']:.2f})")
                        
                        posts_processed += 1
                        
                        # Commit every 50 posts to avoid memory issues
                        if posts_processed % 50 == 0:
                            db.session.commit()
                            logger.info(f"Processed {posts_processed} posts, found {gems_found} gems")
                        
                    except Exception as e:
                        logger.error(f"Error processing post {hn_id}: {e}")
                        continue
                
                # Check if we found the target post
                if hn_id == 44782782:
                    logger.info("Found target post 44782782!")
                    break
            
            db.session.commit()
            logger.info(f"Historical fetch completed: {posts_processed} posts processed, {gems_found} gems found")
            
        except Exception as e:
            logger.error(f"Historical fetch failed: {e}")
            db.session.rollback()
    
    @app.cli.command('analyze-super-gems')
    def analyze_super_gems():
        """Manually trigger super gems analysis."""
        try:
            import asyncio
            import os
            from super_gem_analyzer import SuperGemsAnalyzer
            
            # Get config
            gemini_api_key = app.config.get('GEMINI_API_KEY')
            if not gemini_api_key:
                logger.error("GEMINI_API_KEY not configured. Please set it in your environment.")
                return
            
            analysis_hours = int(os.environ.get('SUPER_GEMS_ANALYSIS_HOURS', 48))
            top_n = int(os.environ.get('SUPER_GEMS_TOP_N', 5))
            
            logger.info(f"Starting manual super gems analysis for last {analysis_hours} hours...")
            
            # Get proper database path
            database_url = app.config.get('DATABASE_URL', '')
            if database_url.startswith('sqlite:///'):
                db_path = database_url.replace('sqlite:///', '')
                # Handle relative vs absolute paths
                if not db_path.startswith('/'):
                    # For relative paths, resolve to absolute
                    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'instance', os.path.basename(db_path))
            else:
                db_path = 'instance/hn_hidden_gems.db'  # fallback
            
            # Create analyzer
            analyzer = SuperGemsAnalyzer(
                gemini_api_key=gemini_api_key,
                db_path=db_path
            )
            
            # Run analysis
            asyncio.run(analyzer.run_analysis(hours=analysis_hours, top_n=top_n))
            
            logger.info("Super gems analysis completed successfully!")
            
            # Trigger podcast generation after super gems analysis completes
            podcast_enabled = os.environ.get('AUDIO_GENERATION_ENABLED', 'false').lower() == 'true'
            if podcast_enabled:
                logger.info("Triggering podcast generation after super gems analysis...")
                try:
                    # Get scheduler instance and trigger podcast generation
                    if hasattr(app, 'scheduler') and app.scheduler:
                        app.scheduler._generate_podcast_audio()
                    else:
                        logger.warning("Scheduler not available, podcast generation skipped")
                except Exception as podcast_error:
                    logger.error(f"Podcast generation failed after super gems analysis: {podcast_error}")
            
        except Exception as e:
            logger.error(f"Super gems analysis failed: {e}")
    
    @app.cli.command()
    def find_duplicates():
        """Find and report duplicate posts in the database."""
        from hn_hidden_gems.models import Post
        
        try:
            logger.info("Searching for duplicate posts...")
            # Start with a smaller batch for performance
            duplicates = Post.find_duplicates(limit=500)
            
            if not duplicates:
                logger.info("No duplicates found.")
                return
            
            logger.info(f"Found {len(duplicates)} duplicate pairs:")
            
            for i, (post1, post2, similarity) in enumerate(duplicates, 1):
                logger.info(f"\n--- Duplicate Pair {i} ---")
                logger.info(f"Post 1: HN ID {post1['hn_id']} by {post1['author']}")
                logger.info(f"  Title: {post1['title'][:60]}...")
                logger.info(f"  URL: {post1.get('url', 'No URL')[:60]}...")
                
                logger.info(f"Post 2: HN ID {post2['hn_id']} by {post2['author']}")
                logger.info(f"  Title: {post2['title'][:60]}...")
                logger.info(f"  URL: {post2.get('url', 'No URL')[:60]}...")
                
                logger.info(f"Similarity: URL={similarity.get('url_similarity', 0):.2f}, "
                           f"Title={similarity.get('title_similarity', 0):.2f}, "
                           f"Content={similarity.get('content_similarity', 0):.2f}")
                logger.info(f"Confidence: {similarity.get('confidence_score', 0):.2f}")
                logger.info(f"Reasons: {', '.join(similarity.get('duplicate_reasons', []))}")
                
                if i >= 10:  # Limit output for readability
                    logger.info(f"\n... and {len(duplicates) - 10} more duplicate pairs")
                    break
            
        except Exception as e:
            logger.error(f"Failed to find duplicates: {e}")
    
    @app.cli.command()
    def clean_duplicates():
        """Automatically clean up duplicate posts by marking lower-quality ones as spam."""
        from hn_hidden_gems.models import Post
        from hn_hidden_gems.utils.duplicate_detector import DuplicateDetector
        
        try:
            logger.info("Finding and cleaning duplicate posts...")
            
            # Get all duplicates
            duplicates = Post.find_duplicates(limit=2000)
            
            if not duplicates:
                logger.info("No duplicates found to clean.")
                return
            
            detector = DuplicateDetector()
            cleaned_count = 0
            
            for post1, post2, similarity in duplicates:
                try:
                    # Get recommendation for what to do
                    recommendation = detector.get_duplicate_action_recommendation(post1, post2, similarity)
                    
                    if recommendation['action'] in ['remove_lower_quality', 'flag_spam_behavior']:
                        remove_post = recommendation['remove_post']
                        keep_post = recommendation['keep_post']
                        
                        # Mark the lower quality post as duplicate/spam
                        success = Post.mark_as_duplicate(
                            remove_post['id'], 
                            keep_post['id'],
                            f"Duplicate of HN ID {keep_post['hn_id']}: {', '.join(recommendation['reasoning'])}"
                        )
                        
                        if success:
                            cleaned_count += 1
                            logger.info(f"Marked HN ID {remove_post['hn_id']} as duplicate of {keep_post['hn_id']}")
                            logger.info(f"  Reason: {', '.join(recommendation['reasoning'])}")
                        
                except Exception as e:
                    logger.error(f"Error processing duplicate pair: {e}")
                    continue
            
            logger.info(f"Cleaning completed. Marked {cleaned_count} posts as duplicates/spam.")
            
        except Exception as e:
            logger.error(f"Failed to clean duplicates: {e}")
    
    @app.cli.command()
    def check_post_duplicates():
        """Interactively check a specific post for duplicates."""
        from hn_hidden_gems.models import Post
        
        try:
            hn_id = input("Enter HN ID to check for duplicates: ").strip()
            if not hn_id.isdigit():
                logger.error("Please enter a valid HN ID (number)")
                return
            
            post = Post.find_by_hn_id(int(hn_id))
            if not post:
                logger.error(f"Post with HN ID {hn_id} not found in database")
                return
            
            logger.info(f"Checking duplicates for post: {post.title[:60]}...")
            
            candidates = Post.get_duplicate_candidates(post)
            
            if not candidates:
                logger.info("No duplicate candidates found.")
                return
            
            logger.info(f"Found {len(candidates)} potential duplicates:")
            
            for i, candidate_info in enumerate(candidates, 1):
                candidate = candidate_info['post']
                similarity = candidate_info['similarity']
                recommendation = candidate_info['recommendation']
                
                logger.info(f"\n--- Candidate {i} ---")
                logger.info(f"HN ID: {candidate.hn_id} by {candidate.author}")
                logger.info(f"Title: {candidate.title[:60]}...")
                logger.info(f"URL: {candidate.url[:60] if candidate.url else 'No URL'}...")
                logger.info(f"Similarity: URL={similarity.get('url_similarity', 0):.2f}, "
                           f"Title={similarity.get('title_similarity', 0):.2f}, "
                           f"Content={similarity.get('content_similarity', 0):.2f}")
                logger.info(f"Confidence: {similarity.get('confidence_score', 0):.2f}")
                logger.info(f"Recommendation: {recommendation['action']}")
                logger.info(f"Reasoning: {', '.join(recommendation['reasoning'])}")
                
                # Ask user what to do
                action = input(f"Mark HN ID {candidate.hn_id} as duplicate? (y/n/s=skip all): ").lower()
                
                if action == 's':
                    break
                elif action == 'y':
                    success = Post.mark_as_duplicate(candidate.id, post.id, 
                                                   f"Manual duplicate marking: {', '.join(recommendation['reasoning'])}")
                    if success:
                        logger.info(f"✅ Marked HN ID {candidate.hn_id} as duplicate")
                    else:
                        logger.error(f"❌ Failed to mark HN ID {candidate.hn_id} as duplicate")
                
        except KeyboardInterrupt:
            logger.info("Duplicate checking cancelled by user")
        except Exception as e:
            logger.error(f"Failed to check duplicates: {e}")
    
    @app.cli.command()
    def cleanup_existing_duplicates():
        """Find and mark existing duplicate posts in the database as spam."""
        from hn_hidden_gems.models import Post
        from hn_hidden_gems.utils.duplicate_detector import DuplicateDetector
        import sqlite3
        
        try:
            logger.info("Finding and cleaning up existing duplicates...")
            
            # Use database query to find obvious URL duplicates first
            # The data is actually in instance/hn_hidden_gems.db
            db_path = 'instance/hn_hidden_gems.db'
            logger.info(f"Using database path: {db_path}")
            
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Find posts with identical URLs
            url_duplicates_query = """
            SELECT url, COUNT(*) as count, GROUP_CONCAT(hn_id) as hn_ids
            FROM posts 
            WHERE url IS NOT NULL AND url != '' 
            AND is_spam = 0
            GROUP BY url 
            HAVING count > 1
            ORDER BY count DESC
            """
            
            cursor.execute(url_duplicates_query)
            url_duplicates = cursor.fetchall()
            
            logger.info(f"Found {len(url_duplicates)} URLs with multiple posts:")
            
            cleaned_count = 0
            detector = DuplicateDetector()
            
            for url, count, hn_ids_str in url_duplicates:
                hn_ids = [int(x) for x in hn_ids_str.split(',')]
                logger.info(f"\nURL: {url}")
                logger.info(f"  Posts: {hn_ids} ({count} total)")
                
                # Get the posts for this URL
                posts_query = """
                SELECT id, hn_id, title, author, author_karma, score, created_at, hn_created_at
                FROM posts WHERE url = ? AND is_spam = 0
                ORDER BY hn_id ASC
                """
                cursor.execute(posts_query, (url,))
                posts = cursor.fetchall()
                
                if len(posts) > 1:
                    # Keep the first post (lowest HN ID), mark others as duplicates
                    keep_post = posts[0]
                    duplicate_posts = posts[1:]
                    
                    logger.info(f"  Keeping: HN ID {keep_post[1]} by {keep_post[3]}")
                    
                    for dup_post in duplicate_posts:
                        # Mark as spam/duplicate
                        update_query = """
                        UPDATE posts 
                        SET is_spam = 1, is_hidden_gem = 0 
                        WHERE id = ?
                        """
                        cursor.execute(update_query, (dup_post[0],))
                        cleaned_count += 1
                        logger.info(f"  Marked duplicate: HN ID {dup_post[1]} by {dup_post[3]}")
            
            conn.commit()
            conn.close()
            
            logger.info(f"\nCleanup completed. Marked {cleaned_count} existing posts as duplicates.")
            
        except Exception as e:
            logger.error(f"Failed to cleanup duplicates: {e}")
    
    # Auto-start scheduler if enabled
    def start_background_scheduler():
        """Start scheduler when Flask app starts."""
        # Only start scheduler in main process, not in Flask reloader process
        if os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
            logger.info("Skipping scheduler start in Flask reloader process")
            return
            
        collection_interval = int(os.environ.get('POST_COLLECTION_INTERVAL_MINUTES', 5))
        if collection_interval > 0:
            if scheduler.start():
                logger.info(f"✅ Auto-started post collection service ({collection_interval} min intervals)")
            else:
                logger.warning("⚠️ Failed to auto-start post collection service")
    
    # Call startup function immediately
    start_background_scheduler()
    
    # Shutdown scheduler when app closes
    import atexit
    def shutdown_scheduler():
        """Stop scheduler when Flask app shuts down."""
        if scheduler.is_running():
            scheduler.stop()
            logger.info("Post collection service stopped on app shutdown")
    
    # Podcast Management CLI Commands
    @app.cli.command('generate-podcast')
    def generate_podcast():
        """Manually trigger podcast generation."""
        try:
            logger.info("Manually triggering podcast generation...")
            
            # Check if podcast generation is enabled
            podcast_enabled = os.environ.get('AUDIO_GENERATION_ENABLED', 'false').lower() == 'true'
            if not podcast_enabled:
                logger.warning("Podcast generation is disabled. Set AUDIO_GENERATION_ENABLED=true to enable.")
                logger.info("Running script generation only (no audio)...")
                
                # Just generate script for testing
                from hn_hidden_gems.services.podcast_generator import PodcastGenerator
                import json
                
                # Check for Gemini API key
                gemini_api_key = app.config.get('GEMINI_API_KEY') or os.environ.get('GEMINI_API_KEY')
                if not gemini_api_key:
                    logger.error("GEMINI_API_KEY not found in environment or config")
                    return
                
                # Load super gems data
                super_gems_file = 'super-gems.json'
                if not os.path.exists(super_gems_file):
                    logger.error(f"Super gems file {super_gems_file} not found. Run super gems analysis first.")
                    return
                
                with open(super_gems_file, 'r') as f:
                    super_gems_data = json.load(f)
                
                # Transform data for podcast generation
                gems_data = {
                    'gems': [],
                    'generation_timestamp': datetime.now().isoformat(),
                    'total_analyzed': len(super_gems_data)
                }
                
                for gem in super_gems_data:  # Process all gems
                    gem_entry = {
                        'hn_id': gem.get('post_hn_id'),
                        'title': gem.get('title'),
                        'url': gem.get('url'),
                        'author': gem.get('author'),
                        'analysis': gem.get('analysis', {}),
                        'author_karma': 50
                    }
                    
                    analysis = gem_entry['analysis']
                    analysis['overall_rating'] = gem.get('super_gem_score', 0)
                    analysis['detailed_analysis'] = gem.get('reasoning', f"This {gem.get('title', 'project')} demonstrates excellent technical merit.")
                    analysis['strengths'] = gem.get('strengths', ["High-quality implementation"])
                    analysis['areas_for_improvement'] = gem.get('concerns', ["Documentation could be expanded"])
                    
                    gems_data['gems'].append(gem_entry)
                
                # Generate script
                podcast_generator = PodcastGenerator(gemini_api_key)
                script_data = podcast_generator.generate_podcast_script(gems_data)
                
                if script_data and script_data.get('script'):
                    # Save script
                    output_file = f"manual_podcast_script_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                    with open(output_file, 'w', encoding='utf-8') as f:
                        f.write(script_data['script'])
                    
                    logger.info(f"✅ Podcast script generated: {output_file}")
                    logger.info(f"📊 Words: {script_data['metadata']['total_words']}, Duration: {script_data['metadata']['estimated_duration_minutes']} min")
                else:
                    logger.error("Failed to generate podcast script")
                
                return
            
            # Full podcast generation (script + audio)
            with app.app_context():
                scheduler._generate_podcast_audio()
                logger.info("✅ Podcast generation completed")
                
        except Exception as e:
            logger.error(f"Podcast generation failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    @app.cli.command('podcast-status')
    def podcast_status():
        """Check podcast generation status and configuration."""
        try:
            logger.info("📊 Podcast Generation Status")
            logger.info("=" * 40)
            
            # Check configuration
            podcast_enabled = os.environ.get('AUDIO_GENERATION_ENABLED', 'false').lower() == 'true'
            gemini_key = os.environ.get('GEMINI_API_KEY')
            tts_credentials = os.environ.get('GOOGLE_TTS_CREDENTIALS_PATH')
            audio_path = os.environ.get('AUDIO_STORAGE_PATH', 'static/audio')
            
            logger.info(f"Audio Generation Enabled: {'✅ Yes' if podcast_enabled else '❌ No'}")
            logger.info(f"Gemini API Key: {'✅ Configured' if gemini_key else '❌ Missing'}")
            logger.info(f"Google TTS Credentials: {'✅ Configured' if tts_credentials else '❌ Not configured'}")
            logger.info(f"Audio Storage Path: {audio_path}")
            
            # Check for existing files
            super_gems_file = 'super-gems.json'
            logger.info(f"Super Gems Data: {'✅ Available' if os.path.exists(super_gems_file) else '❌ Missing'}")
            
            # Check audio storage
            if os.path.exists(audio_path):
                import glob
                audio_files = glob.glob(os.path.join(audio_path, "*.mp3"))
                logger.info(f"Existing Audio Files: {len(audio_files)}")
                for audio_file in audio_files[-3:]:  # Show last 3
                    file_size = os.path.getsize(audio_file) / (1024 * 1024)  # MB
                    logger.info(f"  • {os.path.basename(audio_file)} ({file_size:.1f} MB)")
            else:
                logger.info(f"Audio Storage Directory: ❌ Does not exist ({audio_path})")
            
            # Database status
            try:
                from hn_hidden_gems.models import AudioMetadata
                audio_count = AudioMetadata.query.count()
                logger.info(f"Audio Database Entries: {audio_count}")
                
                latest_audio = AudioMetadata.find_latest('super-gems')
                if latest_audio:
                    logger.info(f"Latest Audio: {latest_audio.filename} ({latest_audio.generation_timestamp})")
                else:
                    logger.info("Latest Audio: None")
                    
            except Exception as e:
                logger.info(f"Database Status: ❌ Error checking database ({e})")
            
        except Exception as e:
            logger.error(f"Failed to get podcast status: {e}")

    atexit.register(shutdown_scheduler)
    
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