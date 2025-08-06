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
                    logger.info(f"    Next run: {job['next_run'] or 'Not scheduled']}")
            
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
    
    # Auto-start scheduler if enabled
    def start_background_scheduler():
        """Start scheduler when Flask app starts."""
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