#!/usr/bin/env python3
"""
Background scheduler for HN Hidden Gems using APScheduler (no Redis required).
"""

import os
import threading
import time
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.executors.pool import ThreadPoolExecutor

from hn_hidden_gems.utils.logger import setup_logger
from hn_hidden_gems.api.hn_api import HackerNewsAPI
from hn_hidden_gems.analyzer.quality_analyzer import QualityAnalyzer
from hn_hidden_gems.models import Post, User, QualityScore, HallOfFame, db

logger = setup_logger(__name__)

class PostCollectionScheduler:
    """Scheduler for regular post collection without Redis dependency."""
    
    def __init__(self, app=None):
        self.app = app
        self.scheduler = None
        self._collection_stats = {
            'last_run': None,
            'last_duration': None,
            'posts_collected': 0,
            'gems_found': 0,
            'total_runs': 0,
            'errors': 0,
            'status': 'stopped'
        }
        self._collection_lock = threading.Lock()
        
    def init_app(self, app):
        """Initialize with Flask app."""
        self.app = app
        
        # Configure scheduler
        executors = {
            'default': ThreadPoolExecutor(max_workers=2)
        }
        
        job_defaults = {
            'coalesce': True,  # Combine multiple pending instances
            'max_instances': 1,  # Only one collection job at a time
            'misfire_grace_time': 300  # 5 minutes grace period
        }
        
        self.scheduler = BackgroundScheduler(
            executors=executors,
            job_defaults=job_defaults,
            timezone='UTC'
        )
        
        # Configure collection job based on settings
        self._configure_collection_job()
        self._configure_hall_of_fame_job()
        self._configure_super_gems_job()
        
    def _configure_collection_job(self):
        """Configure the periodic collection job."""
        if not self.scheduler:
            return
            
        collection_interval = int(os.environ.get('POST_COLLECTION_INTERVAL_MINUTES', 5))
        
        # Remove existing job if any
        try:
            self.scheduler.remove_job('collect_posts')
        except:
            pass
        
        if collection_interval > 0:
            # Add collection job
            self.scheduler.add_job(
                func=self._collect_posts_job,
                trigger=IntervalTrigger(minutes=collection_interval),
                id='collect_posts',
                name=f'Collect HN posts every {collection_interval} minutes',
                replace_existing=True
            )
            logger.info(f"Scheduled post collection every {collection_interval} minutes")
        else:
            logger.info("Post collection disabled (interval = 0)")
    
    def _configure_hall_of_fame_job(self):
        """Configure the periodic Hall of Fame monitoring job."""
        if not self.scheduler:
            return
            
        hof_interval = int(os.environ.get('HALL_OF_FAME_INTERVAL_HOURS', 6))
        
        # Remove existing job if any
        try:
            self.scheduler.remove_job('monitor_hall_of_fame')
        except:
            pass
        
        if hof_interval > 0:
            # Add Hall of Fame monitoring job
            self.scheduler.add_job(
                func=self._monitor_hall_of_fame_job,
                trigger=IntervalTrigger(hours=hof_interval),
                id='monitor_hall_of_fame',
                name=f'Monitor Hall of Fame every {hof_interval} hours',
                replace_existing=True
            )
            logger.info(f"Scheduled Hall of Fame monitoring every {hof_interval} hours")
        else:
            logger.info("Hall of Fame monitoring disabled (interval = 0)")
    
    def _configure_super_gems_job(self):
        """Configure the periodic super gems analysis job."""
        if not self.scheduler:
            return
            
        super_gems_interval = int(os.environ.get('SUPER_GEMS_INTERVAL_HOURS', 6))
        
        # Remove existing job if any
        try:
            self.scheduler.remove_job('analyze_super_gems')
        except:
            pass
        
        if super_gems_interval > 0:
            # Add super gems analysis job
            self.scheduler.add_job(
                func=self._analyze_super_gems_job,
                trigger=IntervalTrigger(hours=super_gems_interval),
                id='analyze_super_gems',
                name=f'Analyze super gems every {super_gems_interval} hours',
                replace_existing=True
            )
            logger.info(f"Scheduled super gems analysis every {super_gems_interval} hours")
        else:
            logger.info("Super gems analysis disabled (interval = 0)")
    
    def start(self):
        """Start the scheduler."""
        if self.scheduler and not self.scheduler.running:
            try:
                self.scheduler.start()
                self._collection_stats['status'] = 'running'
                logger.info("Post collection scheduler started")
                return True
            except Exception as e:
                logger.error(f"Failed to start scheduler: {e}")
                self._collection_stats['status'] = 'error'
                return False
        return False
    
    def stop(self):
        """Stop the scheduler."""
        if self.scheduler and self.scheduler.running:
            try:
                self.scheduler.shutdown(wait=True)
                self._collection_stats['status'] = 'stopped'
                logger.info("Post collection scheduler stopped")
                return True
            except Exception as e:
                logger.error(f"Failed to stop scheduler: {e}")
                return False
        return False
    
    def is_running(self):
        """Check if scheduler is running."""
        return self.scheduler and self.scheduler.running
    
    def get_status(self):
        """Get current status and statistics."""
        jobs = []
        if self.scheduler:
            for job in self.scheduler.get_jobs():
                jobs.append({
                    'id': job.id,
                    'name': job.name,
                    'next_run': job.next_run_time.isoformat() if job.next_run_time else None,
                    'trigger': str(job.trigger)
                })
        
        return {
            'enabled': int(os.environ.get('POST_COLLECTION_INTERVAL_MINUTES', 5)) > 0,
            'running': self.is_running(),
            'interval_minutes': int(os.environ.get('POST_COLLECTION_INTERVAL_MINUTES', 5)),
            'hof_enabled': int(os.environ.get('HALL_OF_FAME_INTERVAL_HOURS', 6)) > 0,
            'hof_interval_hours': int(os.environ.get('HALL_OF_FAME_INTERVAL_HOURS', 6)),
            'super_gems_enabled': int(os.environ.get('SUPER_GEMS_INTERVAL_HOURS', 6)) > 0,
            'super_gems_interval_hours': int(os.environ.get('SUPER_GEMS_INTERVAL_HOURS', 6)),
            'jobs': jobs,
            'stats': self._collection_stats.copy()
        }
    
    def collect_now(self, minutes_back=60):
        """Manually trigger collection."""
        if not self.app:
            raise Exception("Scheduler not initialized with Flask app")
            
        # Run collection in a separate thread to avoid blocking
        thread = threading.Thread(
            target=self._collect_posts_manual,
            args=(minutes_back,),
            daemon=True
        )
        thread.start()
        return True
    
    def _collect_posts_job(self):
        """Scheduled collection job."""
        collection_interval = int(os.environ.get('POST_COLLECTION_INTERVAL_MINUTES', 5))
        self._collect_posts_manual(collection_interval)
    
    def _monitor_hall_of_fame_job(self):
        """Scheduled Hall of Fame monitoring job."""
        if not self.app:
            logger.error("No Flask app available for Hall of Fame monitoring")
            return
            
        with self.app.app_context():
            self._monitor_hall_of_fame()
    
    def _analyze_super_gems_job(self):
        """Scheduled super gems analysis job."""
        if not self.app:
            logger.error("No Flask app available for super gems analysis")
            return
            
        with self.app.app_context():
            self._analyze_super_gems()
    
    def _collect_posts_manual(self, minutes_back):
        """Manual collection with Flask app context."""
        if not self.app:
            logger.error("No Flask app available for collection")
            return
            
        with self.app.app_context():
            self._collect_posts(minutes_back)
    
    def _collect_posts(self, minutes_back):
        """
        Collect posts from the last N minutes.
        Runs within Flask app context.
        """
        # Use lock to prevent concurrent collections
        if not self._collection_lock.acquire(blocking=False):
            logger.warning("Collection already in progress, skipping")
            return
        
        start_time = datetime.utcnow()
        
        try:
            self._collection_stats['status'] = 'collecting'
            logger.info(f"Starting collection of posts from last {minutes_back} minutes")
            
            # Initialize APIs
            hn_api = HackerNewsAPI()
            analyzer = QualityAnalyzer()
            
            # Calculate time window
            cutoff_time = datetime.utcnow() - timedelta(minutes=minutes_back)
            
            # Get recent story IDs
            max_stories = int(os.environ.get('POST_COLLECTION_MAX_STORIES', 500))
            story_ids = hn_api.get_story_ids('new', limit=max_stories)
            
            posts_processed = 0
            posts_created = 0
            gems_found = 0
            errors = 0
            batch_size = int(os.environ.get('POST_COLLECTION_BATCH_SIZE', 25))
            
            for story_id in story_ids:
                try:
                    # Check if we already have this post
                    existing_post = Post.find_by_hn_id(story_id)
                    if existing_post:
                        posts_processed += 1
                        continue
                    
                    # Get post data
                    post_data = hn_api.get_item(story_id)
                    if not post_data or post_data.get('type') != 'story':
                        posts_processed += 1
                        continue
                    
                    # Check if post is within our time window
                    post_time = datetime.fromtimestamp(post_data.get('time', 0))
                    if post_time < cutoff_time:
                        # Posts are ordered by recency, so we can break here
                        logger.info(f"Reached posts older than {minutes_back} minutes, stopping")
                        break
                    
                    # Skip posts without titles
                    if not post_data.get('title'):
                        posts_processed += 1
                        continue
                    
                    # Double-check for existing post to avoid race conditions
                    existing_post = Post.find_by_hn_id(story_id)
                    if existing_post:
                        posts_processed += 1
                        continue
                    
                    # Get author information
                    author_data = hn_api.get_user(post_data['by']) if post_data.get('by') else {}
                    author_karma = author_data.get('karma', 0) if author_data else 0
                    account_created = author_data.get('created', 0) if author_data else 0
                    
                    # Calculate account age in days
                    if account_created > 0:
                        account_age_days = (datetime.utcnow() - datetime.fromtimestamp(account_created)).days
                    else:
                        account_age_days = 0
                    
                    # Create or update user
                    user = User.find_or_create(post_data['by'], {
                        'karma': author_karma,
                        'created': account_created
                    })
                    
                    # Create post
                    post = Post(
                        hn_id=story_id,
                        title=post_data.get('title', ''),
                        url=post_data.get('url'),
                        text=post_data.get('text'),
                        author=post_data['by'],
                        author_karma=author_karma,
                        account_age_days=account_age_days,
                        score=post_data.get('score', 0),
                        descendants=post_data.get('descendants', 0),
                        hn_created_at=post_time
                    )
                    
                    # Analyze quality
                    quality_scores = analyzer.analyze_post_quality({
                        **post_data,
                        'author_karma': author_karma,
                        'account_age_days': account_age_days
                    })
                    
                    # Create quality score
                    quality_score = QualityScore(post=post)
                    quality_score.update_scores(quality_scores)
                    
                    # Determine if it's a hidden gem
                    karma_threshold = int(os.environ.get('KARMA_THRESHOLD', 100))
                    min_interest_score = float(os.environ.get('MIN_INTEREST_SCORE', 0.3))
                    
                    is_gem = (
                        author_karma < karma_threshold and
                        quality_scores['overall_interest'] >= min_interest_score and
                        quality_scores['spam_likelihood'] < 0.4
                    )
                    post.is_hidden_gem = is_gem
                    post.is_spam = quality_scores['spam_likelihood'] >= 0.7
                    
                    # Add to session
                    db.session.add(post)
                    db.session.add(quality_score)
                    
                    # Try to commit this individual post
                    try:
                        db.session.commit()
                        posts_created += 1
                        
                        if is_gem:
                            gems_found += 1
                            logger.info(f"Found gem {story_id}: {post_data.get('title', '')[:50]}... (score: {quality_scores['overall_interest']:.2f})")
                        
                        # Log progress every batch_size posts
                        if posts_created % batch_size == 0:
                            logger.info(f"Progress: {posts_created} posts created, {gems_found} gems found")
                            
                    except Exception as commit_error:
                        # Handle unique constraint violations gracefully
                        db.session.rollback()
                        if "UNIQUE constraint failed: posts.hn_id" in str(commit_error):
                            # This is expected when posts are processed multiple times
                            logger.debug(f"Post {story_id} already exists, skipping duplicate")
                            posts_processed += 1
                        else:
                            logger.error(f"Failed to commit post {story_id}: {commit_error}")
                            errors += 1
                            posts_processed += 1
                        continue
                    
                    posts_processed += 1
                
                except Exception as e:
                    logger.error(f"Error processing post {story_id}: {e}")
                    # Rollback any pending transaction
                    try:
                        db.session.rollback()
                    except:
                        pass
                    errors += 1
                    posts_processed += 1
                    continue
            
            # Ensure any remaining items are committed
            try:
                db.session.commit()
            except:
                db.session.rollback()
            
            # Update statistics
            duration = (datetime.utcnow() - start_time).total_seconds()
            self._collection_stats.update({
                'last_run': start_time.isoformat(),
                'last_duration': duration,
                'posts_collected': posts_created,
                'gems_found': gems_found,
                'total_runs': self._collection_stats['total_runs'] + 1,
                'errors': errors,
                'status': 'running'
            })
            
            logger.info(f"Collection completed: {posts_created} new posts, {gems_found} gems found, {errors} errors in {duration:.1f}s")
            
        except Exception as e:
            logger.error(f"Collection failed: {e}")
            try:
                db.session.rollback()
            except:
                pass
            self._collection_stats.update({
                'status': 'error',
                'errors': errors + 1,
                'last_run': start_time.isoformat(),
                'last_duration': (datetime.utcnow() - start_time).total_seconds()
            })
            
        finally:
            self._collection_lock.release()
    
    def _monitor_hall_of_fame(self):
        """
        Monitor discovered gems for success and update Hall of Fame.
        Runs within Flask app context.
        """
        start_time = datetime.utcnow()
        
        try:
            logger.info("Starting Hall of Fame monitoring...")
            
            # Initialize HN API
            hn_api = HackerNewsAPI()
            
            # Get all hidden gems that aren't spam
            gems = Post.query.filter(
                Post.is_hidden_gem == True,
                Post.is_spam == False
            ).all()
            
            logger.info(f"Monitoring {len(gems)} discovered gems for success...")
            
            new_successes = 0
            updated_entries = 0
            errors = 0
            
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
                    errors += 1
                    continue
            
            # Commit all changes
            try:
                db.session.commit()
                logger.info(f"Hall of Fame monitoring completed:")
                logger.info(f"  - New successes added: {new_successes}")
                logger.info(f"  - Existing entries updated: {updated_entries}")
                logger.info(f"  - Total gems monitored: {len(gems)}")
                logger.info(f"  - Errors: {errors}")
                logger.info(f"  - Duration: {(datetime.utcnow() - start_time).total_seconds():.1f}s")
            except Exception as e:
                logger.error(f"Failed to commit Hall of Fame updates: {e}")
                db.session.rollback()
                
        except Exception as e:
            logger.error(f"Hall of Fame monitoring failed: {e}")
            try:
                db.session.rollback()
            except:
                pass
    
    def _analyze_super_gems(self):
        """
        Analyze top gems with LLM for super gem status.
        Runs within Flask app context.
        """
        try:
            import asyncio
            from super_gem_analyzer import SuperGemsAnalyzer
            from hn_hidden_gems.config import config
            
            # Get config from Flask config
            gemini_api_key = self.app.config.get('GEMINI_API_KEY')
            if not gemini_api_key:
                logger.error("GEMINI_API_KEY not configured, skipping super gems analysis")
                return
            
            analysis_hours = int(os.environ.get('SUPER_GEMS_ANALYSIS_HOURS', 48))
            top_n = int(os.environ.get('SUPER_GEMS_TOP_N', 5))
            
            logger.info(f"Starting super gems analysis for last {analysis_hours} hours...")
            
            # Get proper database path
            import os
            database_url = self.app.config.get('DATABASE_URL', '')
            if database_url.startswith('sqlite:///'):
                db_path = database_url.replace('sqlite:///', '')
                # Handle relative vs absolute paths
                if not db_path.startswith('/'):
                    # For relative paths, resolve to absolute
                    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'instance', os.path.basename(db_path))
            else:
                db_path = 'instance/hn_hidden_gems.db'  # fallback
            
            # Create analyzer
            analyzer = SuperGemsAnalyzer(
                gemini_api_key=gemini_api_key,
                db_path=db_path
            )
            
            # Run analysis in async context
            asyncio.run(analyzer.run_analysis(hours=analysis_hours, top_n=top_n))
            
            logger.info("Super gems analysis completed")
            
        except Exception as e:
            logger.error(f"Super gems analysis failed: {e}")

# Global scheduler instance
scheduler = PostCollectionScheduler()