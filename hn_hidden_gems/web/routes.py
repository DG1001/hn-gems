from datetime import datetime, timedelta
from flask import Blueprint, render_template, jsonify, request, current_app, send_from_directory
from hn_hidden_gems.models import Post, QualityScore, HallOfFame, User
from hn_hidden_gems.utils.logger import setup_logger

logger = setup_logger(__name__)

main = Blueprint('main', __name__)
api = Blueprint('api', __name__, url_prefix='/api')

@main.route('/')
def index():
    """Main page with hidden gems feed."""
    return render_template('index.html')

@main.route('/hall-of-fame')
def hall_of_fame():
    """Hall of Fame page showing successful discoveries."""
    return render_template('hall_of_fame.html')

@main.route('/stats')
def stats():
    """Statistics dashboard."""
    return render_template('stats.html')

@main.route('/about')
def about():
    """About page with project information."""
    return render_template('about.html')

@main.route('/sw.js')
def service_worker():
    """Serve service worker file."""
    return send_from_directory('static', 'sw.js')

@main.route('/favicon.ico')
def favicon():
    """Serve favicon or return 204 if not found."""
    try:
        return send_from_directory('static/images', 'favicon.ico')
    except:
        # Return empty response if favicon doesn't exist
        return '', 204

# API Routes

@api.route('/gems')
def get_gems():
    """Get current hidden gems."""
    try:
        # Get parameters
        limit = min(int(request.args.get('limit', 50)), 100)
        karma_threshold = int(request.args.get('karma_threshold', 50))
        min_score = float(request.args.get('min_score', 0.5))
        hours = int(request.args.get('hours', 24))
        
        # Get recent hidden gems
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        
        gems = Post.query.join(QualityScore).filter(
            Post.is_hidden_gem == True,
            Post.is_spam == False,
            Post.author_karma < karma_threshold,
            Post.created_at >= cutoff_time,
            QualityScore.overall_interest >= min_score
        ).order_by(QualityScore.overall_interest.desc()).limit(limit).all()
        
        return jsonify({
            'gems': [post.to_dict() for post in gems],
            'count': len(gems),
            'filters': {
                'karma_threshold': karma_threshold,
                'min_score': min_score,
                'hours': hours,
                'limit': limit
            }
        })
        
    except Exception as e:
        logger.error(f"Error getting gems: {e}")
        return jsonify({'error': 'Failed to fetch gems'}), 500

@api.route('/gems/hall-of-fame')
def get_hall_of_fame():
    """Get Hall of Fame entries."""
    try:
        limit = min(int(request.args.get('limit', 20)), 100)
        days = int(request.args.get('days', 30))
        
        # Get recent successes
        entries = HallOfFame.query.filter(
            HallOfFame.success_verified == True
        ).order_by(HallOfFame.success_at.desc()).limit(limit).all()
        
        return jsonify({
            'entries': [entry.to_dict() for entry in entries],
            'count': len(entries)
        })
        
    except Exception as e:
        logger.error(f"Error getting hall of fame: {e}")
        return jsonify({'error': 'Failed to fetch hall of fame'}), 500

@api.route('/stats')
def get_stats():
    """Get system statistics."""
    try:
        # Get various statistics
        post_stats = Post.get_stats()
        quality_stats = QualityScore.get_analysis_stats()
        hall_stats = HallOfFame.get_stats()
        
        # Get recent activity
        recent_cutoff = datetime.utcnow() - timedelta(hours=24)
        recent_posts = Post.query.filter(Post.created_at >= recent_cutoff).count()
        recent_gems = Post.query.filter(
            Post.created_at >= recent_cutoff,
            Post.is_hidden_gem == True
        ).count()
        
        # Top authors
        top_authors = User.query.filter(
            User.hidden_gems_count > 0
        ).order_by(User.hidden_gems_count.desc()).limit(10).all()
        
        return jsonify({
            'posts': post_stats,
            'quality': quality_stats,
            'hall_of_fame': hall_stats,
            'recent_activity': {
                'posts_24h': recent_posts,
                'gems_24h': recent_gems
            },
            'top_authors': [user.to_dict() for user in top_authors],
            'generated_at': datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return jsonify({'error': 'Failed to fetch statistics'}), 500

@api.route('/posts/<int:hn_id>')
def get_post(hn_id):
    """Get specific post by HN ID."""
    try:
        post = Post.find_by_hn_id(hn_id)
        if not post:
            return jsonify({'error': 'Post not found'}), 404
        
        return jsonify(post.to_dict())
        
    except Exception as e:
        logger.error(f"Error getting post {hn_id}: {e}")
        return jsonify({'error': 'Failed to fetch post'}), 500

@api.route('/users/<username>')
def get_user(username):
    """Get user information."""
    try:
        user = User.query.filter_by(username=username).first()
        if not user:
            return jsonify({'error': 'User not found'}), 404
        
        # Get user's posts
        posts = user.posts.order_by(Post.created_at.desc()).limit(20).all()
        
        user_data = user.to_dict()
        user_data['recent_posts'] = [post.to_dict() for post in posts]
        
        return jsonify(user_data)
        
    except Exception as e:
        logger.error(f"Error getting user {username}: {e}")
        return jsonify({'error': 'Failed to fetch user'}), 500

@api.route('/search')
def search_posts():
    """Search posts by title or content."""
    try:
        query = request.args.get('q', '').strip()
        if not query:
            return jsonify({'error': 'Query parameter required'}), 400
        
        limit = min(int(request.args.get('limit', 20)), 100)
        
        # Simple text search (can be enhanced with full-text search)
        posts = Post.query.filter(
            Post.title.ilike(f'%{query}%') | 
            Post.text.ilike(f'%{query}%')
        ).order_by(Post.created_at.desc()).limit(limit).all()
        
        return jsonify({
            'posts': [post.to_dict() for post in posts],
            'count': len(posts),
            'query': query
        })
        
    except Exception as e:
        logger.error(f"Error searching posts: {e}")
        return jsonify({'error': 'Search failed'}), 500

@api.route('/feed.xml')
def rss_feed():
    """RSS feed of hidden gems."""
    try:
        from feedgen.feed import FeedGenerator
        
        # Create feed
        fg = FeedGenerator()
        fg.title('HN Hidden Gems')
        fg.description('High-quality Hacker News posts from low-karma accounts')
        fg.link(href=request.url_root, rel='alternate')
        fg.link(href=request.url, rel='self')
        fg.id(request.url_root)
        fg.language('en')
        
        # Get recent gems
        gems = Post.query.join(QualityScore).filter(
            Post.is_hidden_gem == True,
            Post.is_spam == False,
            Post.created_at >= datetime.utcnow() - timedelta(days=7)
        ).order_by(Post.created_at.desc()).limit(50).all()
        
        # Add entries
        for post in gems:
            fe = fg.add_entry()
            fe.id(post.hn_url)
            fe.title(post.title)
            fe.link(href=post.url or post.hn_url)
            fe.description(f"""
                <p><strong>Hidden Gem Discovered!</strong></p>
                <p>Author: {post.author} (karma: {post.author_karma})</p>
                <p>Quality Score: {post.quality_score.overall_interest:.2f}</p>
                <p><a href="{post.hn_url}">View on Hacker News</a></p>
                {f'<p>{post.text[:200]}...</p>' if post.text else ''}
            """)
            fe.pubDate(post.created_at)
            fe.author({'name': f'HN User: {post.author}'})
        
        response = current_app.response_class(
            fg.rss_str(pretty=True),
            mimetype='application/rss+xml'
        )
        return response
        
    except Exception as e:
        logger.error(f"Error generating RSS feed: {e}")
        return jsonify({'error': 'Failed to generate RSS feed'}), 500

@api.route('/health')
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'version': '0.1.0'
    })