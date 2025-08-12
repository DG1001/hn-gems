import html
import re
from datetime import datetime, timedelta
from sqlalchemy import func
from .database import db

class Post(db.Model):
    """Represents a Hacker News post."""
    
    __tablename__ = 'posts'
    
    id = db.Column(db.Integer, primary_key=True)
    hn_id = db.Column(db.Integer, unique=True, nullable=False, index=True)
    title = db.Column(db.Text, nullable=False)
    url = db.Column(db.Text)
    text = db.Column(db.Text)
    author = db.Column(db.String(50), nullable=False, index=True)
    author_karma = db.Column(db.Integer, default=0, index=True)
    account_age_days = db.Column(db.Integer, default=0)
    score = db.Column(db.Integer, default=0)
    descendants = db.Column(db.Integer, default=0)  # Number of comments
    
    # Timing information
    hn_created_at = db.Column(db.DateTime, nullable=False)  # When posted to HN
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False, index=True)  # When discovered by us
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Classification
    is_hidden_gem = db.Column(db.Boolean, default=False, index=True)
    is_spam = db.Column(db.Boolean, default=False)
    post_type = db.Column(db.String(20), default='story')  # story, job, poll, etc.
    
    # Status tracking
    current_hn_score = db.Column(db.Integer, default=0)  # Latest score from HN
    last_checked_at = db.Column(db.DateTime)
    
    # Relationships
    quality_score = db.relationship('QualityScore', back_populates='post', uselist=False)
    hall_of_fame_entry = db.relationship('HallOfFame', back_populates='post', uselist=False)
    # Note: User relationship temporarily removed to avoid SQLAlchemy issues
    
    def __repr__(self):
        return f'<Post {self.hn_id}: {self.title[:50]}...>'
    
    @property
    def hn_url(self):
        """Get Hacker News URL for this post."""
        return f"https://news.ycombinator.com/item?id={self.hn_id}"
    
    @property
    def age_hours(self):
        """Get age of post in hours."""
        if not self.hn_created_at:
            return 0
        return (datetime.utcnow() - self.hn_created_at).total_seconds() / 3600
    
    @property
    def discovery_lead_time_hours(self):
        """Get how early we discovered this post (in hours)."""
        if not self.hn_created_at or not self.created_at:
            return 0
        return (self.created_at - self.hn_created_at).total_seconds() / 3600
    
    def update_from_hn_data(self, hn_data):
        """Update post data from HN API response."""
        self.title = hn_data.get('title', self.title)
        self.url = hn_data.get('url', self.url)
        self.text = hn_data.get('text', self.text)
        self.current_hn_score = hn_data.get('score', self.current_hn_score)
        self.descendants = hn_data.get('descendants', self.descendants)
        self.last_checked_at = datetime.utcnow()
        
        # Update HN created time if available
        if 'time' in hn_data:
            self.hn_created_at = datetime.fromtimestamp(hn_data['time'])
    
    def _clean_text(self, text):
        """Clean HTML entities and basic HTML tags from text."""
        if not text:
            return text
            
        # Decode HTML entities (like &#x2F; -> /)
        cleaned = html.unescape(text)
        
        # Remove basic HTML tags but keep the content
        # This handles <p>, <i>, <a>, etc. while preserving line breaks
        cleaned = re.sub(r'<p>', '\n\n', cleaned)  # Convert <p> to double newline
        cleaned = re.sub(r'<br\s*/?>', '\n', cleaned)  # Convert <br> to newline
        cleaned = re.sub(r'<[^>]+>', '', cleaned)  # Remove other HTML tags
        
        # Clean up excessive whitespace but preserve intentional line breaks
        cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)  # Max 2 consecutive newlines
        cleaned = re.sub(r'[ \t]+', ' ', cleaned)  # Multiple spaces to single space
        cleaned = cleaned.strip()
        
        return cleaned
    
    def to_dict(self):
        """Convert post to dictionary for API responses."""
        return {
            'id': self.id,
            'hn_id': self.hn_id,
            'title': self.title,
            'url': self.url,
            'text': self._clean_text(self.text),
            'author': self.author,
            'author_karma': self.author_karma,
            'account_age_days': self.account_age_days,
            'score': self.score,
            'current_hn_score': self.current_hn_score,
            'descendants': self.descendants,
            'hn_created_at': self.hn_created_at.isoformat() if self.hn_created_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'is_hidden_gem': self.is_hidden_gem,
            'is_spam': self.is_spam,
            'post_type': self.post_type,
            'hn_url': self.hn_url,
            'age_hours': self.age_hours,
            'quality_score': self.quality_score.to_dict() if self.quality_score else None,
            'hall_of_fame': {
                'id': self.hall_of_fame_entry.id,
                'discovered_at': self.hall_of_fame_entry.discovered_at.isoformat() if self.hall_of_fame_entry.discovered_at else None,
                'success_at': self.hall_of_fame_entry.success_at.isoformat() if self.hall_of_fame_entry.success_at else None,
                'success_type': self.hall_of_fame_entry.success_type,
                'discovery_quality': self.hall_of_fame_entry.discovery_quality
            } if self.hall_of_fame_entry else None
        }
    
    @classmethod
    def find_by_hn_id(cls, hn_id):
        """Find post by Hacker News ID."""
        return cls.query.filter_by(hn_id=hn_id).first()
    
    @classmethod
    def get_hidden_gems(cls, limit=50, karma_threshold=50, min_interest_score=0.5):
        """Get current hidden gems."""
        return cls.query.join(cls.quality_score).filter(
            cls.is_hidden_gem == True,
            cls.is_spam == False,
            cls.author_karma < karma_threshold,
            cls.quality_score.has(overall_interest__gte=min_interest_score)
        ).order_by(cls.quality_score.overall_interest.desc()).limit(limit).all()
    
    @classmethod
    def get_recent_posts(cls, hours=24, limit=100):
        """Get recent posts within specified hours."""
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return cls.query.filter(
            cls.created_at >= cutoff
        ).order_by(cls.created_at.desc()).limit(limit).all()
    
    @classmethod
    def get_stats(cls):
        """Get statistics about posts."""
        total_posts = cls.query.count()
        hidden_gems = cls.query.filter_by(is_hidden_gem=True).count()
        spam_posts = cls.query.filter_by(is_spam=True).count()
        hall_of_fame_count = cls.query.join(cls.hall_of_fame_entry).count()
        
        return {
            'total_posts': total_posts,
            'hidden_gems': hidden_gems,
            'spam_posts': spam_posts,
            'hall_of_fame_count': hall_of_fame_count,
            'success_rate': (hall_of_fame_count / hidden_gems * 100) if hidden_gems > 0 else 0
        }
    
    @classmethod
    def find_duplicates(cls, limit=1000):
        """Find potential duplicate posts using the duplicate detection system."""
        from hn_hidden_gems.utils.duplicate_detector import DuplicateDetector
        
        # Get recent posts for duplicate detection
        recent_posts = cls.query.order_by(cls.created_at.desc()).limit(limit).all()
        
        # Convert to format expected by duplicate detector
        post_data = []
        for post in recent_posts:
            post_data.append({
                'id': post.id,
                'hn_id': post.hn_id,
                'title': post.title,
                'url': post.url,
                'text': post.text,
                'author': post.author,
                'score': post.score,
                'current_hn_score': post.current_hn_score,
                'hn_created_at': post.hn_created_at,
                'created_at': post.created_at,
                'is_hidden_gem': post.is_hidden_gem
            })
        
        # Find duplicates
        detector = DuplicateDetector()
        duplicates = detector.find_duplicates_in_list(post_data)
        
        return duplicates
    
    @classmethod
    def mark_as_duplicate(cls, post_id: int, duplicate_of_id: int, reason: str = None):
        """
        Mark a post as a duplicate of another post.
        
        Args:
            post_id: ID of the post to mark as duplicate
            duplicate_of_id: ID of the original post
            reason: Optional reason for marking as duplicate
        """
        post = cls.query.get(post_id)
        if post:
            post.is_spam = True  # Mark as spam since it's a duplicate
            post.is_hidden_gem = False  # Remove gem status if it was a gem
            
            # Add a note about being a duplicate (could extend model to have a duplicate_reason field)
            # For now, we'll use the existing spam classification
            db.session.add(post)
            db.session.commit()
            
            return True
        return False
    
    @classmethod
    def get_duplicate_candidates(cls, post):
        """
        Find potential duplicates for a given post.
        
        Args:
            post: Post object to find duplicates for
            
        Returns:
            List of potential duplicate posts with similarity scores
        """
        from hn_hidden_gems.utils.duplicate_detector import DuplicateDetector
        
        detector = DuplicateDetector()
        
        # Get other posts by same author first (most likely to be duplicates)
        same_author_posts = cls.query.filter(
            cls.author == post.author,
            cls.id != post.id
        ).all()
        
        # Get posts with similar URLs if post has URL
        similar_url_posts = []
        if post.url:
            # Get posts with same domain
            domain = detector.normalize_url(post.url).split('/')[2] if '://' in detector.normalize_url(post.url) else ''
            if domain:
                similar_url_posts = cls.query.filter(
                    cls.url.like(f'%{domain}%'),
                    cls.id != post.id
                ).limit(20).all()
        
        # Get recent posts for broader comparison
        recent_posts = cls.query.filter(
            cls.id != post.id,
            cls.created_at >= datetime.utcnow() - timedelta(days=7)
        ).order_by(cls.created_at.desc()).limit(100).all()
        
        # Combine all candidate posts (remove duplicates)
        all_candidates = list({p.id: p for p in (same_author_posts + similar_url_posts + recent_posts)}.values())
        
        # Check each candidate for duplicates
        candidates_with_scores = []
        post_data = {
            'id': post.id,
            'hn_id': post.hn_id,
            'title': post.title,
            'url': post.url,
            'text': post.text,
            'author': post.author,
            'score': post.score,
            'current_hn_score': post.current_hn_score,
            'hn_created_at': post.hn_created_at,
            'created_at': post.created_at
        }
        
        for candidate in all_candidates:
            candidate_data = {
                'id': candidate.id,
                'hn_id': candidate.hn_id,
                'title': candidate.title,
                'url': candidate.url,
                'text': candidate.text,
                'author': candidate.author,
                'score': candidate.score,
                'current_hn_score': candidate.current_hn_score,
                'hn_created_at': candidate.hn_created_at,
                'created_at': candidate.created_at
            }
            
            is_duplicate, similarity = detector.is_duplicate(post_data, candidate_data)
            if is_duplicate:
                recommendation = detector.get_duplicate_action_recommendation(post_data, candidate_data, similarity)
                candidates_with_scores.append({
                    'post': candidate,
                    'similarity': similarity,
                    'recommendation': recommendation
                })
        
        # Sort by confidence score
        candidates_with_scores.sort(key=lambda x: x['similarity']['confidence_score'], reverse=True)
        
        return candidates_with_scores