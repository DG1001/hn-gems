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
    
    def to_dict(self):
        """Convert post to dictionary for API responses."""
        return {
            'id': self.id,
            'hn_id': self.hn_id,
            'title': self.title,
            'url': self.url,
            'text': self.text,
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