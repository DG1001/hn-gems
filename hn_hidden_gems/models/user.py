from datetime import datetime
from .database import db

class User(db.Model):
    """Represents a Hacker News user."""
    
    __tablename__ = 'users'
    
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False, index=True)
    karma = db.Column(db.Integer, default=0, index=True)
    hn_created_at = db.Column(db.DateTime)  # When user joined HN
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False, index=True)  # When we first saw them
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # User statistics
    total_posts = db.Column(db.Integer, default=0)
    hidden_gems_count = db.Column(db.Integer, default=0)
    hall_of_fame_count = db.Column(db.Integer, default=0)
    
    # Tracking
    last_checked_at = db.Column(db.DateTime)
    is_monitored = db.Column(db.Boolean, default=False)  # Flag for users we want to track closely
    
    # Relationships
    # Note: Post relationship temporarily removed to avoid SQLAlchemy issues
    
    def __repr__(self):
        return f'<User {self.username} (karma: {self.karma})>'
    
    @property
    def account_age_days(self):
        """Get account age in days."""
        if not self.hn_created_at:
            return 0
        return (datetime.utcnow() - self.hn_created_at).days
    
    @property
    def success_rate(self):
        """Get success rate (hall of fame / hidden gems)."""
        if self.hidden_gems_count == 0:
            return 0
        return (self.hall_of_fame_count / self.hidden_gems_count) * 100
    
    def update_from_hn_data(self, hn_data):
        """Update user data from HN API response."""
        self.karma = hn_data.get('karma', self.karma)
        
        if 'created' in hn_data:
            self.hn_created_at = datetime.fromtimestamp(hn_data['created'])
        
        self.last_checked_at = datetime.utcnow()
    
    def update_stats(self):
        """Update user statistics from posts."""
        from .post import Post
        from .hall_of_fame import HallOfFame
        
        self.total_posts = Post.query.filter_by(author=self.username).count()
        self.hidden_gems_count = Post.query.filter_by(author=self.username, is_hidden_gem=True).count()
        
        # Count hall of fame entries
        self.hall_of_fame_count = HallOfFame.query.join(
            HallOfFame.post
        ).filter(Post.author == self.username).count()
    
    def to_dict(self):
        """Convert user to dictionary for API responses."""
        return {
            'id': self.id,
            'username': self.username,
            'karma': self.karma,
            'account_age_days': self.account_age_days,
            'hn_created_at': self.hn_created_at.isoformat() if self.hn_created_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'total_posts': self.total_posts,
            'hidden_gems_count': self.hidden_gems_count,
            'hall_of_fame_count': self.hall_of_fame_count,
            'success_rate': self.success_rate,
            'is_monitored': self.is_monitored,
            'hn_profile_url': f"https://news.ycombinator.com/user?id={self.username}"
        }
    
    @classmethod
    def find_or_create(cls, username, hn_data=None):
        """Find existing user or create new one."""
        user = cls.query.filter_by(username=username).first()
        
        if not user:
            user = cls(username=username)
            if hn_data:
                user.update_from_hn_data(hn_data)
            db.session.add(user)
        elif hn_data:
            user.update_from_hn_data(hn_data)
        
        return user
    
    @classmethod
    def get_low_karma_users(cls, karma_threshold=50):
        """Get users with karma below threshold."""
        return cls.query.filter(cls.karma < karma_threshold).all()
    
    @classmethod
    def get_rising_stars(cls, limit=20):
        """Get users who have produced multiple hidden gems."""
        return cls.query.filter(
            cls.hidden_gems_count >= 2
        ).order_by(cls.success_rate.desc()).limit(limit).all()
    
    @classmethod
    def get_monitoring_candidates(cls, min_gems=1, max_karma=100):
        """Get users who might be worth monitoring closely."""
        return cls.query.filter(
            cls.hidden_gems_count >= min_gems,
            cls.karma <= max_karma,
            cls.is_monitored == False
        ).order_by(cls.hidden_gems_count.desc()).all()