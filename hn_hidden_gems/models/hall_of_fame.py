from datetime import datetime
from .database import db

class HallOfFame(db.Model):
    """Tracks hidden gems that later became successful."""
    
    __tablename__ = 'hall_of_fame'
    
    id = db.Column(db.Integer, primary_key=True)
    post_id = db.Column(db.Integer, db.ForeignKey('posts.id'), nullable=False, unique=True, index=True)
    
    # Discovery information
    discovered_at = db.Column(db.DateTime, nullable=False, index=True)  # When we first identified it as a gem
    discovery_score = db.Column(db.Float, nullable=False)  # Our quality score at discovery
    discovery_hn_score = db.Column(db.Integer, default=0)  # HN score when discovered
    discovery_karma = db.Column(db.Integer, default=0)  # Author karma at discovery
    
    # Success information  
    success_at = db.Column(db.DateTime, index=True)  # When it reached success threshold
    success_hn_score = db.Column(db.Integer)  # HN score at success
    peak_hn_score = db.Column(db.Integer)  # Highest HN score observed
    success_threshold = db.Column(db.Integer, default=100)  # Score threshold that defined "success"
    
    # Timing metrics
    lead_time_hours = db.Column(db.Float)  # Hours between discovery and success
    hn_age_at_discovery_hours = db.Column(db.Float)  # How old the post was when we discovered it
    
    # Success categorization
    success_type = db.Column(db.String(20))  # 'front_page', 'top_10', 'viral', etc.
    success_verified = db.Column(db.Boolean, default=False)
    
    # Additional metadata
    notes = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    post = db.relationship('Post', back_populates='hall_of_fame_entry')
    
    def __repr__(self):
        return f'<HallOfFame {self.post_id}: {self.discovery_hn_score} -> {self.success_hn_score}>'
    
    @property
    def score_improvement(self):
        """Calculate score improvement from discovery to success."""
        if not self.success_hn_score or not self.discovery_hn_score:
            return 0
        return self.success_hn_score - self.discovery_hn_score
    
    @property
    def score_multiplier(self):
        """Calculate score multiplier from discovery to success."""
        if not self.success_hn_score or self.discovery_hn_score <= 0:
            return 0
        return self.success_hn_score / max(self.discovery_hn_score, 1)
    
    @property
    def discovery_quality(self):
        """Rate the quality of our early discovery."""
        if not self.hn_age_at_discovery_hours:
            return 'unknown'
        elif self.hn_age_at_discovery_hours < 2:
            return 'excellent'  # Caught within 2 hours
        elif self.hn_age_at_discovery_hours < 6:
            return 'very_good'  # Caught within 6 hours  
        elif self.hn_age_at_discovery_hours < 12:
            return 'good'       # Caught within 12 hours
        else:
            return 'late'       # Caught after 12+ hours
    
    def update_success_metrics(self, current_hn_score, threshold=100):
        """Update success metrics when post reaches threshold."""
        if not self.success_at and current_hn_score >= threshold:
            self.success_at = datetime.utcnow()
            self.success_hn_score = current_hn_score
            self.success_threshold = threshold
            self.success_verified = True
            
            # Calculate timing metrics
            if self.discovered_at:
                self.lead_time_hours = (self.success_at - self.discovered_at).total_seconds() / 3600
            
            # Determine success type
            if current_hn_score >= 500:
                self.success_type = 'viral'
            elif current_hn_score >= 200:
                self.success_type = 'front_page'
            elif current_hn_score >= 100:
                self.success_type = 'top_100'
            
            self.updated_at = datetime.utcnow()
        
        # Always update peak score
        if current_hn_score > (self.peak_hn_score or 0):
            self.peak_hn_score = current_hn_score
    
    def to_dict(self):
        """Convert hall of fame entry to dictionary for API responses."""
        return {
            'id': self.id,
            'post_id': self.post_id,
            'discovered_at': self.discovered_at.isoformat() if self.discovered_at else None,
            'discovery_score': self.discovery_score,
            'discovery_hn_score': self.discovery_hn_score,
            'discovery_karma': self.discovery_karma,
            'success_at': self.success_at.isoformat() if self.success_at else None,
            'success_hn_score': self.success_hn_score,
            'peak_hn_score': self.peak_hn_score,
            'success_threshold': self.success_threshold,
            'lead_time_hours': self.lead_time_hours,
            'hn_age_at_discovery_hours': self.hn_age_at_discovery_hours,
            'success_type': self.success_type,
            'success_verified': self.success_verified,
            'score_improvement': self.score_improvement,
            'score_multiplier': self.score_multiplier,
            'discovery_quality': self.discovery_quality,
            'notes': self.notes,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'post': self.post.to_dict() if self.post else None
        }
    
    @classmethod
    def create_entry(cls, post, quality_score, hn_age_hours=None):
        """Create a new hall of fame entry for a discovered gem."""
        entry = cls(
            post_id=post.id,
            discovered_at=datetime.utcnow(),
            discovery_score=quality_score.effective_score,
            discovery_hn_score=post.current_hn_score or post.score,
            discovery_karma=post.author_karma,
            hn_age_at_discovery_hours=hn_age_hours or post.age_hours
        )
        
        db.session.add(entry)
        return entry
    
    @classmethod
    def get_recent_successes(cls, days=30, limit=20):
        """Get recent success stories."""
        from datetime import timedelta
        cutoff = datetime.utcnow() - timedelta(days=days)
        
        return cls.query.filter(
            cls.success_at >= cutoff,
            cls.success_verified == True
        ).order_by(cls.success_at.desc()).limit(limit).all()
    
    @classmethod
    def get_best_discoveries(cls, limit=20):
        """Get best discoveries (highest score multipliers)."""
        return cls.query.filter(
            cls.success_verified == True,
            cls.discovery_hn_score > 0
        ).order_by(
            (cls.success_hn_score / cls.discovery_hn_score).desc()
        ).limit(limit).all()
    
    @classmethod
    def get_stats(cls):
        """Get hall of fame statistics."""
        from sqlalchemy import func
        
        stats = db.session.query(
            func.count(cls.id).label('total_entries'),
            func.count(cls.id).filter(cls.success_verified == True).label('verified_successes'),
            func.avg(cls.lead_time_hours).label('avg_lead_time'),
            func.avg(cls.success_hn_score / func.nullif(cls.discovery_hn_score, 0)).label('avg_multiplier'),
            func.count(cls.id).filter(cls.hn_age_at_discovery_hours < 6).label('early_discoveries')
        ).first()
        
        return {
            'total_entries': stats.total_entries or 0,
            'verified_successes': stats.verified_successes or 0,
            'avg_lead_time_hours': round(stats.avg_lead_time or 0, 1),
            'avg_score_multiplier': round(stats.avg_multiplier or 0, 1),
            'early_discoveries': stats.early_discoveries or 0,
            'success_rate': (stats.verified_successes / stats.total_entries * 100) if stats.total_entries else 0
        }