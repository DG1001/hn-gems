from datetime import datetime
from .database import db

class QualityScore(db.Model):
    """Quality scores for posts."""
    
    __tablename__ = 'quality_scores'
    
    id = db.Column(db.Integer, primary_key=True)
    post_id = db.Column(db.Integer, db.ForeignKey('posts.id'), nullable=False, index=True)
    
    # Core quality metrics
    technical_depth = db.Column(db.Float, default=0.0)
    originality = db.Column(db.Float, default=0.0)  
    problem_solving = db.Column(db.Float, default=0.0)
    spam_likelihood = db.Column(db.Float, default=0.0, index=True)
    overall_interest = db.Column(db.Float, default=0.0, index=True)
    
    # Additional metrics
    github_quality = db.Column(db.Float, default=0.0)
    domain_reputation = db.Column(db.Float, default=0.0)
    
    # Analysis metadata
    analyzer_version = db.Column(db.String(20), default='1.0')
    analyzed_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    analysis_time_ms = db.Column(db.Integer)  # Time taken for analysis
    
    # Manual overrides (for training/improvement)
    manual_override = db.Column(db.Boolean, default=False)
    manual_score = db.Column(db.Float)
    manual_notes = db.Column(db.Text)
    manual_updated_at = db.Column(db.DateTime)
    manual_updated_by = db.Column(db.String(50))
    
    # Relationships
    post = db.relationship('Post', back_populates='quality_score')
    
    def __repr__(self):
        return f'<QualityScore {self.post_id}: {self.overall_interest:.2f}>'
    
    @property
    def effective_score(self):
        """Get the effective score (manual override if available, otherwise calculated)."""
        return self.manual_score if self.manual_override and self.manual_score is not None else self.overall_interest
    
    @property
    def is_likely_gem(self):
        """Check if this is likely a hidden gem based on scores."""
        return (
            self.effective_score >= 0.5 and
            self.spam_likelihood < 0.3 and
            self.technical_depth > 0.2
        )
    
    @property
    def confidence_level(self):
        """Get confidence level of the analysis."""
        # Higher confidence for posts with clear signals
        if self.spam_likelihood > 0.7:
            return 'high'  # High confidence it's spam
        elif self.overall_interest > 0.8 and self.spam_likelihood < 0.2:
            return 'high'  # High confidence it's good
        elif self.overall_interest > 0.6 and self.spam_likelihood < 0.3:
            return 'medium'
        else:
            return 'low'
    
    def update_scores(self, scores_dict, analysis_time_ms=None):
        """Update scores from analyzer results."""
        self.technical_depth = scores_dict.get('technical_depth', 0.0)
        self.originality = scores_dict.get('originality', 0.0)
        self.problem_solving = scores_dict.get('problem_solving', 0.0)
        self.spam_likelihood = scores_dict.get('spam_likelihood', 0.0)
        self.overall_interest = scores_dict.get('overall_interest', 0.0)
        self.github_quality = scores_dict.get('github_quality', 0.0)
        self.domain_reputation = scores_dict.get('domain_reputation', 0.0)
        
        self.analyzed_at = datetime.utcnow()
        if analysis_time_ms:
            self.analysis_time_ms = analysis_time_ms
    
    def add_manual_override(self, score, notes, updated_by):
        """Add manual score override."""
        self.manual_override = True
        self.manual_score = score
        self.manual_notes = notes
        self.manual_updated_by = updated_by
        self.manual_updated_at = datetime.utcnow()
    
    def to_dict(self):
        """Convert quality score to dictionary for API responses."""
        return {
            'id': self.id,
            'post_id': self.post_id,
            'technical_depth': self.technical_depth,
            'originality': self.originality,
            'problem_solving': self.problem_solving,
            'spam_likelihood': self.spam_likelihood,
            'overall_interest': self.overall_interest,
            'github_quality': self.github_quality,
            'domain_reputation': self.domain_reputation,
            'effective_score': self.effective_score,
            'is_likely_gem': self.is_likely_gem,
            'confidence_level': self.confidence_level,
            'analyzer_version': self.analyzer_version,
            'analyzed_at': self.analyzed_at.isoformat() if self.analyzed_at else None,
            'analysis_time_ms': self.analysis_time_ms,
            'manual_override': self.manual_override,
            'manual_score': self.manual_score,
            'manual_notes': self.manual_notes,
            'manual_updated_at': self.manual_updated_at.isoformat() if self.manual_updated_at else None,
            'manual_updated_by': self.manual_updated_by
        }
    
    @classmethod
    def get_high_quality_posts(cls, min_score=0.7, limit=50):
        """Get posts with high quality scores."""
        return cls.query.filter(
            cls.overall_interest >= min_score,
            cls.spam_likelihood < 0.3
        ).order_by(cls.overall_interest.desc()).limit(limit).all()
    
    @classmethod
    def get_spam_posts(cls, min_spam_likelihood=0.7, limit=50):
        """Get posts likely to be spam."""
        return cls.query.filter(
            cls.spam_likelihood >= min_spam_likelihood
        ).order_by(cls.spam_likelihood.desc()).limit(limit).all()
    
    @classmethod
    def get_analysis_stats(cls):
        """Get analysis statistics."""
        from sqlalchemy import func
        
        stats = db.session.query(
            func.count(cls.id).label('total_analyzed'),
            func.avg(cls.overall_interest).label('avg_interest'),
            func.avg(cls.spam_likelihood).label('avg_spam'),
            func.count(cls.id).filter(cls.overall_interest >= 0.7).label('high_quality'),
            func.count(cls.id).filter(cls.spam_likelihood >= 0.7).label('spam_count'),
            func.count(cls.id).filter(cls.manual_override == True).label('manual_overrides')
        ).first()
        
        return {
            'total_analyzed': stats.total_analyzed or 0,
            'avg_interest_score': round(stats.avg_interest or 0, 3),
            'avg_spam_likelihood': round(stats.avg_spam or 0, 3),
            'high_quality_count': stats.high_quality or 0,
            'spam_count': stats.spam_count or 0,
            'manual_overrides': stats.manual_overrides or 0
        }