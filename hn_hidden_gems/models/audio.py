from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text, Float, ForeignKey
from sqlalchemy.orm import relationship
from hn_hidden_gems.models.database import db

class AudioMetadata(db.Model):
    """
    Model for storing podcast audio metadata
    """
    __tablename__ = 'audio_metadata'
    
    id = Column(Integer, primary_key=True)
    
    # Basic metadata
    filename = Column(String(255), nullable=False, unique=True, index=True)
    file_path = Column(String(500), nullable=False)
    file_size_bytes = Column(Integer)
    
    # Generation metadata
    generation_timestamp = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    script_source = Column(String(100))  # e.g., 'super-gems', 'manual'
    script_length = Column(Integer)  # Characters in original script
    prepared_text_length = Column(Integer)  # Characters after TTS optimization
    
    # Audio configuration
    language_code = Column(String(10), default='en-US')
    voice_name = Column(String(50))
    audio_encoding = Column(String(10), default='MP3')
    
    # Duration and quality
    estimated_duration_minutes = Column(Integer)
    actual_duration_seconds = Column(Float)  # If we can extract it from audio
    
    # Status tracking
    generation_status = Column(String(20), default='pending', index=True)  # pending, generating, completed, failed
    generation_error = Column(Text)  # Store error message if generation failed
    
    # Content metadata
    gems_count = Column(Integer, default=0)  # Number of gems in this podcast
    content_date = Column(DateTime, index=True)  # Date the content refers to
    
    # TTS service metadata
    tts_service = Column(String(50), default='google_cloud_tts')
    estimated_cost_usd = Column(Float)
    
    # Usage tracking
    download_count = Column(Integer, default=0)
    last_accessed = Column(DateTime)
    
    # Relationships
    # Link to quality scores or posts if needed in future
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    def __repr__(self):
        return f'<AudioMetadata {self.filename}>'
    
    @classmethod
    def create_entry(cls, filename, file_path, metadata_dict):
        """
        Create a new audio metadata entry
        
        Args:
            filename: Audio file name (without path)
            file_path: Full path to the audio file
            metadata_dict: Dictionary with additional metadata
            
        Returns:
            AudioMetadata instance
        """
        audio_entry = cls(
            filename=filename,
            file_path=file_path,
            file_size_bytes=metadata_dict.get('file_size_bytes'),
            script_source=metadata_dict.get('script_source', 'super-gems'),
            script_length=metadata_dict.get('script_length'),
            prepared_text_length=metadata_dict.get('prepared_text_length'),
            language_code=metadata_dict.get('language_code', 'en-US'),
            voice_name=metadata_dict.get('voice_name'),
            audio_encoding=metadata_dict.get('audio_encoding', 'MP3'),
            estimated_duration_minutes=metadata_dict.get('estimated_duration_minutes'),
            generation_status='completed',
            gems_count=metadata_dict.get('gems_count', 0),
            content_date=datetime.fromisoformat(metadata_dict['generated_at']) if metadata_dict.get('generated_at') else None,
            tts_service=metadata_dict.get('tts_service', 'google_cloud_tts'),
            estimated_cost_usd=metadata_dict.get('estimated_cost_usd')
        )
        
        db.session.add(audio_entry)
        return audio_entry
    
    @classmethod
    def find_by_filename(cls, filename):
        """Find audio metadata by filename"""
        return cls.query.filter_by(filename=filename).first()
    
    @classmethod
    def find_latest(cls, script_source='super-gems'):
        """Find the most recent audio file for a given source"""
        return cls.query.filter_by(
            script_source=script_source,
            generation_status='completed'
        ).order_by(cls.generation_timestamp.desc()).first()
    
    @classmethod
    def find_by_date_range(cls, start_date, end_date, script_source='super-gems'):
        """Find audio files within a date range"""
        return cls.query.filter(
            cls.script_source == script_source,
            cls.generation_status == 'completed',
            cls.content_date >= start_date,
            cls.content_date <= end_date
        ).order_by(cls.content_date.desc()).all()
    
    @classmethod
    def get_recent(cls, limit=10, script_source='super-gems'):
        """Get recent audio files"""
        return cls.query.filter_by(
            script_source=script_source,
            generation_status='completed'
        ).order_by(cls.generation_timestamp.desc()).limit(limit).all()
    
    def update_status(self, status, error=None):
        """Update generation status"""
        self.generation_status = status
        if error:
            self.generation_error = error
        self.updated_at = datetime.utcnow()
        db.session.commit()
    
    def record_access(self):
        """Record that this audio file was accessed"""
        self.download_count += 1
        self.last_accessed = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        db.session.commit()
    
    def set_actual_duration(self, duration_seconds):
        """Set the actual duration if we can determine it"""
        self.actual_duration_seconds = duration_seconds
        self.updated_at = datetime.utcnow()
        db.session.commit()
    
    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        return {
            'id': self.id,
            'filename': self.filename,
            'file_path': self.file_path,
            'file_size_bytes': self.file_size_bytes,
            'generation_timestamp': self.generation_timestamp.isoformat() if self.generation_timestamp else None,
            'script_source': self.script_source,
            'language_code': self.language_code,
            'voice_name': self.voice_name,
            'audio_encoding': self.audio_encoding,
            'estimated_duration_minutes': self.estimated_duration_minutes,
            'actual_duration_seconds': self.actual_duration_seconds,
            'generation_status': self.generation_status,
            'gems_count': self.gems_count,
            'content_date': self.content_date.isoformat() if self.content_date else None,
            'download_count': self.download_count,
            'last_accessed': self.last_accessed.isoformat() if self.last_accessed else None,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }

class PodcastScript(db.Model):
    """
    Model for storing generated podcast scripts (before audio generation)
    """
    __tablename__ = 'podcast_scripts'
    
    id = Column(Integer, primary_key=True)
    
    # Script content
    script_text = Column(Text, nullable=False)
    script_hash = Column(String(64), unique=True, index=True)  # SHA-256 hash for deduplication
    
    # Generation metadata
    generation_timestamp = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    script_source = Column(String(100), default='super-gems')
    gems_count = Column(Integer, default=0)
    total_words = Column(Integer)
    estimated_duration_minutes = Column(Integer)
    
    # Source data reference
    source_data_file = Column(String(255))  # e.g., 'super-gems.json'
    source_data_timestamp = Column(DateTime)
    
    # Generation details
    generator_model = Column(String(50), default='gemini-2.5-flash-lite')
    generation_config = Column(Text)  # JSON string of generation config
    
    # Audio generation tracking
    audio_generated = Column(Boolean, default=False, index=True)
    audio_generation_timestamp = Column(DateTime)
    audio_metadata_id = Column(Integer, ForeignKey('audio_metadata.id'))
    
    # Relationship
    audio_metadata = relationship('AudioMetadata', backref='podcast_script')
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    def __repr__(self):
        return f'<PodcastScript {self.id} ({self.gems_count} gems)>'
    
    @classmethod
    def create_from_generator_output(cls, script_data, source_file=None):
        """
        Create a podcast script entry from PodcastGenerator output
        
        Args:
            script_data: Dictionary from PodcastGenerator.generate_podcast_script()
            source_file: Source data file name
            
        Returns:
            PodcastScript instance
        """
        import hashlib
        
        script_text = script_data['script']
        metadata = script_data['metadata']
        
        # Generate hash for deduplication
        script_hash = hashlib.sha256(script_text.encode('utf-8')).hexdigest()
        
        # Check if identical script already exists
        existing = cls.query.filter_by(script_hash=script_hash).first()
        if existing:
            return existing
        
        script_entry = cls(
            script_text=script_text,
            script_hash=script_hash,
            script_source=source_file or 'super-gems',
            gems_count=metadata.get('gems_count', 0),
            total_words=metadata.get('total_words'),
            estimated_duration_minutes=metadata.get('estimated_duration_minutes'),
            source_data_file=source_file,
            generator_model='gemini-2.5-flash-lite'
        )
        
        db.session.add(script_entry)
        return script_entry
    
    @classmethod
    def find_latest(cls, script_source='super-gems'):
        """Find the most recent script"""
        return cls.query.filter_by(script_source=script_source).order_by(
            cls.generation_timestamp.desc()
        ).first()
    
    @classmethod
    def find_unprocessed_for_audio(cls):
        """Find scripts that haven't been converted to audio yet"""
        return cls.query.filter_by(audio_generated=False).order_by(
            cls.generation_timestamp.asc()
        ).all()
    
    def mark_audio_generated(self, audio_metadata):
        """Mark this script as having audio generated"""
        self.audio_generated = True
        self.audio_generation_timestamp = datetime.utcnow()
        self.audio_metadata = audio_metadata
        self.updated_at = datetime.utcnow()
        db.session.commit()
    
    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        return {
            'id': self.id,
            'generation_timestamp': self.generation_timestamp.isoformat(),
            'script_source': self.script_source,
            'gems_count': self.gems_count,
            'total_words': self.total_words,
            'estimated_duration_minutes': self.estimated_duration_minutes,
            'source_data_file': self.source_data_file,
            'audio_generated': self.audio_generated,
            'audio_generation_timestamp': self.audio_generation_timestamp.isoformat() if self.audio_generation_timestamp else None,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }