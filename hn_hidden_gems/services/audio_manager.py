import os
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from hn_hidden_gems.models import AudioMetadata, PodcastScript, db
from hn_hidden_gems.services.podcast_generator import PodcastGenerator
from hn_hidden_gems.services.audio_service import AudioService

class AudioManager:
    """
    High-level manager for audio file operations, cleanup, and organization
    """
    
    def __init__(self, 
                 audio_storage_path: str = "static/audio",
                 gemini_api_key: Optional[str] = None):
        """
        Initialize the audio manager
        
        Args:
            audio_storage_path: Directory for audio file storage
            gemini_api_key: API key for Gemini (for script generation)
        """
        self.audio_storage_path = Path(audio_storage_path)
        self.audio_storage_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize services
        self.podcast_generator = PodcastGenerator(gemini_api_key) if gemini_api_key else None
        self.audio_service = AudioService(audio_storage_path=str(audio_storage_path))
        
        self.logger = logging.getLogger(__name__)
    
    def generate_complete_podcast(self, super_gems_data: Dict[str, Any], 
                                save_to_db: bool = True) -> Dict[str, Any]:
        """
        Complete podcast generation pipeline: script + audio + database
        
        Args:
            super_gems_data: Dictionary with gems and metadata
            save_to_db: Whether to save metadata to database
            
        Returns:
            Dictionary with generation results
        """
        if not self.podcast_generator:
            return {
                "success": False,
                "error": "Podcast generator not initialized (missing Gemini API key)"
            }
        
        try:
            # Step 1: Generate podcast script
            self.logger.info("Generating podcast script...")
            script_data = self.podcast_generator.generate_podcast_script(super_gems_data)
            
            if not script_data or not script_data.get('script'):
                return {
                    "success": False,
                    "error": "Failed to generate podcast script"
                }
            
            # Step 2: Save script to database if requested
            podcast_script = None
            if save_to_db:
                try:
                    podcast_script = PodcastScript.create_from_generator_output(
                        script_data, 'super-gems'
                    )
                    db.session.commit()
                    self.logger.info(f"Saved podcast script to database: {podcast_script.id}")
                except Exception as e:
                    self.logger.error(f"Failed to save script to database: {e}")
                    db.session.rollback()
            
            # Step 3: Generate audio
            self.logger.info("Generating audio from script...")
            date_str = datetime.now().strftime('%Y-%m-%d')
            audio_result = self.audio_service.generate_podcast_audio(script_data, date_str)
            
            if not audio_result['success']:
                return {
                    "success": False,
                    "error": f"Audio generation failed: {audio_result.get('error', 'Unknown error')}",
                    "script_generated": True,
                    "script_data": script_data
                }
            
            # Step 4: Save audio metadata to database if requested
            audio_metadata = None
            if save_to_db and audio_result['success']:
                try:
                    # Create audio metadata entry
                    metadata_dict = audio_result['metadata'].copy()
                    metadata_dict['script_source'] = 'super-gems'
                    
                    audio_metadata = AudioMetadata.create_entry(
                        filename=Path(audio_result['audio_path']).name,
                        file_path=audio_result['audio_path'],
                        metadata_dict=metadata_dict
                    )
                    
                    # Link script and audio if both exist
                    if podcast_script:
                        podcast_script.mark_audio_generated(audio_metadata)
                    
                    db.session.commit()
                    self.logger.info(f"Saved audio metadata to database: {audio_metadata.id}")
                    
                except Exception as e:
                    self.logger.error(f"Failed to save audio metadata to database: {e}")
                    db.session.rollback()
            
            return {
                "success": True,
                "script_data": script_data,
                "audio_path": audio_result['audio_path'],
                "metadata_path": audio_result.get('metadata_path'),
                "cached": audio_result.get('cached', False),
                "database_entries": {
                    "script_id": podcast_script.id if podcast_script else None,
                    "audio_id": audio_metadata.id if audio_metadata else None
                },
                "file_size_mb": audio_result['metadata']['file_size_bytes'] / (1024 * 1024),
                "estimated_duration_minutes": audio_result['metadata']['estimated_duration_minutes']
            }
            
        except Exception as e:
            self.logger.error(f"Complete podcast generation failed: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def cleanup_old_files(self, max_age_days: int = 30, dry_run: bool = False) -> Dict[str, Any]:
        """
        Clean up old audio files and database entries
        
        Args:
            max_age_days: Files older than this will be deleted
            dry_run: If True, just report what would be deleted
            
        Returns:
            Dictionary with cleanup results
        """
        cutoff_time = datetime.now() - timedelta(days=max_age_days)
        results = {
            "files_deleted": 0,
            "files_size_freed": 0,
            "db_entries_cleaned": 0,
            "errors": [],
            "dry_run": dry_run
        }
        
        try:
            # Find old audio metadata entries
            old_audio_entries = AudioMetadata.query.filter(
                AudioMetadata.generation_timestamp < cutoff_time
            ).all()
            
            for audio_entry in old_audio_entries:
                try:
                    # Check if file exists and get size
                    file_path = Path(audio_entry.file_path)
                    file_size = 0
                    
                    if file_path.exists():
                        file_size = file_path.stat().st_size
                        results["files_size_freed"] += file_size
                        
                        if not dry_run:
                            file_path.unlink()
                            self.logger.info(f"Deleted old audio file: {file_path}")
                    
                    # Also delete metadata file if it exists
                    metadata_file = file_path.with_suffix('_metadata.json')
                    if metadata_file.exists() and not dry_run:
                        metadata_file.unlink()
                    
                    # Remove database entry
                    if not dry_run:
                        db.session.delete(audio_entry)
                        results["db_entries_cleaned"] += 1
                    
                    results["files_deleted"] += 1
                    
                except Exception as e:
                    error_msg = f"Error cleaning up {audio_entry.filename}: {e}"
                    self.logger.error(error_msg)
                    results["errors"].append(error_msg)
            
            # Commit database changes
            if not dry_run and results["db_entries_cleaned"] > 0:
                db.session.commit()
            
            # Clean up orphaned files (files without database entries)
            if self.audio_storage_path.exists():
                for file_path in self.audio_storage_path.glob("*.mp3"):
                    # Skip symlinks (like latest.mp3)
                    if file_path.is_symlink():
                        continue
                    
                    # Check if this file has a database entry
                    audio_entry = AudioMetadata.find_by_filename(file_path.name)
                    if not audio_entry and file_path.stat().st_mtime < cutoff_time.timestamp():
                        file_size = file_path.stat().st_size
                        results["files_size_freed"] += file_size
                        
                        if not dry_run:
                            file_path.unlink()
                            self.logger.info(f"Deleted orphaned audio file: {file_path}")
                        
                        results["files_deleted"] += 1
            
            self.logger.info(f"Cleanup completed: {results['files_deleted']} files, "
                           f"{results['files_size_freed'] / (1024*1024):.1f}MB freed, "
                           f"{results['db_entries_cleaned']} DB entries")
            
        except Exception as e:
            error_msg = f"Cleanup failed: {e}"
            self.logger.error(error_msg)
            results["errors"].append(error_msg)
            if not dry_run:
                db.session.rollback()
        
        return results
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """
        Get storage statistics for audio files
        
        Returns:
            Dictionary with storage stats
        """
        stats = {
            "total_files": 0,
            "total_size_bytes": 0,
            "total_size_mb": 0,
            "oldest_file": None,
            "newest_file": None,
            "audio_by_month": {},
            "database_entries": 0
        }
        
        try:
            # Database statistics
            stats["database_entries"] = AudioMetadata.query.count()
            
            # File system statistics
            if self.audio_storage_path.exists():
                for file_path in self.audio_storage_path.glob("*.mp3"):
                    if file_path.is_symlink():
                        continue
                    
                    file_stat = file_path.stat()
                    stats["total_files"] += 1
                    stats["total_size_bytes"] += file_stat.st_size
                    
                    file_date = datetime.fromtimestamp(file_stat.st_mtime)
                    month_key = file_date.strftime("%Y-%m")
                    
                    if month_key not in stats["audio_by_month"]:
                        stats["audio_by_month"][month_key] = {"count": 0, "size_bytes": 0}
                    
                    stats["audio_by_month"][month_key]["count"] += 1
                    stats["audio_by_month"][month_key]["size_bytes"] += file_stat.st_size
                    
                    # Track oldest/newest
                    if stats["oldest_file"] is None or file_date < stats["oldest_file"]:
                        stats["oldest_file"] = file_date
                    
                    if stats["newest_file"] is None or file_date > stats["newest_file"]:
                        stats["newest_file"] = file_date
            
            stats["total_size_mb"] = stats["total_size_bytes"] / (1024 * 1024)
            
            # Convert dates to ISO strings for JSON serialization
            if stats["oldest_file"]:
                stats["oldest_file"] = stats["oldest_file"].isoformat()
            if stats["newest_file"]:
                stats["newest_file"] = stats["newest_file"].isoformat()
            
        except Exception as e:
            self.logger.error(f"Error getting storage stats: {e}")
            stats["error"] = str(e)
        
        return stats
    
    def verify_audio_integrity(self) -> Dict[str, Any]:
        """
        Verify integrity of audio files and database consistency
        
        Returns:
            Dictionary with verification results
        """
        results = {
            "total_checked": 0,
            "missing_files": [],
            "orphaned_files": [],
            "corrupted_files": [],
            "database_inconsistencies": [],
            "healthy_files": 0
        }
        
        try:
            # Check database entries for missing files
            audio_entries = AudioMetadata.query.all()
            
            for entry in audio_entries:
                results["total_checked"] += 1
                file_path = Path(entry.file_path)
                
                if not file_path.exists():
                    results["missing_files"].append({
                        "id": entry.id,
                        "filename": entry.filename,
                        "path": str(file_path)
                    })
                else:
                    # Basic file integrity check
                    try:
                        file_size = file_path.stat().st_size
                        if entry.file_size_bytes and abs(file_size - entry.file_size_bytes) > 1024:
                            results["database_inconsistencies"].append({
                                "id": entry.id,
                                "filename": entry.filename,
                                "issue": f"File size mismatch: DB={entry.file_size_bytes}, Actual={file_size}"
                            })
                        else:
                            results["healthy_files"] += 1
                    except Exception as e:
                        results["corrupted_files"].append({
                            "id": entry.id,
                            "filename": entry.filename,
                            "error": str(e)
                        })
            
            # Check for orphaned files
            if self.audio_storage_path.exists():
                for file_path in self.audio_storage_path.glob("*.mp3"):
                    if file_path.is_symlink():
                        continue
                    
                    audio_entry = AudioMetadata.find_by_filename(file_path.name)
                    if not audio_entry:
                        results["orphaned_files"].append({
                            "filename": file_path.name,
                            "path": str(file_path),
                            "size": file_path.stat().st_size
                        })
            
        except Exception as e:
            self.logger.error(f"Error verifying audio integrity: {e}")
            results["error"] = str(e)
        
        return results
    
    def regenerate_symlinks(self) -> Dict[str, Any]:
        """
        Regenerate symlinks for latest audio files
        
        Returns:
            Dictionary with regeneration results
        """
        results = {
            "symlinks_created": 0,
            "symlinks_updated": 0,
            "errors": []
        }
        
        try:
            # Get latest super gems audio
            latest_audio = AudioMetadata.find_latest('super-gems')
            
            if latest_audio:
                latest_link = self.audio_storage_path / "latest.mp3"
                audio_file = Path(latest_audio.file_path)
                
                # Remove existing symlink
                if latest_link.exists() or latest_link.is_symlink():
                    latest_link.unlink()
                    results["symlinks_updated"] += 1
                else:
                    results["symlinks_created"] += 1
                
                # Create new symlink
                latest_link.symlink_to(audio_file.name)
                self.logger.info(f"Created symlink: {latest_link} -> {audio_file.name}")
            
        except Exception as e:
            error_msg = f"Error regenerating symlinks: {e}"
            self.logger.error(error_msg)
            results["errors"].append(error_msg)
        
        return results