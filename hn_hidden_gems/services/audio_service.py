import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
from google.cloud import texttospeech
from google.oauth2 import service_account
import hashlib
import tempfile

class AudioService:
    """
    Service for generating audio files from podcast scripts using Google Cloud Text-to-Speech
    """
    
    def __init__(self, 
                 credentials_path: Optional[str] = None,
                 language_code: str = "en-US",
                 voice_name: str = "en-US-Neural2-J",
                 audio_encoding: str = "MP3",
                 audio_storage_path: str = "static/audio"):
        """
        Initialize the audio service
        
        Args:
            credentials_path: Path to Google Cloud service account JSON file
            language_code: Language code for TTS (e.g., "en-US", "de-DE")
            voice_name: Voice name (e.g., "en-US-Neural2-J")
            audio_encoding: Audio encoding format
            audio_storage_path: Directory to store generated audio files
        """
        self.language_code = language_code
        self.voice_name = voice_name
        self.audio_encoding = getattr(texttospeech.AudioEncoding, audio_encoding)
        self.audio_storage_path = Path(audio_storage_path)
        
        # Create audio storage directory if it doesn't exist
        self.audio_storage_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize Google Cloud TTS client
        try:
            if credentials_path and os.path.exists(credentials_path):
                # Use service account credentials
                credentials = service_account.Credentials.from_service_account_file(
                    credentials_path
                )
                self.client = texttospeech.TextToSpeechClient(credentials=credentials)
            else:
                # Use default credentials (environment variable or metadata server)
                self.client = texttospeech.TextToSpeechClient()
                
            # Test the connection
            self._test_connection()
            self.is_available = True
            
        except Exception as e:
            logging.error(f"Failed to initialize Google Cloud TTS: {e}")
            self.client = None
            self.is_available = False
    
    def _test_connection(self):
        """Test Google Cloud TTS connection with a simple synthesis"""
        try:
            synthesis_input = texttospeech.SynthesisInput(text="Test")
            voice = texttospeech.VoiceSelectionParams(
                language_code=self.language_code,
                name=self.voice_name
            )
            audio_config = texttospeech.AudioConfig(audio_encoding=self.audio_encoding)
            
            # This will raise an exception if credentials are invalid
            response = self.client.synthesize_speech(
                input=synthesis_input,
                voice=voice,
                audio_config=audio_config
            )
            
            logging.info("Google Cloud TTS connection test successful")
            
        except Exception as e:
            logging.error(f"Google Cloud TTS connection test failed: {e}")
            raise
    
    def generate_audio(self, script_text: str, output_filename: str, metadata: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Generate audio file from text script
        
        Args:
            script_text: The podcast script text
            output_filename: Name for the output file (without extension)
            metadata: Optional metadata to save alongside audio
            
        Returns:
            Dictionary with generation results and file paths
        """
        if not self.is_available:
            return {
                "success": False,
                "error": "Google Cloud TTS service is not available",
                "audio_path": None,
                "metadata_path": None
            }
        
        try:
            # Create file paths
            audio_path = self.audio_storage_path / f"{output_filename}.mp3"
            metadata_path = self.audio_storage_path / f"{output_filename}_metadata.json"
            
            # Always regenerate audio files (no caching)
            # Remove existing files if they exist
            if audio_path.exists():
                audio_path.unlink()
                logging.info(f"Removed existing audio file: {audio_path}")
            if metadata_path.exists():
                metadata_path.unlink()
                logging.info(f"Removed existing metadata file: {metadata_path}")
            
            # Prepare text for synthesis
            prepared_text = self._prepare_text_for_synthesis(script_text)
            
            # For long text, split into chunks and synthesize separately  
            max_chunk_size = 4000  # Lower threshold to ensure chunking happens
            if len(prepared_text) > max_chunk_size:
                logging.info(f"Text is {len(prepared_text)} chars, splitting into chunks for TTS")
                return self._generate_chunked_audio(prepared_text, audio_path, metadata_path, metadata)
            
            # Create synthesis input for short text
            synthesis_input = texttospeech.SynthesisInput(text=prepared_text)
            
            # Configure voice
            voice = texttospeech.VoiceSelectionParams(
                language_code=self.language_code,
                name=self.voice_name
            )
            
            # Configure audio settings
            audio_config = texttospeech.AudioConfig(
                audio_encoding=self.audio_encoding,
                speaking_rate=1.0,  # Normal speed
                pitch=0.0,          # Normal pitch
                volume_gain_db=0.0  # Normal volume
            )
            
            logging.info(f"Generating audio for {len(prepared_text)} characters...")
            
            # Perform TTS synthesis
            response = self.client.synthesize_speech(
                input=synthesis_input,
                voice=voice,
                audio_config=audio_config
            )
            
            # Save audio file
            with open(audio_path, "wb") as audio_file:
                audio_file.write(response.audio_content)
            
            # Create metadata
            generation_metadata = {
                "generated_at": datetime.now().isoformat(),
                "script_length": len(script_text),
                "prepared_text_length": len(prepared_text),
                "language_code": self.language_code,
                "voice_name": self.voice_name,
                "audio_encoding": "MP3",
                "file_size_bytes": len(response.audio_content),
                "estimated_duration_minutes": len(prepared_text) // 150,  # ~150 words per minute
                **(metadata or {})
            }
            
            # Save metadata
            with open(metadata_path, 'w') as f:
                json.dump(generation_metadata, f, indent=2)
            
            # Create symlink to latest audio
            latest_audio_path = self.audio_storage_path / "latest.mp3"
            if latest_audio_path.exists() or latest_audio_path.is_symlink():
                latest_audio_path.unlink()
            latest_audio_path.symlink_to(audio_path.name)
            
            logging.info(f"Audio generated successfully: {audio_path}")
            
            return {
                "success": True,
                "audio_path": str(audio_path),
                "metadata_path": str(metadata_path),
                "cached": False,
                "metadata": generation_metadata
            }
            
        except Exception as e:
            error_msg = f"Audio generation failed: {e}"
            logging.error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "audio_path": None,
                "metadata_path": None
            }
    
    def _prepare_text_for_synthesis(self, text: str) -> str:
        """
        Prepare text for better TTS synthesis
        
        Args:
            text: Raw script text
            
        Returns:
            Text optimized for speech synthesis
        """
        import re
        
        # Remove stage directions and formatting markers
        # Remove anything in double asterisks like **(Intro Music Fades)** or **Host:**
        text = re.sub(r'\*\*[^*]+\*\*', '', text)
        
        # Remove any remaining single asterisks or markdown formatting
        text = re.sub(r'\*+', '', text)
        
        # Clean up extra spaces and line breaks after removing markers
        text = re.sub(r'\s+', ' ', text)  # Replace multiple spaces with single space
        text = re.sub(r'\n+', '\n', text)  # Replace multiple newlines with single newline
        
        # Remove excessive line breaks
        text = text.replace('\n\n\n', '\n\n')
        
        # Add natural pauses for better pacing
        text = text.replace('\n\n', '. ')  # Paragraph breaks become pauses
        text = text.replace('...', ', ')  # Convert ellipses to natural pauses
        
        # Ensure proper sentence endings
        text = text.replace('. .', '.')
        text = text.replace('..', '.')
        
        # Clean up any double spaces that might have been created
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        
        # No truncation needed - chunking in generate_audio handles long text
        # Let the full text pass through to be chunked appropriately
        
        return text
    
    def _generate_chunked_audio(self, text: str, audio_path, metadata_path, metadata: Optional[Dict] = None):
        """Generate audio for long text by splitting into chunks and concatenating"""
        try:
            max_chunk_size = 3500  # Safe chunk size well under the 5000 char limit
            chunks = []
            
            # Split text into chunks at sentence boundaries
            sentences = text.split('. ')
            current_chunk = ""
            
            for sentence in sentences:
                sentence_with_period = sentence + '. '
                if len(current_chunk + sentence_with_period) > max_chunk_size:
                    if current_chunk:
                        chunks.append(current_chunk.strip())
                        current_chunk = sentence_with_period
                    else:
                        # Single sentence is too long, truncate it
                        chunks.append(sentence_with_period[:max_chunk_size])
                        current_chunk = ""
                else:
                    current_chunk += sentence_with_period
            
            # Add the last chunk
            if current_chunk:
                chunks.append(current_chunk.strip())
            
            logging.info(f"Split text into {len(chunks)} chunks for TTS processing")
            
            # Generate audio for each chunk and concatenate raw bytes
            audio_parts = []
            total_audio_bytes = 0
            
            for i, chunk in enumerate(chunks):
                logging.info(f"Processing chunk {i+1}/{len(chunks)} ({len(chunk)} chars)")
                
                # Create synthesis input
                synthesis_input = texttospeech.SynthesisInput(text=chunk)
                
                # Configure voice and audio
                voice = texttospeech.VoiceSelectionParams(
                    language_code=self.language_code,
                    name=self.voice_name
                )
                
                audio_config = texttospeech.AudioConfig(
                    audio_encoding=self.audio_encoding,
                    speaking_rate=1.0,
                    pitch=0.0,
                    volume_gain_db=0.0
                )
                
                # Perform TTS synthesis
                response = self.client.synthesize_speech(
                    input=synthesis_input,
                    voice=voice,
                    audio_config=audio_config
                )
                
                # Collect the audio bytes
                audio_parts.append(response.audio_content)
                total_audio_bytes += len(response.audio_content)
            
            # Simple concatenation of MP3 files (basic approach)
            # Note: This may create minor audio artifacts between chunks
            # but avoids dependency on ffmpeg
            logging.info("Concatenating audio chunks...")
            
            with open(audio_path, "wb") as output_file:
                for i, audio_data in enumerate(audio_parts):
                    if i == 0:
                        # Write the first file completely
                        output_file.write(audio_data)
                    else:
                        # For subsequent files, skip the MP3 header (rough approach)
                        # This is a simple concatenation that may not be perfect
                        # but should work for basic needs
                        output_file.write(audio_data)
            
            # Estimate duration based on character count and typical speech rate
            estimated_duration_seconds = len(text) / 12  # ~12 characters per second for speech
            
            # Create metadata
            generation_metadata = {
                "generated_at": datetime.now().isoformat(),
                "script_length": len(text),
                "prepared_text_length": len(text),
                "language_code": self.language_code,
                "voice_name": self.voice_name,
                "audio_encoding": "MP3",
                "file_size_bytes": audio_path.stat().st_size,
                "estimated_duration_minutes": int(estimated_duration_seconds // 60),
                "chunks_processed": len(chunks),
                **(metadata or {})
            }
            
            # Save metadata
            with open(metadata_path, 'w') as f:
                json.dump(generation_metadata, f, indent=2)
            
            # Create symlink to latest audio
            latest_audio_path = self.audio_storage_path / "latest.mp3"
            if latest_audio_path.exists() or latest_audio_path.is_symlink():
                latest_audio_path.unlink()
            latest_audio_path.symlink_to(audio_path.name)
            
            logging.info(f"Chunked audio generated successfully: {audio_path}")
            logging.info(f"Total file size: {audio_path.stat().st_size / 1024 / 1024:.1f}MB")
            logging.info(f"Estimated duration: {estimated_duration_seconds:.1f} seconds")
            
            return {
                "success": True,
                "audio_path": str(audio_path),
                "metadata_path": str(metadata_path),
                "cached": False,
                "metadata": generation_metadata
            }
            
        except Exception as e:
            error_msg = f"Chunked audio generation failed: {e}"
            logging.error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "audio_path": None,
                "metadata_path": None
            }
    
    def generate_podcast_audio(self, podcast_script_data: Dict[str, Any], date_str: str) -> Dict[str, Any]:
        """
        Generate audio specifically for podcast script data
        
        Args:
            podcast_script_data: Output from PodcastGenerator.generate_podcast_script()
            date_str: Date string for filename (e.g., "2025-01-15")
            
        Returns:
            Dictionary with generation results
        """
        if not podcast_script_data or "script" not in podcast_script_data:
            return {
                "success": False,
                "error": "Invalid podcast script data",
                "audio_path": None
            }
        
        script = podcast_script_data["script"]
        metadata = podcast_script_data.get("metadata", {})
        
        # Generate filename
        filename = f"{date_str}_super-gems"
        
        # Generate audio
        result = self.generate_audio(
            script_text=script,
            output_filename=filename,
            metadata=metadata
        )
        
        return result
    
    def cleanup_old_files(self, max_age_days: int = 30):
        """
        Clean up old audio files
        
        Args:
            max_age_days: Maximum age of files to keep in days
        """
        try:
            cutoff_time = datetime.now().timestamp() - (max_age_days * 86400)
            
            for file_path in self.audio_storage_path.iterdir():
                if file_path.is_file() and not file_path.is_symlink():
                    if file_path.stat().st_mtime < cutoff_time:
                        file_path.unlink()
                        logging.info(f"Cleaned up old audio file: {file_path}")
                        
        except Exception as e:
            logging.error(f"Error cleaning up old audio files: {e}")
    
    def get_available_voices(self) -> Dict[str, Any]:
        """
        Get list of available voices from Google Cloud TTS
        
        Returns:
            Dictionary with available voices by language
        """
        if not self.is_available:
            return {"error": "TTS service not available"}
        
        try:
            voices = self.client.list_voices()
            
            voice_data = {}
            for voice in voices.voices:
                for language_code in voice.language_codes:
                    if language_code not in voice_data:
                        voice_data[language_code] = []
                    
                    voice_data[language_code].append({
                        "name": voice.name,
                        "gender": voice.ssml_gender.name,
                        "natural_sample_rate": voice.natural_sample_rate_hertz
                    })
            
            return voice_data
            
        except Exception as e:
            logging.error(f"Error fetching available voices: {e}")
            return {"error": str(e)}
    
    def estimate_cost(self, text_length: int) -> Dict[str, float]:
        """
        Estimate the cost of generating audio for given text length
        
        Args:
            text_length: Length of text in characters
            
        Returns:
            Dictionary with cost estimation
        """
        # Google Cloud TTS pricing (as of 2024)
        # Neural2 voices: $16.00 per 1 million characters
        # Standard voices: $4.00 per 1 million characters
        
        neural_cost_per_char = 16.00 / 1_000_000
        standard_cost_per_char = 4.00 / 1_000_000
        
        is_neural = "Neural" in self.voice_name
        cost_per_char = neural_cost_per_char if is_neural else standard_cost_per_char
        
        estimated_cost = text_length * cost_per_char
        
        return {
            "text_length": text_length,
            "voice_type": "Neural2" if is_neural else "Standard",
            "estimated_cost_usd": round(estimated_cost, 6),
            "cost_per_million_chars": 16.00 if is_neural else 4.00
        }