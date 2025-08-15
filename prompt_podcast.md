# Claude Code Prompt: Complete HN-Gems Podcast Feature Implementation

## Project Context
I'm working on the HN-Gems Flask application (https://github.com/DG1001/hn-gems), which discovers hidden Hacker News gems from low-karma users. The app already has a "Super Gems" feature that creates AI-generated analyses of the best discovered posts.

## Goal
Implement a complete podcast functionality that converts Super Gems analyses into audio format so users can listen to them on-the-go.

## Technical Requirements Overview

### Core Functionality
1. **Google Cloud Text-to-Speech Integration**
   - Use Google Cloud Text-to-Speech API for professional audio quality
   - Support English and German voices
   - Audio format: MP3 (for best compatibility)

2. **Podcast Script Generation with Gemini 2.5 Flash-Lite**
   - Convert Super Gems HTML analyses into podcast-suitable scripts
   - Use **gemini-2.5-flash-lite** model (cost-efficient: $0.10/1M input, $0.40/1M output)
   - Structure content with intro, main content, outro
   - Optimize text for speech synthesis (URLs, technical terms, etc.)

3. **Audio Generation Pipeline**
   - Background service for automatic audio generation
   - Integration into existing APScheduler architecture
   - Audio caching and storage management

4. **Web Interface**
   - Audio player on Super Gems page
   - Download option for offline usage
   - Progress indicator during audio generation

## Existing Architecture
- **Flask app** with APScheduler for background services
- **SQLite/PostgreSQL** for data storage
- **Google Gemini API** already integrated for Super Gems analysis
- **Existing services**: Post Collection, Hall of Fame, Super Gems Analysis

## Implementation Focus: Podcast Generator Service

### Primary Task: Create `hn_hidden_gems/services/podcast_generator.py`

#### Existing Super Gems Data Structure
```python
# Example from super-gems.json
{
    "gems": [
        {
            "hn_id": 12345,
            "title": "Show HN: My new AI tool",
            "url": "https://example.com",
            "author": "username",
            "score": 15,
            "analysis": {
                "overall_rating": 4,
                "technical_innovation": "●●●●",
                "problem_significance": "●●●",
                "implementation_quality": "●●●●",
                "community_value": "●●●",
                "uniqueness_score": "●●●●",
                "detailed_analysis": "This project demonstrates...",
                "strengths": ["..."],
                "areas_for_improvement": ["..."]
            }
        }
    ],
    "generation_timestamp": "2025-01-15T10:30:00Z",
    "total_analyzed": 5
}
```

#### Required Class Structure
```python
class PodcastGenerator:
    def __init__(self, gemini_api_key: str)
    def generate_podcast_script(self, super_gems_data: dict) -> str
    def _create_intro(self, gem_count: int, date: str) -> str
    def _format_gem_for_audio(self, gem: dict) -> str
    def _create_outro() -> str
    def _optimize_text_for_tts(self, text: str) -> str
```

#### Gemini 2.5 Flash-Lite Integration
```python
import google.generativeai as genai

class PodcastGenerator:
    def __init__(self, api_key: str):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-2.5-flash-lite')
        
    def _generate_gem_script(self, gem_data: dict) -> str:
        prompt = self._create_gemini_prompt(gem_data)
        response = self.model.generate_content(
            prompt,
            generation_config={
                'temperature': 0.7,  # Creative but consistent
                'max_output_tokens': 1000,
                'top_p': 0.8
            }
        )
        return self._optimize_text_for_tts(response.text)
```

#### Gemini Prompt for Script Generation
```
You are an experienced podcast host for tech content. Convert the following HN Super Gems analysis into a natural, flowing podcast script in English.

IMPORTANT for audio optimization:
- Simplify URLs (e.g. "github dot com slash username slash project" instead of full URL)
- Format technical terms for speech-friendly pronunciation
- Create natural transitions between topics
- Add pause markers for better listening flow
- No visual elements (stars, dots) - describe verbally
- Keep segments to 2-3 minutes each

Structure:
1. Brief introduction for this gem
2. Explain the problem/innovation
3. Technical highlights and implementation details
4. Conclusion with rating

INPUT DATA:
{gem_data}

Generate a 2-3 minute script segment for this gem.
```

#### Text Optimization for TTS

**URL Handling:**
- `https://github.com/user/repo` → "github repository by user"
- `example.com/path` → "example dot com"
- Long URLs: remove completely or reduce to domain

**Technical Terms:**
- `API` → "A P I"
- `ML` → "machine learning"
- `AI` → "artificial intelligence"
- `JS` → "JavaScript"
- Remove code snippets or describe verbally

**Ratings Conversion:**
- `⭐⭐⭐⭐⭐` → "five out of five stars"
- `●●●●` → "excellent rating"
- Numerical scores: "rated 4.2 out of 5"

**Pause and Emphasis:**
- Add `...` for natural pauses
- Mark emphasizing words
- Paragraphs for longer pauses

#### Podcast Script Structure

**Intro Template:**
```
Welcome to HN Hidden Gems Podcast, your weekly deep dive into overlooked treasures from Hacker News...
Today we're analyzing [gem_count] exceptional discoveries from [date]...
```

**Per Gem Format:**
```
Our first gem today comes from user [username] with a project called "[title]"...
This innovative tool addresses...
What makes this particularly impressive is...
The project has gained [X] GitHub stars, showing early community interest...
Overall, this gem earns [rating] out of five stars...
```

**Outro Template:**
```
That wraps up today's episode of HN Hidden Gems...
Visit our website to explore these gems in detail...
```

#### Configuration and Environment
```bash
# Gemini API
GEMINI_API_KEY=your_gemini_key
GEMINI_MODEL=gemini-2.5-flash-lite

# Google Cloud Text-to-Speech
GOOGLE_TTS_API_KEY=your_api_key
TTS_LANGUAGE_CODE=en-US
TTS_VOICE_NAME=en-US-Neural2-J
TTS_AUDIO_ENCODING=MP3

# Podcast Settings
PODCAST_LANGUAGE=en
PODCAST_STYLE=professional
MAX_SCRIPT_LENGTH=5000
AUDIO_GENERATION_ENABLED=true
AUDIO_STORAGE_PATH=static/audio
AUDIO_CLEANUP_DAYS=30
```

## Complete Implementation Steps

### Phase 1: Podcast Generator Service
1. **Create `hn_hidden_gems/services/podcast_generator.py`**
   - Implement PodcastGenerator class
   - Gemini 2.5 Flash-Lite integration
   - Text optimization for TTS
   - Template system for intro/outro

### Phase 2: Audio Generation Service
1. **Create `hn_hidden_gems/services/audio_service.py`**
   - Google Cloud TTS API integration
   - Audio file generation and storage
   - Error handling and retry logic

### Phase 3: Background Service Integration
1. **Extend existing APScheduler**
   - Add audio generation to super gems pipeline
   - Trigger after Super Gems analysis completion
   - Cache management for generated scripts

### Phase 4: Database Schema Extension
1. **Create `hn_hidden_gems/models/audio.py`**
   - Audio metadata model
   - Generation status tracking
   - File path and metadata storage

### Phase 5: Web Interface
1. **Update `templates/super-gems.html`**
   - Audio player component
   - Download links
   - Generation status display

2. **Create API endpoints in `app.py`**
   - `/api/audio/super-gems/latest`
   - `/api/audio/super-gems/<date>`
   - `/api/audio/generate`

### Phase 6: File Management
1. **Audio storage structure**
   ```
   /static/audio/super-gems/
     ├── 2025-01-15_super-gems.mp3
     ├── 2025-01-15_super-gems_metadata.json
     └── latest.mp3 (symlink)
   ```

## Audio Configuration Specifications
- **Format**: MP3, 128kbps
- **Language**: English (with German option)
- **Voice**: Professional, clear voice (en-US-Neural2-J)
- **Speed**: Normal (1.0x)

## Expected Output Structure
```python
{
    "script": "full_podcast_script_text",
    "metadata": {
        "gems_count": 5,
        "estimated_duration_minutes": 15,
        "generation_timestamp": "2025-01-15T10:30:00Z",
        "total_words": 2500,
        "language": "en"
    }
}
```

## Error Handling Requirements
- Gemini API error handling
- Fallback to template-based generation
- Comprehensive logging
- Retry logic for transient failures
- Cache invalid script prevention

## Integration Points
```python
# In super_gems_service.py - after successful analysis
def analyze_super_gems():
    # ... existing Super Gems analysis
    
    if super_gems_data:
        try:
            podcast_script = podcast_generator.generate_podcast_script(super_gems_data)
            save_podcast_script(podcast_script, timestamp)
            
            # Trigger audio generation
            audio_service.generate_audio(podcast_script)
        except Exception as e:
            logger.error(f"Podcast generation failed: {e}")
```

## Dependencies to Add
```python
# requirements.txt additions
google-cloud-texttospeech>=2.14.0
google-generativeai>=0.3.0
pydub>=0.25.1  # Audio processing utilities
```

## Testing Requirements
- Unit tests for PodcastGenerator class
- Mock Gemini API responses
- TTS optimization function tests
- Integration tests with real Super Gems data
- Audio compatibility verification

## Implementation Priorities
1. **Functionality first** - Get basic audio generation working
2. **Integration** - Seamless integration into existing architecture
3. **User experience** - Simple operation and good performance
4. **Robustness** - Error handling and fallback mechanisms

## Key Success Metrics
- Successfully converts Super Gems analysis to natural podcast scripts
- Generates high-quality MP3 audio files
- Integrates seamlessly with existing background services
- Provides reliable user interface for audio access
- Maintains cost efficiency with Gemini 2.5 Flash-Lite

Start with Phase 1 (PodcastGenerator) and work systematically through the implementation. Focus on an MVP version that we can iteratively improve. Ensure all components respect the existing Flask + APScheduler architecture and database patterns.