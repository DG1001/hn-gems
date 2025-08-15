import os
import json
import re
from typing import Dict, List, Any
from datetime import datetime
import google.generativeai as genai
from dataclasses import dataclass
from urllib.parse import urlparse

@dataclass
class PodcastMetadata:
    """Metadata for generated podcast"""
    gems_count: int
    estimated_duration_minutes: int
    generation_timestamp: str
    total_words: int
    language: str

class PodcastGenerator:
    """
    Service to generate podcast scripts from Super Gems analyses using Gemini 2.5 Flash-Lite
    """
    
    def __init__(self, gemini_api_key: str):
        """Initialize the podcast generator with Gemini API"""
        genai.configure(api_key=gemini_api_key)
        self.model = genai.GenerativeModel('gemini-2.5-flash-lite')
        
        # Generation config for consistent, natural speech
        self.generation_config = genai.types.GenerationConfig(
            temperature=0.7,  # Creative but consistent
            top_p=0.8,
            max_output_tokens=8000,  # Increased for multiple gems
        )
    
    def generate_podcast_script(self, super_gems_data: dict) -> Dict[str, Any]:
        """
        Generate a complete podcast script from super gems data
        
        Args:
            super_gems_data: Dictionary containing gems array and metadata
            
        Returns:
            Dictionary with script content and metadata
        """
        gems = super_gems_data.get('gems', [])
        generation_date = super_gems_data.get('generation_timestamp', datetime.now().isoformat())
        
        if not gems:
            return self._create_empty_script()
        
        # Generate intro
        intro = self._create_intro(len(gems), generation_date)
        
        # Generate segments for each gem
        gem_segments = []
        for i, gem in enumerate(gems):
            try:
                print(f"Generating script for gem {i+1}/{len(gems)}: {gem.get('title', 'Unknown')}")
                segment = self._generate_gem_script(gem)
                if segment:
                    gem_segments.append(segment)
                    print(f"✅ Generated {len(segment.split())} words for gem {i+1}")
                else:
                    print(f"❌ Empty segment for gem {i+1}")
            except Exception as e:
                print(f"❌ Error generating script for gem {gem.get('hn_id', 'unknown')}: {e}")
                # Try fallback script
                try:
                    fallback = self._generate_fallback_script(gem)
                    if fallback:
                        gem_segments.append(fallback)
                        print(f"✅ Used fallback script for gem {i+1}")
                except Exception as fallback_error:
                    print(f"❌ Fallback also failed for gem {i+1}: {fallback_error}")
                continue
        
        # Generate outro
        outro = self._create_outro()
        
        # Combine all parts
        full_script = f"{intro}\n\n{' '.join(gem_segments)}\n\n{outro}"
        
        # Calculate metadata
        words = len(full_script.split())
        estimated_duration = max(1, words // 150)  # ~150 words per minute
        
        metadata = PodcastMetadata(
            gems_count=len(gems),
            estimated_duration_minutes=estimated_duration,
            generation_timestamp=datetime.now().isoformat(),
            total_words=words,
            language="en"
        )
        
        return {
            "script": full_script,
            "metadata": metadata.__dict__
        }
    
    def _create_intro(self, gem_count: int, date: str) -> str:
        """Create podcast introduction"""
        # Parse date for more natural speech
        try:
            parsed_date = datetime.fromisoformat(date.replace('Z', '+00:00'))
            readable_date = parsed_date.strftime("%B %d, %Y")
        except:
            readable_date = "today"
        
        intro = f"""Welcome to HN Hidden Gems Podcast, your weekly deep dive into overlooked treasures from Hacker News.

I'm your AI host, and today we're analyzing {gem_count} exceptional discoveries from {readable_date}. These are high-quality projects and innovations from low-karma accounts that might have slipped under your radar.

Each gem has been carefully analyzed for technical innovation, problem significance, and potential community impact. Let's dive in."""
        
        return intro
    
    def _create_outro(self) -> str:
        """Create podcast outro"""
        outro = """That wraps up today's episode of HN Hidden Gems Podcast.

Remember, these discoveries come from developers who are just starting to build their reputation in the Hacker News community. By paying attention to these hidden gems, you're not just finding great tools and ideas... you're supporting innovation from the ground up.

Visit our website to explore these gems in detail, try them out, and maybe even contribute to their development.

Until next time, keep discovering the hidden treasures of the tech world."""
        
        return outro
    
    def _generate_gem_script(self, gem_data: dict) -> str:
        """Generate script segment for a single gem using Gemini"""
        prompt = self._create_gemini_prompt(gem_data)
        
        try:
            response = self.model.generate_content(
                prompt,
                generation_config=self.generation_config
            )
            
            raw_script = response.text
            optimized_script = self._optimize_text_for_tts(raw_script)
            return optimized_script
            
        except Exception as e:
            print(f"Error generating gem script with Gemini: {e}")
            # Fallback to template-based generation
            return self._generate_fallback_script(gem_data)
    
    def _create_gemini_prompt(self, gem_data: dict) -> str:
        """Create Gemini prompt for script generation"""
        analysis = gem_data.get('analysis', {})
        
        prompt = f"""You are an experienced podcast host for tech content. Convert the following HN Super Gems analysis into a natural, flowing podcast script segment in English.

IMPORTANT for audio optimization:
- Simplify URLs (e.g. "github dot com slash username slash project" instead of full URL)
- Format technical terms for speech-friendly pronunciation
- Create natural transitions between topics
- Add pause markers (...) for better listening flow
- No visual elements (stars, dots) - describe verbally
- Keep segment to 1-2 minutes (150-300 words) for efficient processing

Structure:
1. Brief introduction for this gem
2. Explain the problem/innovation
3. Technical highlights and implementation details
4. Conclusion

Post Title: {gem_data.get('title', 'Unknown Title')}
Post URL: {gem_data.get('url', 'No URL')}
Author: {gem_data.get('author', 'Unknown Author')} (karma: {gem_data.get('author_karma', 0)})

Detailed Analysis: {analysis.get('detailed_analysis', 'No detailed analysis available')}

Strengths: {', '.join(analysis.get('strengths', []))}
Areas for Improvement: {', '.join(analysis.get('areas_for_improvement', []))}

Real Community Metrics (when available):
- GitHub Stars: {gem_data.get('badges', {}).get('github_stars', 'Not available')}
- Open Source: {'Yes' if gem_data.get('badges', {}).get('is_open_source') else 'No'}
- Working Demo: {'Yes' if gem_data.get('badges', {}).get('has_demo') else 'No'}

When available, mention real community metrics (GitHub stars, repository activity) rather than speculating about future community impact.

Generate a 1-2 minute script segment for this individual gem (this is one of multiple gems in the episode). Make it engaging and informative. Focus only on this specific project."""
        
        return prompt
    
    
    def _generate_fallback_script(self, gem_data: dict) -> str:
        """Generate fallback script without AI when Gemini fails"""
        title = gem_data.get('title', 'Unknown Project')
        author = gem_data.get('author', 'unknown developer')
        analysis = gem_data.get('analysis', {})
        
        # Add real implementation metrics if available
        badges = gem_data.get('badges', {})
        github_stars = badges.get('github_stars', 0)
        is_open_source = badges.get('is_open_source', False)
        has_demo = badges.get('has_demo', False)
        
        # Build implementation details from factual GitHub metrics
        implementation_details = ""
        if github_stars > 0:
            implementation_details = f" The repository shows strong development practices with {github_stars} stars"
            if has_demo:
                implementation_details += " and includes a working demo"
            if is_open_source:
                implementation_details += ", and it's open source for community collaboration"
        elif is_open_source:
            implementation_details = " As an open source project, it encourages transparency and community contribution"
        elif has_demo:
            implementation_details = " There's a working demo available for you to try"
        
        script = f"""Our next hidden gem comes from {author}, with a project called "{title}".

This innovative solution tackles an important problem in the developer community. The technical approach shows notable innovation, addressing a significant problem with a unique approach.{implementation_details}

{analysis.get('detailed_analysis', 'This project represents the kind of quality innovation we love to see from emerging developers in the Hacker News community.')}

Overall, this gem demonstrates the value of paying attention to contributions from newer community members. It's definitely worth checking out."""
        
        return script
    
    def _optimize_text_for_tts(self, text: str) -> str:
        """Optimize text for text-to-speech synthesis"""
        # URL handling
        text = re.sub(r'https?://github\.com/([^/]+)/([^/\s]+)', r'github repository by \1', text)
        text = re.sub(r'https?://([^/\s]+)\.com[^\s]*', r'\1 dot com', text)
        text = re.sub(r'https?://([^/\s]+)\.[a-z]{2,4}[^\s]*', r'\1 website', text)
        
        # Technical term handling
        text = re.sub(r'\bAPI\b', 'A P I', text)
        text = re.sub(r'\bML\b', 'machine learning', text)
        text = re.sub(r'\bAI\b', 'artificial intelligence', text)
        text = re.sub(r'\bJS\b', 'JavaScript', text)
        text = re.sub(r'\bCSS\b', 'C S S', text)
        text = re.sub(r'\bHTML\b', 'H T M L', text)
        text = re.sub(r'\bSQL\b', 'S Q L', text)
        text = re.sub(r'\bCLI\b', 'command line interface', text)
        text = re.sub(r'\bGUI\b', 'graphical user interface', text)
        text = re.sub(r'\bOS\b', 'operating system', text)
        text = re.sub(r'\bUI\b', 'user interface', text)
        text = re.sub(r'\bUX\b', 'user experience', text)
        
        # Ratings conversion
        text = re.sub(r'⭐{5}', 'five out of five stars', text)
        text = re.sub(r'⭐{4}', 'four out of five stars', text)
        text = re.sub(r'⭐{3}', 'three out of five stars', text)
        text = re.sub(r'⭐{2}', 'two out of five stars', text)
        text = re.sub(r'⭐{1}', 'one out of five stars', text)
        
        # Dot indicators
        text = re.sub(r'●●●●', 'exceptional rating', text)
        text = re.sub(r'●●●', 'excellent rating', text)
        text = re.sub(r'●●', 'good rating', text)
        text = re.sub(r'●', 'basic rating', text)
        
        # Pause and emphasis markers
        text = re.sub(r'\.\.\.\s*', '... ', text)  # Ensure space after pauses
        text = re.sub(r'\n\n+', '\n\n', text)  # Clean up multiple newlines
        
        # Code snippets removal (replace with description)
        text = re.sub(r'```[^`]*```', '[code example]', text)
        text = re.sub(r'`[^`]+`', '[code term]', text)
        
        # Clean up extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def _create_empty_script(self) -> Dict[str, Any]:
        """Create an empty script when no gems are provided"""
        script = """Welcome to HN Hidden Gems Podcast.

Unfortunately, no qualifying hidden gems were found in the recent analysis window. This could be because there weren't any high-quality posts from low-karma accounts recently, or they didn't meet our quality thresholds.

Don't worry though... the next batch of hidden treasures is just around the corner. Keep checking back for fresh discoveries from the innovative minds in the Hacker News community.

Until next time, keep exploring."""
        
        metadata = PodcastMetadata(
            gems_count=0,
            estimated_duration_minutes=1,
            generation_timestamp=datetime.now().isoformat(),
            total_words=len(script.split()),
            language="en"
        )
        
        return {
            "script": script,
            "metadata": metadata.__dict__
        }