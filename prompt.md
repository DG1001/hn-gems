# Claude Code Prompt: HN Hidden Gems Finder

## Project Context
I want to build "HN Hidden Gems Finder" - a tool that discovers high-quality Hacker News posts from low-karma accounts that would otherwise be overlooked. This is based on my personal experience where my Show HN post got zero traction because my account had only 1 karma.

## Important Discovery
My research shows:
1. **No similar tools exist** - all existing HN tools filter OUT low-karma posts, not highlight them
2. **API Usage is encouraged** - HN API has NO rate limits, Algolia HN Search API is official and free to use
3. This tool will run on my own server with a live demo for the Show HN post

## Starting Code Base
```python
import requests
import json
from datetime import datetime, timedelta
import time
from typing import List, Dict, Tuple
import re

class HNHiddenGemsFinder:
    """
    Findet interessante Hacker News Posts von Low-Karma-Accounts,
    die sonst übersehen werden würden.
    """
    
    def __init__(self):
        self.hn_api_base = "https://hacker-news.firebaseio.com/v0"
        self.seen_posts = set()
        
    def get_new_posts(self, story_type: str = "new", limit: int = 100) -> List[Dict]:
        """Holt die neuesten Posts von HN."""
        try:
            # Get story IDs
            response = requests.get(f"{self.hn_api_base}/{story_type}stories.json")
            story_ids = response.json()[:limit]
            
            posts = []
            for story_id in story_ids:
                if story_id not in self.seen_posts:
                    post_data = self.get_post_details(story_id)
                    if post_data:
                        posts.append(post_data)
                        self.seen_posts.add(story_id)
                        
            return posts
        except Exception as e:
            print(f"Error fetching posts: {e}")
            return []
    
    def get_post_details(self, post_id: int) -> Dict:
        """Holt Details eines einzelnen Posts."""
        try:
            response = requests.get(f"{self.hn_api_base}/item/{post_id}.json")
            post = response.json()
            
            # Get author karma
            if post and 'by' in post:
                user_data = self.get_user_karma(post['by'])
                post['author_karma'] = user_data.get('karma', 0)
                post['account_age_days'] = self.calculate_account_age(user_data.get('created', 0))
                
            return post
        except:
            return None
    
    def get_user_karma(self, username: str) -> Dict:
        """Holt Karma-Daten eines Users."""
        try:
            response = requests.get(f"{self.hn_api_base}/user/{username}.json")
            return response.json()
        except:
            return {'karma': 0}
    
    def calculate_account_age(self, created_timestamp: int) -> int:
        """Berechnet Account-Alter in Tagen."""
        if created_timestamp:
            created_date = datetime.fromtimestamp(created_timestamp)
            return (datetime.now() - created_date).days
        return 0
    
    def analyze_post_quality(self, post: Dict) -> Dict[str, float]:
        """
        Analysiert Post-Qualität mit verschiedenen Metriken.
        In einer echten Implementation würde hier ein LLM verwendet.
        """
        scores = {
            'technical_depth': 0.0,
            'originality': 0.0,
            'problem_solving': 0.0,
            'spam_likelihood': 0.0,
            'overall_interest': 0.0
        }
        
        title = post.get('title', '').lower()
        text = post.get('text', '').lower()
        url = post.get('url', '')
        
        # Technical depth indicators
        tech_keywords = ['algorithm', 'implementation', 'architecture', 'performance', 
                        'open source', 'api', 'framework', 'database', 'docker', 
                        'kubernetes', 'ai', 'machine learning', 'compiler']
        tech_score = sum(1 for keyword in tech_keywords if keyword in title or keyword in text)
        scores['technical_depth'] = min(tech_score / 3, 1.0)
        
        # Originality - Show HN posts get bonus
        if title.startswith('show hn:'):
            scores['originality'] += 0.3
        if 'github.com' in url:
            scores['originality'] += 0.2
        if any(phrase in title for phrase in ['built', 'created', 'made', 'developed']):
            scores['originality'] += 0.2
            
        # Problem solving indicators
        problem_keywords = ['solution', 'solves', 'fixes', 'helps', 'easier', 'faster', 
                           'alternative', 'replacement', 'tool', 'utility']
        problem_score = sum(1 for keyword in problem_keywords if keyword in title or keyword in text)
        scores['problem_solving'] = min(problem_score / 2, 1.0)
        
        # Spam detection
        spam_indicators = [
            len(re.findall(r'[A-Z]{3,}', title)) > 2,  # Too many CAPS
            title.count('!') > 1,  # Multiple exclamation marks
            any(word in title.lower() for word in ['cryptocurrency', 'nft', 'blockchain', 'earn money']),
            len(title) < 20,  # Too short
            not url and not text,  # No content
            '$$$' in title or text,
            'click here' in title.lower() or text
        ]
        scores['spam_likelihood'] = sum(spam_indicators) / len(spam_indicators)
        
        # Overall interest score
        scores['overall_interest'] = (
            scores['technical_depth'] * 0.3 +
            scores['originality'] * 0.3 +
            scores['problem_solving'] * 0.3 -
            scores['spam_likelihood'] * 0.5
        )
        
        return scores
    
    def find_hidden_gems(self, karma_threshold: int = 50, min_interest_score: float = 0.5) -> List[Tuple[Dict, Dict]]:
        """
        Findet interessante Posts von Low-Karma-Accounts.
        """
        posts = self.get_new_posts(limit=100)
        hidden_gems = []
        
        for post in posts:
            # Skip if no author karma info
            if 'author_karma' not in post:
                continue
                
            # Focus on low-karma accounts
            if post['author_karma'] < karma_threshold:
                quality_scores = self.analyze_post_quality(post)
                
                # Filter out likely spam
                if quality_scores['spam_likelihood'] < 0.3 and quality_scores['overall_interest'] >= min_interest_score:
                    hidden_gems.append((post, quality_scores))
        
        # Sort by interest score
        hidden_gems.sort(key=lambda x: x[1]['overall_interest'], reverse=True)
        
        return hidden_gems
```

## Development Tasks

### 1. Create a complete Python project structure
- Create proper project structure with:
  - `hn_hidden_gems/` main package
  - `requirements.txt` with all dependencies
  - `setup.py` for installation
  - `README.md` with clear documentation
  - `.gitignore` for Python projects
  - `LICENSE` (MIT)

### 2. Enhance with dual API strategy
- Use official HN Firebase API (no rate limits!) for:
  - Real-time updates of new posts
  - User karma and account age data
  - Complete post metadata
- Integrate Algolia HN Search API for:
  - Advanced filtering capabilities
  - Historical data analysis
  - Full-text search within posts
- Add LLM integration for content analysis:
  - OpenAI API for detecting technical innovation
  - Identifying problem significance
  - Evaluating code quality (for GitHub links)
  - Finding similar existing solutions
  
### 3. Add persistent storage
- SQLite database for:
  - Tracking analyzed posts
  - Storing quality scores
  - User preferences
  - Historical data for trend analysis
- Database schema with proper indices

### 4. Build production-ready web interface with Flask
- Create a clean, fast-loading web UI that shows:
  - **Live Hidden Gems Feed** - real-time updates of overlooked quality posts
  - **Hall of Fame** - showcase posts that were initially overlooked but later became popular
  - **Success Stories** - track gems that eventually reached front page after discovery
  - Filtering options (karma threshold, categories, time range)
  - Post details with quality score breakdown
  - "Boost" button to help promote good posts (share on social media)
  - Statistics dashboard (success rate, most overlooked authors)
- API endpoints for:
  - Getting latest gems
  - Hall of Fame entries
  - Success metrics
  - RSS feed for discoveries
- Production features:
  - Server-side rendering for fast initial load
  - Proper SEO for discovered gems
  - Mobile-responsive design

### 5. Add notification system
- Email notifications (using SMTP)
- Discord webhook integration
- Slack integration
- RSS feed generation
- Configurable notification rules (keywords, score thresholds)

### 6. Implement efficient data management
- PostgreSQL for production database (better than SQLite for concurrent access)
- Redis for caching frequently accessed data
- Efficient polling strategy:
  - Poll HN API every 30-60 seconds (no rate limits!)
  - Use Algolia for batch historical analysis
  - Cache analysis results for 5-10 minutes
- Background job processing with Celery for:
  - LLM analysis of posts
  - GitHub repository scanning
  - Hall of Fame updates

### 7. Add monitoring and analytics
- Track which hidden gems eventually became popular
- Success rate metrics
- User engagement tracking
- Performance monitoring

### 8. Create production deployment configuration
- Docker setup with:
  - Multi-stage Dockerfile for optimized images
  - docker-compose.yml for complete stack (Flask, PostgreSQL, Redis, Celery)
  - Environment variable configuration
  - Nginx reverse proxy configuration
- Production hardening:
  - Gunicorn with proper worker configuration
  - SSL/TLS setup with Let's Encrypt
  - Monitoring with Prometheus/Grafana
  - Log aggregation with ELK stack
  - Automated backups for Hall of Fame data
- Performance optimization:
  - CDN for static assets
  - Database query optimization
  - Prepared for HN traffic spikes

### 9. Add Hall of Fame and success tracking
- **Hall of Fame system**:
  - Track discovered gems that later reached front page
  - Calculate "discovery lead time" (how early we spotted them)
  - Show before/after karma scores
  - Create leaderboard of most successful discoveries
- **Success metrics**:
  - Percentage of gems that eventually succeeded
  - Average time from discovery to front page
  - Most undervalued authors who later became popular
- **Advanced analysis features**:
  - Machine learning model to improve predictions over time
  - GitHub repository analysis (for Show HN posts with repos)
  - Comment sentiment analysis
  - Author history analysis
  - Similar post detection
  - Pattern recognition for what makes overlooked posts eventually succeed

### 10. Create comprehensive documentation
- API documentation
- Configuration guide
- Deployment instructions
- Contributing guidelines
- Examples and use cases

## Technical Requirements
- Python 3.9+
- Async support for better performance
- Type hints throughout
- Comprehensive error handling
- Logging with proper levels
- Unit tests with pytest
- Integration tests
- CI/CD with GitHub Actions

## Output Structure
Please create the complete project with all files. Start with the core functionality and then add features incrementally. Make sure each component is well-tested and documented.

## Special Focus Areas
1. **Low-karma account support**: This is the core mission - helping good content from new users get discovered
2. **Spam filtering**: Must be excellent to maintain quality
3. **Easy deployment**: Should work in XaresAICoder environment
4. **Real value**: Must actually find gems that would otherwise be missed

## Meta Note
This tool itself will be posted as "Show HN: I built a tool to find overlooked Show HN posts after mine got zero attention" - so it needs to be genuinely useful and well-executed. The irony is intentional and should make for a good discussion starter.

The live demo will be running on my server, showing real-time discoveries of hidden gems. Maybe it will even discover my XaresAICoder post that initially got overlooked! That would be the perfect full-circle moment.

## Additional Implementation Notes
- Since HN API has no rate limits, we can be more aggressive with polling (every 30-60 seconds)
- Use Algolia API for deep historical analysis to build the initial Hall of Fame
- The production server should handle HN traffic spikes (prepare for the "HN hug of death")
- Include my own overlooked XaresAICoder post as the origin story in the About section