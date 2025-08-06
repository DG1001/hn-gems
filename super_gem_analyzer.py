import os
import json
import asyncio
import aiohttp
from datetime import datetime, timedelta
from typing import List, Dict, Any
import google.generativeai as genai
from dataclasses import dataclass
from jinja2 import Template
import subprocess
import re
from urllib.parse import urlparse

@dataclass
class SuperGemAnalysis:
    """Detailed analysis of a potential super gem"""
    post_id: int
    title: str
    url: str
    author: str
    author_karma: int
    original_score: float
    
    # LLM Analysis Results
    technical_innovation: float  # 0-1
    problem_significance: float  # 0-1
    implementation_quality: float  # 0-1
    community_value: float  # 0-1
    uniqueness_score: float  # 0-1
    
    # Specific Checks
    is_open_source: bool
    has_working_demo: bool
    has_documentation: bool
    is_commercially_focused: bool  # negative signal
    
    # GitHub Analysis (if applicable)
    github_stars: int = 0
    github_commits: int = 0
    github_contributors: int = 0
    code_quality_score: float = 0.0
    readme_quality: float = 0.0
    
    # Summary
    llm_reasoning: str
    super_gem_score: float  # 0-1 final score
    key_strengths: List[str]
    potential_concerns: List[str]
    similar_tools: List[str]

class SuperGemsAnalyzer:
    def __init__(self, gemini_api_key: str, db_path: str = "hn_hidden_gems.db"):
        self.db_path = db_path
        genai.configure(api_key=gemini_api_key)
        self.model = genai.GenerativeModel('gemini-2.5-flash')
        
        # Analysis prompts
        self.main_analysis_prompt = """
        Analyze this Hacker News post for its value to the developer community.
        
        Post Title: {title}
        Post URL: {url}
        Post Text: {text}
        Author Karma: {author_karma}
        
        Please evaluate the following aspects (score 0-1):
        
        1. Technical Innovation: How novel or innovative is the technical approach?
        2. Problem Significance: How important is the problem being solved?
        3. Implementation Quality: Based on available information, how well-executed is this?
        4. Community Value: How valuable would this be to the HN developer community?
        5. Uniqueness: How unique is this compared to existing solutions?
        
        Also determine:
        - Is this open source? (look for GitHub links, license mentions)
        - Does it have a working demo?
        - Is the documentation good?
        - Is this primarily a commercial/paid product? (negative signal)
        - What similar tools/solutions exist?
        - What are the key strengths?
        - What are potential concerns?
        
        If this is a Show HN post about a developer tool, give extra weight to:
        - Solves real developer pain points
        - Good technical documentation
        - Open source with permissive license
        - Active development
        - Not just another wrapper around existing APIs
        
        Return your analysis as a JSON object with all the scores and findings.
        """
        
        self.github_analysis_prompt = """
        Analyze this GitHub repository for code quality and project health:
        
        Repository: {repo_url}
        README Content: {readme}
        File Structure: {file_structure}
        Recent Commits: {recent_commits}
        
        Evaluate:
        1. Code Quality (0-1): Based on structure, organization, patterns
        2. README Quality (0-1): Clarity, completeness, examples
        3. Project Activity: Is it actively maintained?
        4. Architecture: Is it well-designed?
        5. Key Technologies Used
        
        Return as JSON with scores and observations.
        """
    
    async def fetch_url_content(self, url: str) -> str:
        """Fetch content from a URL"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        return await response.text()
        except:
            pass
        return ""
    
    def extract_github_url(self, text: str, url: str) -> str:
        """Extract GitHub repository URL from post"""
        # Check if URL itself is GitHub
        if 'github.com' in url:
            return url
            
        # Search in text
        github_pattern = r'https?://github\.com/[\w\-]+/[\w\-]+'
        matches = re.findall(github_pattern, text)
        return matches[0] if matches else None
    
    async def analyze_github_repo(self, repo_url: str) -> Dict[str, Any]:
        """Analyze a GitHub repository"""
        try:
            # Extract owner/repo from URL
            parsed = urlparse(repo_url)
            parts = parsed.path.strip('/').split('/')
            if len(parts) >= 2:
                owner, repo = parts[0], parts[1]
            else:
                return {}
            
            # Use GitHub API to get repo info
            api_url = f"https://api.github.com/repos/{owner}/{repo}"
            async with aiohttp.ClientSession() as session:
                async with session.get(api_url) as response:
                    if response.status == 200:
                        repo_data = await response.json()
                        
                        # Get README
                        readme_url = f"https://raw.githubusercontent.com/{owner}/{repo}/main/README.md"
                        readme_content = await self.fetch_url_content(readme_url)
                        if not readme_content:
                            readme_url = f"https://raw.githubusercontent.com/{owner}/{repo}/master/README.md"
                            readme_content = await self.fetch_url_content(readme_url)
                        
                        # Basic analysis without full repo clone
                        return {
                            'stars': repo_data.get('stargazers_count', 0),
                            'forks': repo_data.get('forks_count', 0),
                            'open_issues': repo_data.get('open_issues_count', 0),
                            'created_at': repo_data.get('created_at'),
                            'updated_at': repo_data.get('updated_at'),
                            'description': repo_data.get('description', ''),
                            'language': repo_data.get('language', ''),
                            'readme_content': readme_content[:5000],  # First 5k chars
                            'license': repo_data.get('license', {}).get('name', 'Unknown')
                        }
        except Exception as e:
            print(f"Error analyzing GitHub repo: {e}")
        return {}
    
    async def analyze_with_llm(self, post: Dict[str, Any]) -> SuperGemAnalysis:
        """Perform deep analysis using Gemini"""
        
        # Prepare post content
        post_text = post.get('text', '')
        if not post_text and post.get('url'):
            # Try to fetch content from URL
            post_text = await self.fetch_url_content(post['url'])
            post_text = post_text[:3000]  # Limit to first 3k chars
        
        # Check for GitHub repo
        github_url = self.extract_github_url(post_text + ' ' + post.get('url', ''), post.get('url', ''))
        github_data = {}
        if github_url:
            github_data = await self.analyze_github_repo(github_url)
        
        # Main LLM analysis
        main_prompt = self.main_analysis_prompt.format(
            title=post['title'],
            url=post.get('url', 'No URL'),
            text=post_text[:2000],  # Limit text length
            author_karma=post.get('author_karma', 0)
        )
        
        try:
            response = self.model.generate_content(main_prompt)
            
            # Parse LLM response (assuming JSON)
            # In practice, you'd need better parsing/validation
            analysis_data = json.loads(response.text)
            
            # GitHub-specific analysis if applicable
            if github_data:
                github_prompt = self.github_analysis_prompt.format(
                    repo_url=github_url,
                    readme=github_data.get('readme_content', ''),
                    file_structure="[Would need repo clone for full analysis]",
                    recent_commits="[Would need API calls for commit history]"
                )
                
                github_response = self.model.generate_content(github_prompt)
                github_analysis = json.loads(github_response.text)
            else:
                github_analysis = {}
            
            # Combine all analysis
            super_gem = SuperGemAnalysis(
                post_id=post['id'],
                title=post['title'],
                url=post.get('url', ''),
                author=post['by'],
                author_karma=post.get('author_karma', 0),
                original_score=post.get('gem_score', 0),
                
                # LLM scores
                technical_innovation=analysis_data.get('technical_innovation', 0),
                problem_significance=analysis_data.get('problem_significance', 0),
                implementation_quality=analysis_data.get('implementation_quality', 0),
                community_value=analysis_data.get('community_value', 0),
                uniqueness_score=analysis_data.get('uniqueness', 0),
                
                # Specific checks
                is_open_source=analysis_data.get('is_open_source', False),
                has_working_demo=analysis_data.get('has_working_demo', False),
                has_documentation=analysis_data.get('has_documentation', False),
                is_commercially_focused=analysis_data.get('is_commercial', False),
                
                # GitHub stats
                github_stars=github_data.get('stars', 0),
                code_quality_score=github_analysis.get('code_quality', 0),
                readme_quality=github_analysis.get('readme_quality', 0),
                
                # Summary
                llm_reasoning=analysis_data.get('reasoning', ''),
                key_strengths=analysis_data.get('strengths', []),
                potential_concerns=analysis_data.get('concerns', []),
                similar_tools=analysis_data.get('similar_tools', []),
                
                # Calculate final score
                super_gem_score=self.calculate_super_gem_score(analysis_data, github_data)
            )
            
            return super_gem
            
        except Exception as e:
            print(f"LLM Analysis error for post {post['id']}: {e}")
            return None
    
    def calculate_super_gem_score(self, analysis: Dict, github_data: Dict) -> float:
        """Calculate final super gem score with weighted factors"""
        
        base_score = (
            analysis.get('technical_innovation', 0) * 0.25 +
            analysis.get('problem_significance', 0) * 0.25 +
            analysis.get('implementation_quality', 0) * 0.20 +
            analysis.get('community_value', 0) * 0.20 +
            analysis.get('uniqueness', 0) * 0.10
        )
        
        # Bonuses
        if analysis.get('is_open_source'):
            base_score += 0.1
        if analysis.get('has_working_demo'):
            base_score += 0.05
        if github_data.get('stars', 0) > 10:  # Early stars are a good sign
            base_score += 0.05
            
        # Penalties
        if analysis.get('is_commercial'):
            base_score -= 0.15
        if not analysis.get('has_documentation'):
            base_score -= 0.05
            
        return min(max(base_score, 0), 1)  # Clamp to 0-1
    
    async def get_top_gems(self, hours: int = 48, limit: int = 10) -> List[Dict]:
        """Get top gems from the last N hours from database"""
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        
        cutoff_time = datetime.now() - timedelta(hours=hours)
        cutoff_timestamp = int(cutoff_time.timestamp())
        
        # Query for top gems
        query = """
        SELECT * FROM analyzed_posts 
        WHERE created_at > ? 
        AND spam_likelihood < 0.3
        AND overall_interest > 0.4
        AND author_karma < 100
        ORDER BY overall_interest DESC
        LIMIT ?
        """
        
        cursor = conn.cursor()
        cursor.execute(query, (cutoff_timestamp, limit * 2))  # Get extra in case some fail
        
        posts = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return posts[:limit]
    
    def generate_static_html(self, super_gems: List[SuperGemAnalysis], output_path: str = "super-gems.html"):
        """Generate static HTML page with super gems"""
        
        html_template = Template("""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HN Super Gems - AI-Curated Hidden Treasures</title>
    <style>
        body {
            font-family: Verdana, Geneva, sans-serif;
            font-size: 10pt;
            color: #000;
            background-color: #f6f6ef;
            margin: 0;
            padding: 0;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 8px;
        }
        .header {
            background-color: #ff6600;
            padding: 8px;
            margin-bottom: 10px;
        }
        .header h1 {
            margin: 0;
            font-size: 14pt;
            font-weight: bold;
        }
        .header a {
            color: #000;
            text-decoration: none;
        }
        .subtitle {
            font-size: 9pt;
            color: #666;
            margin-top: 4px;
        }
        .gem {
            background-color: #fff;
            border: 1px solid #e0e0e0;
            margin-bottom: 20px;
            padding: 15px;
        }
        .gem-header {
            margin-bottom: 10px;
        }
        .gem-title {
            font-size: 12pt;
            font-weight: bold;
            margin-bottom: 4px;
        }
        .gem-title a {
            color: #000;
            text-decoration: none;
        }
        .gem-title a:hover {
            text-decoration: underline;
        }
        .gem-meta {
            font-size: 8pt;
            color: #666;
        }
        .gem-scores {
            display: flex;
            gap: 20px;
            margin: 10px 0;
            flex-wrap: wrap;
        }
        .score-item {
            background-color: #f8f8f8;
            padding: 5px 10px;
            border-radius: 3px;
            font-size: 9pt;
        }
        .score-high { background-color: #d4f4dd; }
        .score-medium { background-color: #fff4d4; }
        .score-low { background-color: #f4d4d4; }
        .gem-analysis {
            margin-top: 10px;
            padding: 10px;
            background-color: #f8f8f8;
            border-left: 3px solid #ff6600;
        }
        .strengths, .concerns {
            margin-top: 8px;
        }
        .strengths ul, .concerns ul {
            margin: 4px 0;
            padding-left: 20px;
        }
        .badges {
            display: flex;
            gap: 8px;
            margin-top: 8px;
        }
        .badge {
            background-color: #ff6600;
            color: #fff;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 8pt;
        }
        .badge.oss { background-color: #28a745; }
        .badge.demo { background-color: #17a2b8; }
        .super-score {
            font-size: 16pt;
            font-weight: bold;
            color: #ff6600;
            float: right;
        }
        .generated-time {
            text-align: center;
            color: #666;
            font-size: 8pt;
            margin-top: 20px;
        }
        .about {
            background-color: #fff;
            border: 1px solid #e0e0e0;
            padding: 15px;
            margin-bottom: 20px;
        }
    </style>
</head>
<body>
    <div class="header">
        <div class="container">
            <h1><a href="/">HN Super Gems</a></h1>
            <div class="subtitle">AI-curated hidden treasures from low-karma Hacker News accounts</div>
        </div>
    </div>
    
    <div class="container">
        <div class="about">
            <strong>About:</strong> These are the best hidden gems from the last 48 hours, discovered by 
            <a href="https://github.com/DG1001/hn-gems">hn-gems</a> and analyzed by AI for exceptional quality.
            Each post is from a low-karma account (&lt;100) but shows high potential value to the HN community.
            <br><br>
            <em>Why?</em> Great content from new users often gets overlooked. This tool helps surface quality posts
            that deserve more attention.
        </div>
        
        {% for gem in super_gems %}
        <div class="gem">
            <div class="super-score">{{ "%.1f"|format(gem.super_gem_score * 10) }}/10</div>
            
            <div class="gem-header">
                <div class="gem-title">
                    <a href="{{ gem.url or 'https://news.ycombinator.com/item?id=' + gem.post_id|string }}">
                        {{ gem.title }}
                    </a>
                </div>
                <div class="gem-meta">
                    by <a href="https://news.ycombinator.com/user?id={{ gem.author }}">{{ gem.author }}</a> 
                    ({{ gem.author_karma }} karma) | 
                    <a href="https://news.ycombinator.com/item?id={{ gem.post_id }}">discuss on HN</a>
                </div>
            </div>
            
            <div class="badges">
                {% if gem.is_open_source %}
                <span class="badge oss">Open Source</span>
                {% endif %}
                {% if gem.has_working_demo %}
                <span class="badge demo">Working Demo</span>
                {% endif %}
                {% if gem.github_stars > 0 %}
                <span class="badge">★ {{ gem.github_stars }} GitHub stars</span>
                {% endif %}
            </div>
            
            <div class="gem-scores">
                <div class="score-item {% if gem.technical_innovation > 0.7 %}score-high{% elif gem.technical_innovation > 0.4 %}score-medium{% else %}score-low{% endif %}">
                    Technical Innovation: {{ "%.0f"|format(gem.technical_innovation * 100) }}%
                </div>
                <div class="score-item {% if gem.problem_significance > 0.7 %}score-high{% elif gem.problem_significance > 0.4 %}score-medium{% else %}score-low{% endif %}">
                    Problem Significance: {{ "%.0f"|format(gem.problem_significance * 100) }}%
                </div>
                <div class="score-item {% if gem.implementation_quality > 0.7 %}score-high{% elif gem.implementation_quality > 0.4 %}score-medium{% else %}score-low{% endif %}">
                    Implementation: {{ "%.0f"|format(gem.implementation_quality * 100) }}%
                </div>
                <div class="score-item {% if gem.community_value > 0.7 %}score-high{% elif gem.community_value > 0.4 %}score-medium{% else %}score-low{% endif %}">
                    Community Value: {{ "%.0f"|format(gem.community_value * 100) }}%
                </div>
                <div class="score-item {% if gem.uniqueness_score > 0.7 %}score-high{% elif gem.uniqueness_score > 0.4 %}score-medium{% else %}score-low{% endif %}">
                    Uniqueness: {{ "%.0f"|format(gem.uniqueness_score * 100) }}%
                </div>
            </div>
            
            <div class="gem-analysis">
                <strong>AI Analysis:</strong> {{ gem.llm_reasoning }}
                
                {% if gem.key_strengths %}
                <div class="strengths">
                    <strong>Strengths:</strong>
                    <ul>
                        {% for strength in gem.key_strengths %}
                        <li>{{ strength }}</li>
                        {% endfor %}
                    </ul>
                </div>
                {% endif %}
                
                {% if gem.potential_concerns %}
                <div class="concerns">
                    <strong>Considerations:</strong>
                    <ul>
                        {% for concern in gem.potential_concerns %}
                        <li>{{ concern }}</li>
                        {% endfor %}
                    </ul>
                </div>
                {% endif %}
                
                {% if gem.similar_tools %}
                <div style="margin-top: 8px;">
                    <strong>Similar to:</strong> {{ ', '.join(gem.similar_tools) }}
                </div>
                {% endif %}
            </div>
        </div>
        {% endfor %}
        
        <div class="generated-time">
            Generated on {{ generation_time }} | 
            <a href="https://github.com/DG1001/hn-gems">Source Code</a> | 
            <a href="/">Live Gem Feed</a>
        </div>
    </div>
</body>
</html>
        """)
        
        # Sort by super gem score
        super_gems.sort(key=lambda x: x.super_gem_score, reverse=True)
        
        html_content = html_template.render(
            super_gems=super_gems,
            generation_time=datetime.now().strftime("%Y-%m-%d %H:%M UTC")
        )
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"Generated static HTML: {output_path}")
    
    async def run_analysis(self, hours: int = 48, top_n: int = 5):
        """Main method to run the super gems analysis"""
        
        print(f"Fetching top gems from last {hours} hours...")
        top_gems = await self.get_top_gems(hours=hours, limit=top_n * 2)
        
        if not top_gems:
            print("No gems found in the specified time range")
            return
        
        print(f"Found {len(top_gems)} candidates, analyzing with LLM...")
        
        super_gems = []
        for gem in top_gems:
            if len(super_gems) >= top_n:
                break
                
            print(f"Analyzing: {gem['title']}")
            analysis = await self.analyze_with_llm(gem)
            
            if analysis and analysis.super_gem_score > 0.5:  # Quality threshold
                super_gems.append(analysis)
                print(f"  ✓ Super gem! Score: {analysis.super_gem_score:.2f}")
            else:
                print(f"  ✗ Not qualified as super gem")
        
        if super_gems:
            print(f"\nGenerating static HTML with {len(super_gems)} super gems...")
            self.generate_static_html(super_gems)
            
            # Also save as JSON for potential API use
            with open('super-gems.json', 'w') as f:
                json.dump([
                    {
                        'post_id': g.post_id,
                        'title': g.title,
                        'url': g.url,
                        'author': g.author,
                        'super_gem_score': g.super_gem_score,
                        'analysis': {
                            'technical_innovation': g.technical_innovation,
                            'problem_significance': g.problem_significance,
                            'implementation_quality': g.implementation_quality,
                            'community_value': g.community_value,
                            'uniqueness': g.uniqueness_score
                        },
                        'badges': {
                            'is_open_source': g.is_open_source,
                            'has_demo': g.has_working_demo,
                            'github_stars': g.github_stars
                        },
                        'reasoning': g.llm_reasoning,
                        'strengths': g.key_strengths,
                        'concerns': g.potential_concerns
                    }
                    for g in super_gems
                ], f, indent=2)
        else:
            print("No posts qualified as super gems")


# Scheduler script
async def scheduled_super_gems_analysis():
    """Run this on a schedule (cron job or Python scheduler)"""
    
    # Get config from environment or config file
    GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
    ANALYSIS_HOURS = int(os.getenv('ANALYSIS_HOURS', '48'))
    TOP_N_GEMS = int(os.getenv('TOP_N_GEMS', '5'))
    
    analyzer = SuperGemsAnalyzer(
        gemini_api_key=GEMINI_API_KEY,
        db_path='hn_hidden_gems.db'
    )
    
    await analyzer.run_analysis(
        hours=ANALYSIS_HOURS,
        top_n=TOP_N_GEMS
    )

if __name__ == "__main__":
    # For testing
    asyncio.run(scheduled_super_gems_analysis())