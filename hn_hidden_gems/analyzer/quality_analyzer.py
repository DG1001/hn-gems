import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import urlparse
from hn_hidden_gems.config import Config
from hn_hidden_gems.utils.logger import setup_logger

logger = setup_logger(__name__)

class QualityAnalyzer:
    """Analyzes post quality using various metrics and signals."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'HN Hidden Gems Finder Bot 1.0'
        })
    
    def analyze_post_quality(self, post: Dict) -> Dict[str, float]:
        """Comprehensive post quality analysis."""
        try:
            scores = {
                'technical_depth': 0.0,
                'originality': 0.0,
                'problem_solving': 0.0,
                'spam_likelihood': 0.0,
                'overall_interest': 0.0,
                'github_quality': 0.0,
                'domain_reputation': 0.0
            }
            
            title = post.get('title', '').lower()
            text = post.get('text', '').lower()
            url = post.get('url', '')
            
            # Technical depth analysis
            scores['technical_depth'] = self._analyze_technical_depth(title, text, url)
            
            # Originality scoring
            scores['originality'] = self._analyze_originality(title, text, url)
            
            # Problem solving potential
            scores['problem_solving'] = self._analyze_problem_solving(title, text)
            
            # Spam detection
            scores['spam_likelihood'] = self._detect_spam(title, text, url, post)
            
            # GitHub repository quality (if applicable)
            if 'github.com' in url:
                scores['github_quality'] = self._analyze_github_repo(url)
            
            # Domain reputation
            scores['domain_reputation'] = self._analyze_domain_reputation(url)
            
            # Calculate overall interest score
            scores['overall_interest'] = self._calculate_overall_score(scores)
            
            logger.debug(f"Quality analysis for post {post.get('id')}: {scores}")
            return scores
            
        except Exception as e:
            logger.error(f"Error analyzing post quality: {e}")
            return self._default_scores()
    
    def _analyze_technical_depth(self, title: str, text: str, url: str) -> float:
        """Analyze technical depth indicators."""
        tech_keywords = [
            'algorithm', 'implementation', 'architecture', 'performance',
            'open source', 'api', 'framework', 'database', 'docker',
            'kubernetes', 'ai', 'machine learning', 'compiler', 'rust',
            'golang', 'python', 'javascript', 'typescript', 'react',
            'vue', 'angular', 'node.js', 'postgresql', 'mongodb',
            'redis', 'elasticsearch', 'tensorflow', 'pytorch',
            'microservices', 'devops', 'ci/cd', 'testing', 'security'
        ]
        
        advanced_keywords = [
            'distributed systems', 'concurrency', 'parallel processing',
            'optimization', 'scalability', 'fault tolerance', 'consensus',
            'cryptography', 'blockchain', 'neural networks', 'deep learning',
            'compiler design', 'operating systems', 'memory management',
            'garbage collection', 'jit compilation', 'virtualization'
        ]
        
        combined_text = f"{title} {text}"
        
        # Count basic technical keywords
        basic_score = sum(1 for keyword in tech_keywords if keyword in combined_text)
        basic_score = min(basic_score / 5, 0.6)  # Cap at 0.6
        
        # Count advanced technical keywords (higher weight)
        advanced_score = sum(1 for keyword in advanced_keywords if keyword in combined_text)
        advanced_score = min(advanced_score / 3, 0.4) * 1.5  # Higher multiplier
        
        # GitHub/technical domains bonus
        tech_domains = ['github.com', 'arxiv.org', 'papers.withcode.com']
        domain_bonus = 0.2 if any(domain in url for domain in tech_domains) else 0
        
        total_score = min(basic_score + advanced_score + domain_bonus, 1.0)
        return total_score
    
    def _analyze_originality(self, title: str, text: str, url: str) -> float:
        """Analyze originality indicators."""
        score = 0.0
        
        # Show HN bonus
        if title.startswith('show hn:'):
            score += 0.4
        
        # Personal project indicators
        creation_words = ['built', 'created', 'made', 'developed', 'wrote', 'designed']
        if any(word in title for word in creation_words):
            score += 0.3
        
        # GitHub repository bonus
        if 'github.com' in url:
            score += 0.2
        
        # Personal domain indicators
        personal_indicators = ['my', 'i built', 'i made', 'i created', 'i wrote']
        if any(indicator in title for indicator in personal_indicators):
            score += 0.2
        
        # Demo/live site indicators
        demo_words = ['demo', 'try it', 'live', 'playground', 'interactive']
        if any(word in title or word in text for word in demo_words):
            score += 0.1
        
        return min(score, 1.0)
    
    def _analyze_problem_solving(self, title: str, text: str) -> float:
        """Analyze problem-solving potential."""
        problem_keywords = [
            'solution', 'solves', 'fixes', 'helps', 'easier', 'faster',
            'alternative', 'replacement', 'tool', 'utility', 'automates',
            'simplifies', 'improves', 'optimizes', 'reduces', 'eliminates'
        ]
        
        pain_point_keywords = [
            'frustrating', 'annoying', 'difficult', 'hard', 'impossible',
            'slow', 'inefficient', 'manual', 'tedious', 'repetitive'
        ]
        
        combined_text = f"{title} {text}"
        
        # Count problem-solving indicators
        problem_score = sum(1 for keyword in problem_keywords if keyword in combined_text)
        problem_score = min(problem_score / 3, 0.7)
        
        # Pain point identification bonus
        pain_score = sum(1 for keyword in pain_point_keywords if keyword in combined_text)
        pain_score = min(pain_score / 2, 0.3)
        
        return min(problem_score + pain_score, 1.0)
    
    def _detect_spam(self, title: str, text: str, url: str, post: Dict) -> float:
        """Detect spam indicators."""
        spam_score = 0.0
        
        # Title analysis
        if len(title) < 20:
            spam_score += 0.2
        
        if title.count('!') > 1:
            spam_score += 0.3
        
        if len(re.findall(r'[A-Z]{3,}', title)) > 2:
            spam_score += 0.4
        
        # Spam keywords
        spam_keywords = [
            'cryptocurrency', 'crypto', 'nft', 'blockchain', 'earn money',
            'make money', 'get rich', 'investment', 'trading', 'forex',
            'click here', 'limited time', 'act now', 'exclusive'
        ]
        
        combined_text = f"{title} {text}".lower()
        spam_keyword_count = sum(1 for keyword in spam_keywords if keyword in combined_text)
        spam_score += min(spam_keyword_count * 0.2, 0.6)
        
        # Suspicious patterns
        if '$$$' in combined_text or '💰' in combined_text:
            spam_score += 0.3
        
        # Empty or very short content
        if not url and len(text) < 50:
            spam_score += 0.4
        
        # Suspicious domains
        suspicious_domains = [
            'bit.ly', 'tinyurl.com', 'goo.gl', 't.co',
            'affiliate', 'referral', 'promo'
        ]
        
        if any(domain in url for domain in suspicious_domains):
            spam_score += 0.3
        
        return min(spam_score, 1.0)
    
    def _analyze_github_repo(self, url: str) -> float:
        """Analyze GitHub repository quality."""
        try:
            # Extract owner and repo from URL
            path_parts = urlparse(url).path.strip('/').split('/')
            if len(path_parts) < 2:
                return 0.0
            
            owner, repo = path_parts[0], path_parts[1]
            api_url = f"https://api.github.com/repos/{owner}/{repo}"
            
            response = self.session.get(api_url, timeout=5)
            if response.status_code != 200:
                return 0.0
            
            repo_data = response.json()
            
            score = 0.0
            
            # Stars (logarithmic scale)
            stars = repo_data.get('stargazers_count', 0)
            if stars > 0:
                score += min(0.3, stars / 100 * 0.1)
            
            # Recent activity
            updated_at = repo_data.get('updated_at')
            if updated_at:
                from dateutil.parser import parse
                last_update = parse(updated_at)
                days_since_update = (datetime.utcnow() - last_update.replace(tzinfo=None)).days
                if days_since_update < 30:
                    score += 0.2
                elif days_since_update < 90:
                    score += 0.1
            
            # README and description
            if repo_data.get('description'):
                score += 0.1
            
            # Language diversity (check languages API)
            languages_url = f"https://api.github.com/repos/{owner}/{repo}/languages"
            lang_response = self.session.get(languages_url, timeout=5)
            if lang_response.status_code == 200:
                languages = lang_response.json()
                if len(languages) > 1:
                    score += 0.1
            
            # License
            if repo_data.get('license'):
                score += 0.1
            
            # Issues and PRs (activity indicator)
            if repo_data.get('open_issues_count', 0) > 0:
                score += 0.1
            
            return min(score, 1.0)
            
        except Exception as e:
            logger.debug(f"Error analyzing GitHub repo {url}: {e}")
            return 0.0
    
    def _analyze_domain_reputation(self, url: str) -> float:
        """Analyze domain reputation."""
        if not url:
            return 0.5  # Neutral for text posts
        
        domain = urlparse(url).netloc.lower()
        
        # High reputation domains
        high_rep_domains = [
            'github.com', 'arxiv.org', 'papers.withcode.com', 'medium.com',
            'dev.to', 'hackernoon.com', 'towards*science.com', 
            'stackoverflow.com', 'reddit.com', 'youtube.com',
            'microsoft.com', 'google.com', 'amazon.com', 'facebook.com',
            'twitter.com', 'linkedin.com', 'ieee.org', 'acm.org'
        ]
        
        # Medium reputation domains
        medium_rep_domains = [
            'substack.com', 'hashnode.com', 'blogspot.com', 'wordpress.com',
            'gitlab.com', 'bitbucket.org', 'sourceforge.net'
        ]
        
        # Check for high reputation
        for high_domain in high_rep_domains:
            if high_domain.replace('*', '') in domain:
                return 0.8
        
        # Check for medium reputation
        for med_domain in medium_rep_domains:
            if med_domain in domain:
                return 0.6
        
        # Default for unknown domains
        return 0.4
    
    def _calculate_overall_score(self, scores: Dict[str, float]) -> float:
        """Calculate overall interest score."""
        weights = {
            'technical_depth': 0.25,
            'originality': 0.25,
            'problem_solving': 0.20,
            'github_quality': 0.15,
            'domain_reputation': 0.10,
            'spam_penalty': -0.5  # Negative weight for spam
        }
        
        overall = (
            scores['technical_depth'] * weights['technical_depth'] +
            scores['originality'] * weights['originality'] +
            scores['problem_solving'] * weights['problem_solving'] +
            scores['github_quality'] * weights['github_quality'] +
            scores['domain_reputation'] * weights['domain_reputation'] -
            scores['spam_likelihood'] * abs(weights['spam_penalty'])
        )
        
        return max(0.0, min(overall, 1.0))
    
    def _default_scores(self) -> Dict[str, float]:
        """Return default scores in case of error."""
        return {
            'technical_depth': 0.0,
            'originality': 0.0,
            'problem_solving': 0.0,
            'spam_likelihood': 0.5,
            'overall_interest': 0.0,
            'github_quality': 0.0,
            'domain_reputation': 0.4
        }