import requests
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
from hn_hidden_gems.config import Config
from hn_hidden_gems.utils.logger import setup_logger

logger = setup_logger(__name__)

class HackerNewsAPI:
    """Client for Hacker News Firebase API."""
    
    def __init__(self):
        self.base_url = Config.HN_API_BASE
        self.session = requests.Session()
        self.seen_posts: Set[int] = set()
        
    def get_story_ids(self, story_type: str = "new", limit: int = 100) -> List[int]:
        """Get story IDs from HN API."""
        try:
            url = f"{self.base_url}/{story_type}stories.json"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            story_ids = response.json()[:limit]
            logger.debug(f"Retrieved {len(story_ids)} {story_type} story IDs")
            return story_ids
            
        except requests.RequestException as e:
            logger.error(f"Error fetching {story_type} stories: {e}")
            return []
    
    def get_item(self, item_id: int) -> Optional[Dict]:
        """Get item details by ID."""
        try:
            url = f"{self.base_url}/item/{item_id}.json"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            item = response.json()
            if not item:
                logger.debug(f"Item {item_id} not found or deleted")
                return None
                
            return item
            
        except requests.RequestException as e:
            logger.error(f"Error fetching item {item_id}: {e}")
            return None
    
    def get_user(self, username: str) -> Optional[Dict]:
        """Get user details by username."""
        try:
            url = f"{self.base_url}/user/{username}.json"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            user = response.json()
            if not user:
                logger.debug(f"User {username} not found")
                return None
                
            return user
            
        except requests.RequestException as e:
            logger.error(f"Error fetching user {username}: {e}")
            return None
    
    def get_posts_with_metadata(self, story_type: str = "new", limit: int = 100) -> List[Dict]:
        """Get posts with complete metadata including user karma."""
        story_ids = self.get_story_ids(story_type, limit)
        posts = []
        
        for story_id in story_ids:
            if story_id in self.seen_posts:
                continue
                
            item = self.get_item(story_id)
            if not item or item.get('type') != 'story':
                continue
            
            # Skip if deleted or dead
            if item.get('deleted') or item.get('dead'):
                continue
            
            # Get author information
            author = item.get('by')
            if author:
                user_data = self.get_user(author)
                if user_data:
                    item['author_karma'] = user_data.get('karma', 0)
                    item['account_age_days'] = self._calculate_account_age(
                        user_data.get('created', 0)
                    )
                else:
                    item['author_karma'] = 0
                    item['account_age_days'] = 0
            else:
                continue  # Skip posts without authors
            
            # Add processing timestamp
            item['processed_at'] = datetime.utcnow().isoformat()
            
            posts.append(item)
            self.seen_posts.add(story_id)
            
            # Small delay to be respectful
            time.sleep(0.1)
        
        logger.info(f"Retrieved {len(posts)} posts with metadata")
        return posts
    
    def _calculate_account_age(self, created_timestamp: int) -> int:
        """Calculate account age in days."""
        if not created_timestamp:
            return 0
        
        created_date = datetime.fromtimestamp(created_timestamp)
        return (datetime.utcnow() - created_date).days
    
    def get_recent_posts(self, hours: int = 6) -> List[Dict]:
        """Get posts from the last N hours."""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        cutoff_timestamp = int(cutoff_time.timestamp())
        
        posts = self.get_posts_with_metadata("new", 500)  # Get more to filter by time
        
        recent_posts = [
            post for post in posts 
            if post.get('time', 0) > cutoff_timestamp
        ]
        
        logger.info(f"Found {len(recent_posts)} posts from last {hours} hours")
        return recent_posts
    
    def clear_seen_posts(self):
        """Clear the seen posts cache."""
        self.seen_posts.clear()
        logger.info("Cleared seen posts cache")