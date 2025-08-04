from algoliasearch.search_client import SearchClient
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from hn_hidden_gems.config import Config
from hn_hidden_gems.utils.logger import setup_logger

logger = setup_logger(__name__)

class AlgoliaHNAPI:
    """Client for Algolia HN Search API."""
    
    def __init__(self):
        if not Config.ALGOLIA_APP_ID or not Config.ALGOLIA_API_KEY:
            logger.warning("Algolia credentials not configured")
            self.client = None
            self.index = None
            return
            
        self.client = SearchClient.create(
            Config.ALGOLIA_APP_ID, 
            Config.ALGOLIA_API_KEY
        )
        self.index = self.client.init_index(Config.ALGOLIA_INDEX_NAME)
    
    def search_posts(
        self, 
        query: str = "", 
        tags: List[str] = None, 
        numeric_filters: List[str] = None,
        hits_per_page: int = 100,
        page: int = 0
    ) -> List[Dict]:
        """Search posts using Algolia."""
        if not self.index:
            logger.error("Algolia not configured")
            return []
        
        try:
            search_params = {
                'hitsPerPage': hits_per_page,
                'page': page
            }
            
            if tags:
                search_params['tagFilters'] = tags
            
            if numeric_filters:
                search_params['numericFilters'] = numeric_filters
            
            results = self.index.search(query, search_params)
            hits = results.get('hits', [])
            
            logger.debug(f"Algolia search returned {len(hits)} results")
            return hits
            
        except Exception as e:
            logger.error(f"Algolia search error: {e}")
            return []
    
    def get_low_karma_posts(
        self, 
        karma_threshold: int = 50, 
        hours_back: int = 24
    ) -> List[Dict]:
        """Get recent posts from low-karma authors."""
        if not self.index:
            return []
        
        # Calculate time range
        start_time = datetime.utcnow() - timedelta(hours=hours_back)
        start_timestamp = int(start_time.timestamp())
        
        numeric_filters = [
            f"created_at_i>{start_timestamp}",
            "points>=0"  # Exclude heavily downvoted posts
        ]
        
        tags = ["story"]  # Only get stories, not comments
        
        posts = self.search_posts(
            query="",
            tags=tags,
            numeric_filters=numeric_filters,
            hits_per_page=1000
        )
        
        # Filter by author karma (we'll need to fetch this separately)
        # For now, return all posts - karma filtering will happen in the main logic
        logger.info(f"Retrieved {len(posts)} posts from Algolia")
        return posts
    
    def search_similar_posts(self, title: str, url: str = None) -> List[Dict]:
        """Search for similar posts to detect duplicates."""
        if not self.index:
            return []
        
        try:
            # Search by title similarity
            title_results = self.search_posts(
                query=title,
                tags=["story"],
                hits_per_page=10
            )
            
            similar_posts = []
            
            # Add title matches
            for post in title_results:
                if post.get('title', '').lower() != title.lower():
                    similar_posts.append({
                        'post': post,
                        'similarity_type': 'title',
                        'similarity_score': 0.8  # Placeholder score
                    })
            
            # Search by URL if provided
            if url:
                url_results = self.search_posts(
                    query=url,
                    tags=["story"],
                    hits_per_page=5
                )
                
                for post in url_results:
                    if post.get('url') == url:
                        similar_posts.append({
                            'post': post,
                            'similarity_type': 'url',
                            'similarity_score': 1.0
                        })
            
            logger.debug(f"Found {len(similar_posts)} similar posts")
            return similar_posts
            
        except Exception as e:
            logger.error(f"Error searching similar posts: {e}")
            return []
    
    def get_historical_success_stories(self, days_back: int = 30) -> List[Dict]:
        """Get posts that started with low engagement but later succeeded."""
        if not self.index:
            return []
        
        start_time = datetime.utcnow() - timedelta(days=days_back)
        start_timestamp = int(start_time.timestamp())
        
        # Find posts that now have high points but may have started low
        numeric_filters = [
            f"created_at_i>{start_timestamp}",
            "points>=100"  # Posts that eventually succeeded
        ]
        
        posts = self.search_posts(
            query="",
            tags=["story"],
            numeric_filters=numeric_filters,
            hits_per_page=500
        )
        
        logger.info(f"Retrieved {len(posts)} potential success stories")
        return posts
    
    def search_by_keywords(self, keywords: List[str]) -> List[Dict]:
        """Search posts by specific keywords."""
        if not self.index or not keywords:
            return []
        
        query = " OR ".join(keywords)
        
        posts = self.search_posts(
            query=query,
            tags=["story"],
            hits_per_page=200
        )
        
        logger.debug(f"Found {len(posts)} posts matching keywords: {keywords}")
        return posts