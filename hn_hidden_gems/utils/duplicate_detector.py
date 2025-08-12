"""
Duplicate Post Detection System

This module provides functionality to detect duplicate posts based on various criteria:
- Identical URLs
- Identical titles  
- Similar text content (using content similarity scoring)
- Same author posting very similar content
"""

import difflib
import hashlib
import re
from typing import List, Dict, Tuple, Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

class DuplicateDetector:
    """Detects duplicate posts using multiple criteria."""
    
    def __init__(self, 
                 url_similarity_threshold: float = 0.95,
                 title_similarity_threshold: float = 0.85,
                 content_similarity_threshold: float = 0.8,
                 same_author_threshold: float = 0.7):
        """
        Initialize the duplicate detector.
        
        Args:
            url_similarity_threshold: Minimum similarity score for URLs (0.0 to 1.0)
            title_similarity_threshold: Minimum similarity score for titles
            content_similarity_threshold: Minimum similarity score for content
            same_author_threshold: Lower threshold when posts are by same author
        """
        self.url_similarity_threshold = url_similarity_threshold
        self.title_similarity_threshold = title_similarity_threshold
        self.content_similarity_threshold = content_similarity_threshold
        self.same_author_threshold = same_author_threshold
    
    def normalize_url(self, url: str) -> str:
        """
        Normalize URL by removing tracking parameters and fragments.
        
        Args:
            url: Original URL
            
        Returns:
            Normalized URL string
        """
        if not url:
            return ""
            
        try:
            parsed = urlparse(url.lower())
            
            # Remove common tracking parameters
            tracking_params = {
                'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
                'fbclid', 'gclid', 'mc_cid', 'mc_eid', '_ga', 'ref', 'source'
            }
            
            query_params = parse_qs(parsed.query)
            filtered_params = {
                k: v for k, v in query_params.items() 
                if k.lower() not in tracking_params
            }
            
            # Rebuild query string
            new_query = urlencode(filtered_params, doseq=True)
            
            # Remove fragment and rebuild URL
            normalized = urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path.rstrip('/'),  # Remove trailing slash
                parsed.params,
                new_query,
                ""  # Remove fragment
            ))
            
            return normalized
            
        except Exception:
            # If URL parsing fails, just return lowercased version
            return url.lower().strip()
    
    def normalize_title(self, title: str) -> str:
        """
        Normalize title by removing common variations.
        
        Args:
            title: Original title
            
        Returns:
            Normalized title string
        """
        if not title:
            return ""
            
        # Convert to lowercase and strip whitespace
        normalized = title.lower().strip()
        
        # Remove punctuation variations
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        
        # Normalize whitespace
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized
    
    def normalize_content(self, text: str) -> str:
        """
        Normalize post content/text for comparison.
        
        Args:
            text: Original text content
            
        Returns:
            Normalized text string
        """
        if not text:
            return ""
            
        # Remove HTML tags if any
        text = re.sub(r'<[^>]+>', '', text)
        
        # Convert to lowercase and normalize whitespace
        normalized = re.sub(r'\s+', ' ', text.lower().strip())
        
        # Remove common HN prefixes/suffixes
        prefixes_to_remove = [
            r'^ask hn:?\s*',
            r'^show hn:?\s*',
            r'^tell hn:?\s*',
            r'^hn:?\s*',
            r'^poll:?\s*'
        ]
        
        for prefix in prefixes_to_remove:
            normalized = re.sub(prefix, '', normalized)
        
        return normalized.strip()
    
    def calculate_similarity(self, text1: str, text2: str) -> float:
        """
        Calculate similarity between two text strings using sequence matching.
        
        Args:
            text1: First text string
            text2: Second text string
            
        Returns:
            Similarity score between 0.0 and 1.0
        """
        if not text1 and not text2:
            return 1.0
        if not text1 or not text2:
            return 0.0
            
        # Use difflib for sequence matching
        return difflib.SequenceMatcher(None, text1, text2).ratio()
    
    def get_content_hash(self, title: str, url: str, text: str) -> str:
        """
        Generate a hash of normalized content for quick duplicate detection.
        
        Args:
            title: Post title
            url: Post URL
            text: Post text content
            
        Returns:
            SHA256 hash of normalized content
        """
        normalized_title = self.normalize_title(title or "")
        normalized_url = self.normalize_url(url or "")
        normalized_text = self.normalize_content(text or "")
        
        # Combine all normalized content
        combined = f"{normalized_title}|{normalized_url}|{normalized_text}"
        
        return hashlib.sha256(combined.encode('utf-8')).hexdigest()
    
    def is_duplicate(self, post1_data: Dict, post2_data: Dict) -> Tuple[bool, Dict]:
        """
        Determine if two posts are duplicates.
        
        Args:
            post1_data: First post data dict with keys: title, url, text, author
            post2_data: Second post data dict with keys: title, url, text, author
            
        Returns:
            Tuple of (is_duplicate: bool, similarity_scores: Dict)
        """
        # Extract and normalize data
        title1 = self.normalize_title(post1_data.get('title', ''))
        title2 = self.normalize_title(post2_data.get('title', ''))
        
        url1 = self.normalize_url(post1_data.get('url', ''))
        url2 = self.normalize_url(post2_data.get('url', ''))
        
        text1 = self.normalize_content(post1_data.get('text', ''))
        text2 = self.normalize_content(post2_data.get('text', ''))
        
        author1 = post1_data.get('author', '').lower()
        author2 = post2_data.get('author', '').lower()
        
        same_author = author1 == author2 and author1 != ''
        
        # Calculate similarity scores
        url_similarity = self.calculate_similarity(url1, url2) if url1 and url2 else 0.0
        title_similarity = self.calculate_similarity(title1, title2)
        content_similarity = self.calculate_similarity(text1, text2) if text1 and text2 else 0.0
        
        similarity_scores = {
            'url_similarity': url_similarity,
            'title_similarity': title_similarity,
            'content_similarity': content_similarity,
            'same_author': same_author,
            'author1': author1,
            'author2': author2
        }
        
        # Determine if duplicate based on multiple criteria
        is_duplicate = False
        duplicate_reasons = []
        
        # Exact URL match (high confidence)
        if url1 and url2 and url_similarity >= self.url_similarity_threshold:
            is_duplicate = True
            duplicate_reasons.append(f"URL similarity: {url_similarity:.3f}")
        
        # High title similarity
        if title_similarity >= self.title_similarity_threshold:
            if same_author and title_similarity >= self.same_author_threshold:
                is_duplicate = True
                duplicate_reasons.append(f"Same author, title similarity: {title_similarity:.3f}")
            elif title_similarity >= self.title_similarity_threshold:
                is_duplicate = True
                duplicate_reasons.append(f"Title similarity: {title_similarity:.3f}")
        
        # High content similarity (if both have content)
        if text1 and text2 and content_similarity >= self.content_similarity_threshold:
            is_duplicate = True
            duplicate_reasons.append(f"Content similarity: {content_similarity:.3f}")
        
        # Same author posting very similar content
        if same_author and (
            title_similarity >= self.same_author_threshold or 
            content_similarity >= self.same_author_threshold
        ):
            is_duplicate = True
            if f"Same author, title similarity: {title_similarity:.3f}" not in duplicate_reasons:
                duplicate_reasons.append(f"Same author, similar content (T:{title_similarity:.3f}, C:{content_similarity:.3f})")
        
        similarity_scores['is_duplicate'] = is_duplicate
        similarity_scores['duplicate_reasons'] = duplicate_reasons
        similarity_scores['confidence_score'] = max(url_similarity, title_similarity, content_similarity)
        
        return is_duplicate, similarity_scores
    
    def find_duplicates_in_list(self, posts: List[Dict]) -> List[Tuple[Dict, Dict, Dict]]:
        """
        Find all duplicate pairs in a list of posts.
        
        Args:
            posts: List of post dictionaries
            
        Returns:
            List of tuples: (post1, post2, similarity_data)
        """
        duplicates = []
        
        # Create content hashes for quick screening
        post_hashes = {}
        for post in posts:
            content_hash = self.get_content_hash(
                post.get('title', ''),
                post.get('url', ''),
                post.get('text', '')
            )
            if content_hash in post_hashes:
                # Exact content match found
                post_hashes[content_hash].append(post)
            else:
                post_hashes[content_hash] = [post]
        
        # Check exact hash matches first
        for hash_key, hash_posts in post_hashes.items():
            if len(hash_posts) > 1:
                for i in range(len(hash_posts)):
                    for j in range(i + 1, len(hash_posts)):
                        similarity_data = {
                            'url_similarity': 1.0,
                            'title_similarity': 1.0, 
                            'content_similarity': 1.0,
                            'same_author': hash_posts[i].get('author', '') == hash_posts[j].get('author', ''),
                            'is_duplicate': True,
                            'duplicate_reasons': ['Exact content match'],
                            'confidence_score': 1.0
                        }
                        duplicates.append((hash_posts[i], hash_posts[j], similarity_data))
        
        # Then check for fuzzy matches
        for i in range(len(posts)):
            for j in range(i + 1, len(posts)):
                # Skip if already found as exact match
                hash1 = self.get_content_hash(
                    posts[i].get('title', ''), 
                    posts[i].get('url', ''), 
                    posts[i].get('text', '')
                )
                hash2 = self.get_content_hash(
                    posts[j].get('title', ''), 
                    posts[j].get('url', ''), 
                    posts[j].get('text', '')
                )
                
                if hash1 == hash2:
                    continue  # Already processed as exact match
                
                is_dup, similarity = self.is_duplicate(posts[i], posts[j])
                if is_dup:
                    duplicates.append((posts[i], posts[j], similarity))
        
        return duplicates
    
    def get_duplicate_action_recommendation(self, post1: Dict, post2: Dict, similarity: Dict) -> Dict:
        """
        Recommend action to take for a duplicate pair.
        
        Args:
            post1: First post data
            post2: Second post data  
            similarity: Similarity analysis results
            
        Returns:
            Dictionary with recommended action and reasoning
        """
        # Get HN IDs and scores
        hn_id1 = post1.get('hn_id', 0)
        hn_id2 = post2.get('hn_id', 0)
        score1 = post1.get('score', 0) or post1.get('current_hn_score', 0)
        score2 = post2.get('score', 0) or post2.get('current_hn_score', 0)
        created1 = post1.get('hn_created_at') or post1.get('created_at')
        created2 = post2.get('hn_created_at') or post2.get('created_at')
        
        recommendation = {
            'action': 'remove_lower_quality',
            'keep_post': None,
            'remove_post': None,
            'reasoning': []
        }
        
        # Determine which post to keep based on multiple factors
        keep_post1_reasons = []
        keep_post2_reasons = []
        
        # Prefer higher HN score
        if score1 > score2:
            keep_post1_reasons.append(f"Higher HN score ({score1} vs {score2})")
        elif score2 > score1:
            keep_post2_reasons.append(f"Higher HN score ({score2} vs {score1})")
        
        # Prefer earlier post (original)
        if created1 and created2:
            if created1 < created2:
                keep_post1_reasons.append("Posted earlier (likely original)")
            elif created2 < created1:
                keep_post2_reasons.append("Posted earlier (likely original)")
        
        # Prefer lower HN ID (usually means posted earlier)
        if hn_id1 < hn_id2:
            keep_post1_reasons.append(f"Lower HN ID ({hn_id1} vs {hn_id2})")
        elif hn_id2 < hn_id1:
            keep_post2_reasons.append(f"Lower HN ID ({hn_id2} vs {hn_id1})")
        
        # Check if same author (might be spam behavior)
        if similarity.get('same_author', False):
            recommendation['action'] = 'flag_spam_behavior'
            recommendation['reasoning'].append("Same author posting duplicate content")
        
        # Decide which post to keep
        if len(keep_post1_reasons) > len(keep_post2_reasons):
            recommendation['keep_post'] = post1
            recommendation['remove_post'] = post2
            recommendation['reasoning'].extend([f"Keep post {hn_id1}: {', '.join(keep_post1_reasons)}"])
        elif len(keep_post2_reasons) > len(keep_post1_reasons):
            recommendation['keep_post'] = post2
            recommendation['remove_post'] = post1
            recommendation['reasoning'].extend([f"Keep post {hn_id2}: {', '.join(keep_post2_reasons)}"])
        else:
            # Tie-breaker: keep the one with lower HN ID
            if hn_id1 < hn_id2:
                recommendation['keep_post'] = post1
                recommendation['remove_post'] = post2
                recommendation['reasoning'].append(f"Tie-breaker: keeping lower HN ID {hn_id1}")
            else:
                recommendation['keep_post'] = post2
                recommendation['remove_post'] = post1
                recommendation['reasoning'].append(f"Tie-breaker: keeping lower HN ID {hn_id2}")
        
        # Add confidence level
        confidence = similarity.get('confidence_score', 0.0)
        if confidence >= 0.9:
            recommendation['confidence'] = 'high'
        elif confidence >= 0.7:
            recommendation['confidence'] = 'medium'
        else:
            recommendation['confidence'] = 'low'
        
        return recommendation