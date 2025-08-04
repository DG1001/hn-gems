// Main JavaScript for HN Hidden Gems application

// Global configuration
const CONFIG = {
    API_BASE: '/api',
    REFRESH_INTERVAL: 5 * 60 * 1000, // 5 minutes
    TOAST_DURATION: 5000
};

// Utility functions
const Utils = {
    /**
     * Format timestamp to relative time
     */
    formatRelativeTime: function(dateString) {
        if (!dateString) return 'unknown';
        
        const date = new Date(dateString);
        const now = new Date();
        const diffMs = now - date;
        const diffMinutes = Math.floor(diffMs / (1000 * 60));
        const diffHours = Math.floor(diffMs / (1000 * 60 * 60));
        const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
        
        if (diffMinutes < 1) return 'just now';
        if (diffMinutes < 60) return `${diffMinutes}m ago`;
        if (diffHours < 24) return `${diffHours}h ago`;
        if (diffDays < 7) return `${diffDays}d ago`;
        
        return date.toLocaleDateString();
    },

    /**
     * Format lead time duration
     */
    formatLeadTime: function(hours) {
        if (!hours) return '-';
        
        if (hours < 1) {
            return `${Math.round(hours * 60)}m`;
        } else if (hours < 24) {
            return `${Math.round(hours)}h`;
        } else {
            const days = Math.round(hours / 24);
            return `${days}d`;
        }
    },

    /**
     * Truncate text to specified length
     */
    truncateText: function(text, maxLength = 200) {
        if (!text || text.length <= maxLength) return text;
        return text.substring(0, maxLength) + '...';
    },

    /**
     * Get quality score color class
     */
    getScoreColorClass: function(score) {
        if (score >= 0.8) return 'success';
        if (score >= 0.6) return 'warning';
        if (score >= 0.4) return 'primary';
        return 'secondary';
    },

    /**
     * Show toast notification
     */
    showToast: function(message, type = 'info') {
        const toast = document.createElement('div');
        toast.className = `alert alert-${type} alert-dismissible fade show position-fixed`;
        toast.style.cssText = 'top: 20px; right: 20px; z-index: 9999; min-width: 300px;';
        toast.innerHTML = `
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        `;
        
        document.body.appendChild(toast);
        
        // Auto-remove after duration
        setTimeout(() => {
            if (toast.parentNode) {
                toast.remove();
            }
        }, CONFIG.TOAST_DURATION);
    },

    /**
     * Copy text to clipboard
     */
    copyToClipboard: async function(text) {
        try {
            await navigator.clipboard.writeText(text);
            return true;
        } catch (err) {
            console.error('Failed to copy to clipboard:', err);
            return false;
        }
    },

    /**
     * Share content using Web Share API or fallback
     */
    shareContent: async function(title, url) {
        if (navigator.share) {
            try {
                await navigator.share({ title, url });
                return true;
            } catch (err) {
                if (err.name !== 'AbortError') {
                    console.error('Error sharing:', err);
                }
            }
        }
        
        // Fallback: copy to clipboard
        const success = await Utils.copyToClipboard(url);
        if (success) {
            Utils.showToast('Link copied to clipboard!', 'success');
        } else {
            Utils.showToast('Failed to copy link', 'danger');
        }
        return success;
    }
};

// API client
const API = {
    /**
     * Make API request with error handling
     */
    request: async function(endpoint, options = {}) {
        try {
            const response = await fetch(`${CONFIG.API_BASE}${endpoint}`, {
                headers: {
                    'Content-Type': 'application/json',
                    ...options.headers
                },
                ...options
            });
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            return await response.json();
        } catch (error) {
            console.error(`API request failed for ${endpoint}:`, error);
            throw error;
        }
    },

    /**
     * Get hidden gems
     */
    getGems: function(params = {}) {
        const queryString = new URLSearchParams(params).toString();
        return API.request(`/gems${queryString ? '?' + queryString : ''}`);
    },

    /**
     * Get hall of fame entries
     */
    getHallOfFame: function(params = {}) {
        const queryString = new URLSearchParams(params).toString();
        return API.request(`/gems/hall-of-fame${queryString ? '?' + queryString : ''}`);
    },

    /**
     * Get statistics
     */
    getStats: function() {
        return API.request('/stats');
    },

    /**
     * Get specific post
     */
    getPost: function(hnId) {
        return API.request(`/posts/${hnId}`);
    },

    /**
     * Get user information
     */
    getUser: function(username) {
        return API.request(`/users/${username}`);
    },

    /**
     * Search posts
     */
    searchPosts: function(query, params = {}) {
        const searchParams = new URLSearchParams({ q: query, ...params });
        return API.request(`/search?${searchParams.toString()}`);
    }
};

// UI Components
const UI = {
    /**
     * Show loading state
     */
    showLoading: function(containerId, message = 'Loading...') {
        const container = document.getElementById(containerId);
        if (container) {
            container.innerHTML = `
                <div class="text-center py-5">
                    <i class="fas fa-spinner fa-spin fa-3x text-muted mb-3"></i>
                    <h3 class="text-muted">${message}</h3>
                </div>
            `;
        }
    },

    /**
     * Show error state
     */
    showError: function(containerId, message = 'An error occurred') {
        const container = document.getElementById(containerId);
        if (container) {
            container.innerHTML = `
                <div class="text-center py-5">
                    <i class="fas fa-exclamation-triangle fa-3x text-danger mb-3"></i>
                    <h3 class="text-danger">Error</h3>
                    <p class="text-muted">${message}</p>
                </div>
            `;
        }
    },

    /**
     * Show empty state
     */
    showEmpty: function(containerId, message = 'No data available') {
        const container = document.getElementById(containerId);
        if (container) {
            container.innerHTML = `
                <div class="text-center py-5">
                    <i class="fas fa-search fa-3x text-muted mb-3"></i>
                    <h3 class="text-muted">No Results</h3>
                    <p class="text-muted">${message}</p>
                </div>
            `;
        }
    }
};

// Event handlers
const EventHandlers = {
    /**
     * Handle gem sharing
     */
    shareGem: async function(button) {
        const card = button.closest('.gem-card, .hof-entry');
        if (!card) return;
        
        const titleElement = card.querySelector('.gem-title, .hof-title');
        const linkElement = card.querySelector('.hn-link');
        
        if (!titleElement || !linkElement) return;
        
        const title = `Hidden Gem: ${titleElement.textContent}`;
        const url = linkElement.href;
        
        const originalIcon = button.innerHTML;
        button.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
        
        await Utils.shareContent(title, url);
        
        // Reset button after a delay
        setTimeout(() => {
            button.innerHTML = originalIcon;
        }, 2000);
    },

    /**
     * Handle filter changes
     */
    onFilterChange: function() {
        // Debounce filter changes
        clearTimeout(EventHandlers._filterTimeout);
        EventHandlers._filterTimeout = setTimeout(() => {
            if (typeof loadGems === 'function') {
                loadGems();
            }
        }, 500);
    },

    /**
     * Handle search
     */
    onSearch: function(query) {
        if (!query || query.length < 2) return;
        
        API.searchPosts(query)
            .then(data => {
                console.log('Search results:', data);
                // Handle search results
            })
            .catch(error => {
                console.error('Search failed:', error);
                Utils.showToast('Search failed', 'danger');
            });
    }
};

// Global functions (exposed to window for inline event handlers)
window.shareGem = EventHandlers.shareGem;
window.Utils = Utils;
window.API = API;
window.UI = UI;

// Initialize application
document.addEventListener('DOMContentLoaded', function() {
    // Add filter change listeners
    const filterInputs = document.querySelectorAll('#karmaThreshold, #minScore, #timeRange');
    filterInputs.forEach(input => {
        input.addEventListener('change', EventHandlers.onFilterChange);
        input.addEventListener('input', EventHandlers.onFilterChange);
    });
    
    // Add keyboard shortcuts
    document.addEventListener('keydown', function(e) {
        // Ctrl/Cmd + R: Refresh current page data
        if ((e.ctrlKey || e.metaKey) && e.key === 'r' && e.shiftKey) {
            e.preventDefault();
            
            if (typeof loadGems === 'function') {
                loadGems();
            } else if (typeof loadHallOfFame === 'function') {
                loadHallOfFame();
            } else if (typeof loadStats === 'function') {
                loadStats();
            }
            
            Utils.showToast('Refreshing data...', 'info');
        }
    });
    
    // Initialize tooltips
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function(tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
    
    console.log('HN Hidden Gems app initialized');
});

// Service worker registration (for PWA functionality)
if ('serviceWorker' in navigator) {
    window.addEventListener('load', function() {
        navigator.serviceWorker.register('/sw.js')
            .then(function(registration) {
                console.log('ServiceWorker registration successful');
            })
            .catch(function(err) {
                console.log('ServiceWorker registration failed');
            });
    });
}