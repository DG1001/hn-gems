# HN Hidden Gems Finder

[![GitHub](https://img.shields.io/badge/GitHub-DG1001%2Fhn--gems-blue?logo=github)](https://github.com/DG1001/hn-gems)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

A tool that discovers high-quality Hacker News posts from low-karma accounts that would otherwise be overlooked.

## Overview

The HN Hidden Gems Finder helps surface excellent content from new or low-karma Hacker News users that often gets buried despite being valuable. This addresses the problem where great "Show HN" posts and discussions get no traction simply because the author doesn't have established karma.

## Features

- **Real-time Hidden Gems Feed**: Continuously discovers overlooked quality posts (every 5 minutes)
- **Automated Background Services**: 
  - **Post Collection**: Automatic discovery and analysis with configurable intervals (no Redis required)
  - **Hall of Fame Monitoring**: Tracks gems that achieve success and automatically promotes them (every 6 hours)
  - **Super Gems Analysis**: AI-powered deep analysis of top gems using Google Gemini with user-friendly visual scoring (every 6 hours)
- **Hall of Fame**: Automated tracking of discovered gems that later became popular (≥100 points)
- **Success Metrics**: Real-time monitoring of discovery accuracy and timing
- **Quality Analysis**: AI-powered content analysis to identify technical depth and originality
- **Anti-spam Protection**: Advanced filtering to maintain high quality
- **Visual Scoring System**: User-friendly star ratings and professional dot indicators instead of intimidating numerical scores
- **Knowledge-Aware AI**: Smart evaluation system that avoids penalizing posts for recent technology releases
- **Time-based Collection**: Intelligent collection that only processes posts from specified time windows

## Quick Start

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Environment** (optional):
   ```bash
   cp .env.sample .env
   # Edit .env to customize settings
   ```

3. **Initialize Database**
   ```bash
   python -m hn_hidden_gems.models.init_db
   ```

4. **Configure Collection Service (Optional)**
   ```bash
   # Set collection interval (default: 5 minutes, 0 to disable)
   export POST_COLLECTION_INTERVAL_MINUTES=5
   ```

5. **Run Application**
   ```bash
   python app.py
   ```
   
   The application automatically starts all background services:
   - **Post Collection**: Discovers new gems every 5 minutes
   - **Hall of Fame Monitoring**: Checks for gem success every 6 hours
   - **Super Gems Analysis**: Deep AI analysis of top gems every 6 hours

## Architecture

The system uses the official Hacker News API for all data collection:
- **HN Firebase API**: Real-time updates with no rate limits

Key components:
- **Data Collection**: Automatic background collection of HN new posts
- **Quality Analysis**: AI-powered content evaluation with spam detection
- **Super Gems Analysis**: Advanced AI evaluation with visual scoring and knowledge-aware assessments
- **Storage**: SQLite for development, PostgreSQL for production
- **Web Interface**: Flask application with real-time updates
- **Background Service**: APScheduler-based in-process collection (no Redis required)
- **Time-based Processing**: Collects only posts from specified time windows

### Super Gems Analysis System

The Super Gems feature provides comprehensive AI-powered analysis of top hidden gems:

**Visual Scoring System:**
- ⭐⭐⭐⭐⭐ **Star ratings** for overall quality (instead of intimidating numerical scores)
- **Professional dot indicators** for detailed metrics:
  - ●●●● Exceptional (91-100%)
  - ●●● Excellent (76-90%)
  - ●● Good (51-75%)
  - ● Basic (0-50%)

**Smart AI Analysis:**
- **Knowledge-aware evaluation** that doesn't penalize posts for recent technology releases
- **Technical merit focus** over factual verification for emerging technologies
- **Automatic bias correction** for outdated knowledge assumptions
- **Comprehensive GitHub integration** for code quality assessment

**Analysis Dimensions:**
- Technical Innovation
- Problem Significance  
- Implementation Quality
- Community Value
- Uniqueness Score

## Configuration

Configure the application using environment variables:

### Core Settings
- `FLASK_ENV`: development/production
- `DATABASE_URL`: Database connection string
- `SECRET_KEY`: Flask secret key for security
- `HOST`: Server host (default: 127.0.0.1)
- `PORT`: Server port (default: 5000)

### Background Services
- `POST_COLLECTION_INTERVAL_MINUTES=5`: Minutes between post collections (0 to disable)
- `POST_COLLECTION_BATCH_SIZE=25`: Posts to commit per batch
- `POST_COLLECTION_MAX_STORIES=500`: Max story IDs to fetch per run
- `HALL_OF_FAME_INTERVAL_HOURS=6`: Hours between Hall of Fame monitoring (0 to disable)
- `SUPER_GEMS_INTERVAL_HOURS=6`: Hours between super gems analysis (0 to disable)
- `SUPER_GEMS_ANALYSIS_HOURS=48`: Hours back to analyze for super gems
- `SUPER_GEMS_TOP_N=5`: Number of top gems to analyze per run

### Quality Thresholds
- `KARMA_THRESHOLD=100`: Max author karma for gems
- `MIN_INTEREST_SCORE=0.3`: Min quality score for gems

### Super Gems Analysis
- `GEMINI_API_KEY`: Google Gemini API key for super gems analysis (required for super gems feature)
- The system automatically applies knowledge-aware evaluation to avoid penalizing recent technology releases
- Uses temperature=0.1 for consistent, focused AI responses
- Generates both HTML and JSON output for comprehensive analysis results

### Logging Settings
- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `LOG_FILE`: Log file path (default: logs/app.log)

## Development

```bash
# Run tests (when implemented)
pytest

# Check service status
flask collection-status

# View configuration
flask config-collection
```

## Background Collection Service

The application includes an automatic background service for collecting new HN posts:

```bash
# Check service status
python scripts/manage_collector_simple.py status

# Manually trigger collection
python scripts/manage_collector_simple.py collect --minutes 60

# Flask CLI commands
flask config-collection          # Show configuration for both services
flask start-collector           # Start both services manually
flask stop-collector            # Stop both services manually
flask collect-now               # Manually trigger post collection
flask monitor-gems              # Manually trigger Hall of Fame monitoring
flask analyze-super-gems        # Manually trigger super gems analysis
flask collection-status         # Check status of all services
```

### Service Features
- **Triple Background Services**: Post collection + Hall of Fame monitoring + Super gems analysis
- **No External Dependencies**: No Redis or Celery required
- **Auto-start/stop**: All services start with Flask app, stop when app stops
- **Configurable Intervals**: 
  - Post collection: Default 5 minutes (set to 0 to disable)
  - Hall of Fame monitoring: Default 6 hours (set to 0 to disable)
  - Super gems analysis: Default 6 hours (set to 0 to disable)
- **Time-based Collection**: Only processes posts from specified time windows
- **Automated Success Tracking**: Promotes gems to Hall of Fame when they reach ≥100 points
- **Thread-safe**: Prevents overlapping collection runs
- **Progress Tracking**: Built-in statistics and status reporting for all services

## API Endpoints

### Core Endpoints
- `GET /api/gems`: Latest hidden gems with filtering
- `GET /api/gems/hall-of-fame`: Hall of fame entries
- `GET /super-gems`: AI-curated super gems analysis page with visual scoring
- `GET /super-gems.html`: Static HTML version of super gems analysis
- `GET /super-gems.json`: JSON API for super gems analysis data
- `GET /api/stats`: Success metrics and statistics
- `GET /api/posts/<hn_id>`: Get specific post by HN ID
- `GET /api/users/<username>`: Get user information
- `GET /api/search?q=<query>`: Search posts by title/content
- `GET /feed.xml`: RSS feed of hidden gems

### Collection Service Endpoints
- `GET /api/collection/status`: Service status and statistics
- `POST /api/collection/trigger`: Manually trigger collection
- `GET /api/collection/config`: Current configuration

### Utility Endpoints
- `GET /api/health`: Health check endpoint

## Development Tools

This project was developed using:
- **[XaresAICoder](https://github.com/DG1001/XaresAICoder)** - Open-source browser IDE with integrated AI coding assistants
- **[Claude Code](https://claude.ai/code)** - AI-powered development assistant for code analysis and implementation

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## Support

For issues and feature requests, please use the GitHub issue tracker.