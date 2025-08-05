# HN Hidden Gems Finder

[![GitHub](https://img.shields.io/badge/GitHub-DG1001%2Fhn--gems-blue?logo=github)](https://github.com/DG1001/hn-gems)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

A tool that discovers high-quality Hacker News posts from low-karma accounts that would otherwise be overlooked.

## Overview

The HN Hidden Gems Finder helps surface excellent content from new or low-karma Hacker News users that often gets buried despite being valuable. This addresses the problem where great "Show HN" posts and discussions get no traction simply because the author doesn't have established karma.

## Features

- **Real-time Hidden Gems Feed**: Continuously discovers overlooked quality posts
- **Background Collection Service**: Automatic post collection with configurable intervals (no Redis required)
- **Hall of Fame**: Tracks discovered gems that later became popular
- **Success Metrics**: Monitors discovery accuracy and timing
- **Quality Analysis**: AI-powered content analysis to identify technical depth and originality
- **Multiple Notification Channels**: Email, Discord, Slack, and RSS feeds
- **Anti-spam Protection**: Advanced filtering to maintain high quality
- **Time-based Collection**: Intelligent collection that only processes posts from specified time windows

## Quick Start

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set Environment Variables**
   ```bash
   cp config/.env.example .env
   # Edit .env with your API keys
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
   # Background collection service starts automatically
   ```

## Architecture

The system uses the official Hacker News API for all data collection:
- **HN Firebase API**: Real-time updates with no rate limits

Key components:
- **Data Collection**: Automatic background collection of HN new posts
- **Quality Analysis**: AI-powered content evaluation with spam detection
- **Storage**: SQLite for development, PostgreSQL for production
- **Web Interface**: Flask application with real-time updates
- **Background Service**: APScheduler-based in-process collection (no Redis required)
- **Time-based Processing**: Collects only posts from specified time windows

## Configuration

Configure the application using environment variables:

### Core Settings
- `FLASK_ENV`: development/production
- `DATABASE_URL`: Database connection string
- `OPENAI_API_KEY`: OpenAI API key for content analysis

### Background Collection Service
- `POST_COLLECTION_INTERVAL_MINUTES=5`: Minutes between collections (0 to disable)
- `POST_COLLECTION_BATCH_SIZE=25`: Posts to commit per batch
- `POST_COLLECTION_MAX_STORIES=500`: Max story IDs to fetch per run

### Quality Thresholds
- `KARMA_THRESHOLD=100`: Max author karma for gems
- `MIN_INTEREST_SCORE=0.3`: Min quality score for gems

### Notification Settings
- `SMTP_*`: Email notification settings
- `DISCORD_WEBHOOK_URL`: Discord webhook for notifications
- `SLACK_WEBHOOK_URL`: Slack webhook for notifications

## Development

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black .

# Type checking
mypy .

# Linting
flake8
```

## Background Collection Service

The application includes an automatic background service for collecting new HN posts:

```bash
# Check service status
python scripts/manage_collector_simple.py status

# Manually trigger collection
python scripts/manage_collector_simple.py collect --minutes 60

# Flask CLI commands
flask config-collection          # Show current configuration
flask start-collector           # Start service manually
flask stop-collector            # Stop service manually
flask collect-now               # Manually trigger collection
flask collection-status         # Check service status
```

### Service Features
- **No External Dependencies**: No Redis or Celery required
- **Auto-start/stop**: Starts with Flask app, stops when app stops
- **Configurable Intervals**: Default 5 minutes, set to 0 to disable
- **Time-based Collection**: Only processes posts from specified time windows
- **Thread-safe**: Prevents overlapping collection runs
- **Progress Tracking**: Built-in statistics and status reporting

## API Endpoints

### Core Endpoints
- `GET /api/gems`: Latest hidden gems with filtering
- `GET /api/gems/hall-of-fame`: Hall of fame entries
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