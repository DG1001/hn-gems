# HN Hidden Gems Finder

A tool that discovers high-quality Hacker News posts from low-karma accounts that would otherwise be overlooked.

## Overview

The HN Hidden Gems Finder helps surface excellent content from new or low-karma Hacker News users that often gets buried despite being valuable. This addresses the problem where great "Show HN" posts and discussions get no traction simply because the author doesn't have established karma.

## Features

- **Real-time Hidden Gems Feed**: Continuously discovers overlooked quality posts
- **Hall of Fame**: Tracks discovered gems that later became popular
- **Success Metrics**: Monitors discovery accuracy and timing
- **Quality Analysis**: AI-powered content analysis to identify technical depth and originality
- **Multiple Notification Channels**: Email, Discord, Slack, and RSS feeds
- **Anti-spam Protection**: Advanced filtering to maintain high quality

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

4. **Run Application**
   ```bash
   python app.py
   ```

## Architecture

The system uses a dual API strategy:
- **HN Firebase API**: Real-time updates (no rate limits)
- **Algolia HN Search API**: Historical analysis and advanced filtering

Key components:
- **Data Collection**: Continuous monitoring of HN new posts
- **Quality Analysis**: AI-powered content evaluation
- **Storage**: SQLite for development, PostgreSQL for production
- **Web Interface**: Flask application with real-time updates
- **Background Processing**: Async analysis and notifications

## Configuration

Configure the application using environment variables:

- `FLASK_ENV`: development/production
- `DATABASE_URL`: Database connection string
- `OPENAI_API_KEY`: OpenAI API key for content analysis
- `ALGOLIA_APP_ID`: Algolia application ID
- `ALGOLIA_API_KEY`: Algolia API key
- `REDIS_URL`: Redis connection string
- `SMTP_*`: Email notification settings

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

## API Endpoints

- `GET /api/gems`: Latest hidden gems
- `GET /api/gems/hall-of-fame`: Hall of fame entries
- `GET /api/stats`: Success metrics
- `GET /feed.xml`: RSS feed

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