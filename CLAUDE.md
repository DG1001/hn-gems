# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the HN Hidden Gems Finder project - a tool that discovers high-quality Hacker News posts from low-karma accounts that would otherwise be overlooked. The project is a Flask web application that analyzes HN posts using the official Hacker News API and Algolia HN Search API.

## Development Environment

### Dependencies
The project uses Python 3.11+ with Flask as the web framework. Install dependencies with:
```bash
pip install -r requirements.txt
```

Core dependencies include:
- Flask 3.1.1 (web framework)
- python-dotenv 1.1.1 (environment configuration)
- requests 2.32.4 (HTTP client for APIs)

### Virtual Environment
A virtual environment is already set up in the `venv/` directory. Activate it with:
```bash
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate  # Windows
```

## Architecture

### Core Components (Planned)
Based on the project specification in `prompt.md`, the system will consist of:

1. **HN Data Collection**: Uses dual API strategy with HN Firebase API (no rate limits) and Algolia HN Search API
2. **Quality Analysis**: LLM-based content analysis to identify overlooked quality posts
3. **Storage Layer**: SQLite for development, PostgreSQL for production
4. **Web Interface**: Flask application with real-time hidden gems feed
5. **Background Processing**: Async processing for post analysis and notifications

### Key Features to Implement
- **Hidden Gems Detection**: Find quality posts from low-karma accounts
- **Hall of Fame**: Track discovered gems that later became popular
- **Success Metrics**: Monitor discovery accuracy and timing
- **Notification System**: Email, Discord, Slack, and RSS feeds

## Development Commands

### Running the Application
```bash
# Development server
python app.py  # (when app.py is created)
# or
flask run

# Production server (future)
gunicorn app:app
```

### Testing
```bash
# Run tests (when test suite is implemented)
python -m pytest
# or
pytest
```

### Database Operations
```bash
# Database migrations (when implemented)
flask db upgrade
flask db migrate -m "description"
```

## Project Structure (To Be Created)

The project will follow this structure:
```
hn_hidden_gems/
├── __init__.py
├── models/          # Database models
├── api/            # API clients (HN, Algolia)
├── analyzer/       # Quality analysis logic
├── web/            # Flask routes and templates
└── utils/          # Utility functions

static/             # CSS, JS, images
templates/          # Jinja2 templates
tests/              # Test suite
config/             # Configuration files
```

## API Integration

### Hacker News API
- Base URL: https://hacker-news.firebaseio.com/v0
- No rate limits - can poll every 30-60 seconds
- Endpoints: /newstories.json, /item/{id}.json, /user/{username}.json

### Algolia HN Search API
- Used for historical analysis and advanced filtering
- Official and free to use

## Configuration

Environment variables will be managed through `.env` files:
- `FLASK_ENV`: development/production
- `DATABASE_URL`: Database connection string
- `OPENAI_API_KEY`: For LLM analysis
- `NOTIFICATION_*`: Various notification service credentials

## Key Implementation Notes

1. **Low-Karma Focus**: Core mission is helping good content from new users get discovered
2. **Spam Filtering**: Must maintain high quality through excellent spam detection
3. **Real-Time Updates**: System should handle HN traffic spikes efficiently
4. **Success Tracking**: Track which discovered gems later become popular

## Current State

The repository currently contains:
- `requirements.txt` with basic Flask dependencies
- `prompt.md` with detailed project specification
- `venv/` directory with Python virtual environment
- Basic project structure ready for development

No main application code has been implemented yet - the project is in the initial setup phase.