#!/usr/bin/env python3
"""
Simple management script for HN Hidden Gems post collection service (no Redis required).
"""

import os
import sys
import time
import argparse
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def get_config():
    """Get current configuration."""
    interval = int(os.environ.get('POST_COLLECTION_INTERVAL_MINUTES', 5))
    port = int(os.environ.get('FLASK_PORT', 5000))
    return {
        'interval': interval,
        'enabled': interval > 0,
        'batch_size': int(os.environ.get('POST_COLLECTION_BATCH_SIZE', 25)),
        'max_stories': int(os.environ.get('POST_COLLECTION_MAX_STORIES', 500)),
        'port': port,
        'base_url': f'http://127.0.0.1:{port}',
    }

def status():
    """Show service status."""
    config = get_config()
    
    print("📊 HN Hidden Gems Post Collection Status")
    print("=" * 45)
    print(f"Configuration:")
    print(f"  Enabled: {'✅ Yes' if config['enabled'] else '❌ No'}")
    print(f"  Interval: {config['interval']} minutes")
    print(f"  Batch size: {config['batch_size']} posts")
    print(f"  Max stories: {config['max_stories']}")
    print(f"  Flask port: {config['port']}")
    print()
    
    print("Service Details:")
    print("  🔄 Background collection runs within the Flask app")
    print("  📝 No external dependencies (Redis/Celery) required")
    print("  ⚡ Automatic startup when Flask app starts")
    print("  🛑 Automatic shutdown when Flask app stops")
    print()
    
    # Try to get collection status from API if Flask app is running
    try:
        import requests
        response = requests.get(f'{config["base_url"]}/api/collection/status', timeout=5)
        if response.status_code == 200:
            api_status = response.json()
            print("Live Service Status (from running Flask app):")
            print(f"  Running: {'✅ Yes' if api_status.get('running') else '❌ No'}")
            
            if api_status.get('jobs'):
                print("  Scheduled Jobs:")
                for job in api_status['jobs']:
                    print(f"    - {job['name']}")
                    print(f"      Next run: {job['next_run'] or 'Not scheduled'}")
            
            stats = api_status.get('stats', {})
            if stats:
                print(f"  Statistics:")
                print(f"    Status: {stats.get('status', 'unknown')}")
                print(f"    Total runs: {stats.get('total_runs', 0)}")
                print(f"    Last run: {stats.get('last_run') or 'Never'}")
                if stats.get('last_duration'):
                    print(f"    Last duration: {stats['last_duration']:.1f}s")
                print(f"    Posts collected (last run): {stats.get('posts_collected', 0)}")
                print(f"    Gems found (last run): {stats.get('gems_found', 0)}")
                print(f"    Errors (last run): {stats.get('errors', 0)}")
        else:
            print(f"⚠️ Flask app not responding (HTTP {response.status_code})")
            
    except Exception as e:
        print("⚠️ Flask app not running or not accessible")
        print(f"   Start the Flask app to see live status: python app.py")
        print(f"   Make sure app is running on port {config['port']} or set FLASK_PORT environment variable")

def manual_collect(minutes_back=60):
    """Manually trigger collection via API."""
    config = get_config()
    
    if not config['enabled']:
        print("❌ Post collection is disabled")
        return False
    
    print(f"🔄 Triggering manual collection for last {minutes_back} minutes...")
    
    try:
        import requests
        response = requests.post(f'{config["base_url"]}/api/collection/trigger', 
                               json={'minutes_back': minutes_back}, 
                               timeout=10)
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ {result['message']}")
            print("   Collection is running in the background")
            print("   Use 'python manage_collector_simple.py status' to check progress")
            return True
        else:
            print(f"❌ Failed to trigger collection: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Failed to trigger collection: {e}")
        print("   Make sure the Flask app is running: python app.py")
        print(f"   and accessible on port {config['port']} (set FLASK_PORT if different)")
        return False

def start():
    """Instructions to start the service."""
    config = get_config()
    
    print("🚀 Starting HN Hidden Gems Post Collection Service")
    print("=" * 50)
    
    if not config['enabled']:
        print("❌ Post collection is currently disabled")
        print("   To enable, set: export POST_COLLECTION_INTERVAL_MINUTES=5")
        print("   Then restart the Flask app")
        return
    
    print(f"✅ Service is configured with {config['interval']} minute intervals")
    print()
    print("To start the service:")
    print("1. Make sure configuration is correct:")
    print(f"   export POST_COLLECTION_INTERVAL_MINUTES={config['interval']}")
    print("2. Start the Flask application:")
    print("   python app.py")
    print()
    print("The post collection service will automatically start with the Flask app!")
    print("No Redis or Celery required - everything runs in-process.")

def stop():
    """Instructions to stop the service."""
    print("🛑 Stopping HN Hidden Gems Post Collection Service")
    print("=" * 50)
    print("The service runs within the Flask app, so to stop it:")
    print("1. Stop the Flask app (Ctrl+C)")
    print("2. Or disable collection: export POST_COLLECTION_INTERVAL_MINUTES=0")
    print("   Then restart the Flask app")

def main():
    parser = argparse.ArgumentParser(description='Manage HN Hidden Gems post collection service (no Redis)')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Start command
    subparsers.add_parser('start', help='Show instructions to start the service')
    
    # Stop command
    subparsers.add_parser('stop', help='Show instructions to stop the service')
    
    # Status command
    subparsers.add_parser('status', help='Show service status')
    
    # Collect command
    collect_parser = subparsers.add_parser('collect', help='Manually trigger collection')
    collect_parser.add_argument('--minutes', type=int, default=60,
                               help='Minutes back to collect (default: 60)')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    if args.command == 'start':
        start()
    elif args.command == 'stop':
        stop()
    elif args.command == 'status':
        status()
    elif args.command == 'collect':
        manual_collect(args.minutes)

if __name__ == '__main__':
    main()