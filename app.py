from flask import Flask, render_template, jsonify
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key')

@app.route('/')
def index():
    return '''
    <h1>🎉 Welcome to XaresAICoder!</h1>
    <p>Your Flask application is running successfully.</p>
    <p><a href="/api/status">Check API Status</a></p>
    <p><strong>Next steps:</strong></p>
    <ul>
        <li>Edit this file (app.py) to build your application</li>
        <li>Use the terminal to install additional packages: <code>pip install package-name</code></li>
        <li>Run the app: <code>python app.py</code></li>
    </ul>
    '''

@app.route('/api/status')
def api_status():
    return jsonify({
        'status': 'running',
        'message': 'Flask API is working!',
        'framework': 'Flask',
        'python_version': '3.11+'
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
