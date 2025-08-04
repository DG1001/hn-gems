# Flask Project

This is a Flask application created with XaresAICoder.

## Setup

1. Activate the virtual environment:
   ```bash
   source venv/bin/activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Run the application:
   ```bash
   python app.py
   ```

4. **Access your app:**
   
   When you start the Flask app, VS Code will automatically detect port 5000
   and provide the correct URL via port forwarding notifications.
   Simply click the provided link to access your application!

## Development

- Edit `app.py` to add your routes and logic
- Add new dependencies to `requirements.txt`
- Use the integrated terminal for package management

## AI Coding Assistance

XaresAICoder includes four powerful AI coding tools. Choose the one that best fits your workflow:

### 🤖 OpenCode SST - Multi-model AI Assistant
Best for: Project analysis, multi-model support, collaborative development

```bash
# Quick setup
setup_opencode

# Get started
opencode          # Start interactive session
# Then type: /init  # Initialize project analysis
```

**Key Commands:**
- `/init` - Analyze your project
- `/share` - Share session for collaboration
- `/help` - Show available commands

### 🤖 Aider - AI Pair Programming
Best for: Interactive coding, file editing, git integration

```bash
# Setup (requires API key)
export OPENAI_API_KEY=your_key_here  # or ANTHROPIC_API_KEY, GEMINI_API_KEY
setup_aider

# Get started
aider             # Start interactive pair programming
```

**Features:**
- Direct file editing with AI
- Automatic git commits
- Supports multiple AI models
- Works with your existing codebase

### 🤖 Gemini CLI - Google's AI Assistant  
Best for: Code generation, debugging, Google ecosystem integration

```bash
# Setup (requires API key from https://makersuite.google.com/app/apikey)
export GEMINI_API_KEY=your_key_here
setup_gemini

# Get started
gemini            # Start interactive session
```

**Features:**
- Natural language code generation
- Code explanation and debugging
- Project analysis and suggestions

### 🤖 Claude Code - Anthropic's Agentic Tool
Best for: Deep codebase understanding, multi-file editing, advanced workflows

```bash
# Setup (requires Claude Pro/Max or API billing)
setup_claude

# Get started
claude            # Start agentic coding session
```

**Features:**
- Understands entire codebase
- Multi-file editing capabilities
- Git workflow automation
- Advanced reasoning and planning

## Quick Setup for All Tools

Run this command to see setup instructions for all AI tools:
```bash
setup_ai_tools
```
