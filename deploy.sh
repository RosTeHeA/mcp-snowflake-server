#!/bin/bash

# Deploy script for MCP Snowflake Server
# Following the same pattern as your iridium-web-services deployment

set -e  # Exit on any error

# Configuration - Using your existing SSH setup
SSH_ALIAS="do-droplet"  # Your existing SSH alias
SERVER_PATH="/home/appuser/mcp-snowflake-server"
LOCAL_PATH="/Users/msorensen/Documents/GitHub/mcp-snowflake-server"

echo "🚀 Deploying MCP Snowflake Server to server..."
echo "📍 Local path: $LOCAL_PATH"
echo "🌐 Server: $SSH_ALIAS:$SERVER_PATH"

# Check if we're in the right directory
if [ ! -f "pyproject.toml" ]; then
    echo "❌ Error: Not in the correct project directory"
    echo "Please run this script from the mcp-snowflake-server directory"
    exit 1
fi

# Check for uncommitted changes
if [ -n "$(git status --porcelain)" ]; then
    echo "⚠️  Warning: You have uncommitted changes. Consider committing first."
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Deployment cancelled."
        exit 1
    fi
fi

echo "📡 Connecting to server and deploying..."

# SSH and deploy
ssh $SSH_ALIAS << 'EOF'
    set -e
    
    echo "📂 Checking if project directory exists..."
    if [ ! -d "/home/appuser/mcp-snowflake-server" ]; then
        echo "🆕 Creating project directory and cloning repository..."
        cd /home/appuser
        git clone https://github.com/RosTeHeA/mcp-snowflake-server.git
    fi
    
    echo "📂 Navigating to project directory..."
    cd /home/appuser/mcp-snowflake-server
    
    echo "🔄 Pulling latest changes from git..."
    git pull origin main
    
    echo "📦 Installing uv if needed..."
    if ! command -v uv &> /dev/null; then
        curl -LsSf https://astral.sh/uv/install.sh | sh
        export PATH="$HOME/.local/bin:$PATH"
    fi
    
    echo "📦 Installing/updating dependencies..."
    $HOME/.local/bin/uv sync
    
    echo "🧪 Testing installation..."
    $HOME/.local/bin/uv run start_http_server.py --help > /dev/null
    
    echo "✅ Server deployment complete!"
    echo "🚀 To start the HTTP server, run:"
    echo "   cd /home/appuser/mcp-snowflake-server"
    echo "   nohup $HOME/.local/bin/uv run start_http_server.py --connections-file my_connections.toml --connection-name production --host 0.0.0.0 --port 8000 > server.log 2>&1 &"
EOF

echo "🎉 Deployment completed successfully!"
echo ""
echo "Next steps:"
echo "1. Test locally: uv run mcp_snowflake_server --connections-file my_connections.toml --connection-name development"
echo "2. Configure Claude Desktop to use your custom server"
echo "3. Test the connection through Claude"

