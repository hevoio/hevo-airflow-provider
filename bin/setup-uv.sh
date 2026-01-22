#!/bin/bash

set -e

echo "=========================================="
echo "Hevo Airflow Provider - UV Setup"
echo "=========================================="
echo

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "❌ uv is not installed."
    echo
    echo "To install uv, run:"
    echo "  curl -LsSf https://astral.sh/uv/install.sh | sh"
    echo
    echo "Or using Homebrew:"
    echo "  brew install uv"
    echo
    exit 1
fi

echo "✅ uv is installed: $(uv --version)"
echo

# Get Python version
PYTHON_VERSION=${1:-3.9}
echo "📦 Setting up virtual environment with Python $PYTHON_VERSION..."
echo

# Create virtual environment with uv
uv venv --python $PYTHON_VERSION

echo
echo "✅ Virtual environment created"
echo

# Activate virtual environment message
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    ACTIVATE_CMD=".venv\\Scripts\\activate"
else
    ACTIVATE_CMD="source .venv/bin/activate"
fi

echo "To activate the virtual environment, run:"
echo "  $ACTIVATE_CMD"
echo

# Install dependencies
echo "📦 Installing dependencies with uv..."
echo

# Install the package in editable mode with all extras
uv pip install -e ".[dev]"

echo
echo "✅ Dependencies installed successfully"
echo

# Setup git hooks
if [ -f "bin/add-git-precommit-hook.sh" ]; then
    echo "📋 Setting up git hooks..."
    sh bin/add-git-precommit-hook.sh
    echo "✅ Git hooks configured"
    echo
fi

echo "=========================================="
echo "✅ Setup complete!"
echo "=========================================="
echo
echo "Next steps:"
echo "  1. Activate virtual environment: $ACTIVATE_CMD"
echo "  2. Run tests: uv run pytest"
echo "  3. Run linters: uv run ruff check src/"
echo
