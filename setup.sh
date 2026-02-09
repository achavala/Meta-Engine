#!/bin/bash
# ============================================================================
# META ENGINE — Setup Script
# ============================================================================
# Run this once to set up the Meta Engine environment.
# Usage: bash setup.sh
# ============================================================================

set -e

echo "🏛️  META ENGINE — Setup"
echo "========================"
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

META_DIR="$(cd "$(dirname "$0")" && pwd)"
echo "Meta Engine directory: $META_DIR"
echo ""

# ---- Step 1: Create .env if not exists ----
if [ ! -f "$META_DIR/.env" ]; then
    echo -e "${YELLOW}⚠️  No .env file found. Copying template...${NC}"
    cp "$META_DIR/.env.template" "$META_DIR/.env"
    echo -e "${GREEN}✅ .env created from template${NC}"
    echo -e "${YELLOW}   ➡️  IMPORTANT: Edit .env with your actual credentials!${NC}"
    echo ""
else
    echo -e "${GREEN}✅ .env already exists${NC}"
fi

# ---- Step 2: Check Python ----
echo ""
echo "Checking Python..."
PYTHON=$(which python3 || which python)
if [ -z "$PYTHON" ]; then
    echo -e "${RED}❌ Python not found. Install Python 3.9+${NC}"
    exit 1
fi
PY_VERSION=$($PYTHON --version)
echo -e "${GREEN}✅ $PY_VERSION${NC}"

# ---- Step 3: Create venv or use existing ----
echo ""
if [ -d "$META_DIR/venv" ]; then
    echo -e "${GREEN}✅ Virtual environment exists${NC}"
else
    echo "Creating virtual environment..."
    $PYTHON -m venv "$META_DIR/venv"
    echo -e "${GREEN}✅ Virtual environment created${NC}"
fi

# Activate venv
source "$META_DIR/venv/bin/activate"

# ---- Step 4: Install dependencies ----
echo ""
echo "Installing dependencies..."
pip install --quiet --upgrade pip
pip install --quiet -r "$META_DIR/requirements.txt"
echo -e "${GREEN}✅ Dependencies installed${NC}"

# ---- Step 5: Create output directories ----
mkdir -p "$META_DIR/output" "$META_DIR/logs"
echo -e "${GREEN}✅ Output directories created${NC}"

# ---- Step 6: Verify engine paths ----
echo ""
echo "Verifying engine paths..."

if [ -d "$HOME/PutsEngine" ]; then
    echo -e "${GREEN}✅ PutsEngine found at $HOME/PutsEngine${NC}"
else
    echo -e "${RED}❌ PutsEngine NOT found at $HOME/PutsEngine${NC}"
fi

if [ -d "$HOME/TradeNova" ]; then
    echo -e "${GREEN}✅ TradeNova found at $HOME/TradeNova${NC}"
else
    echo -e "${RED}❌ TradeNova NOT found at $HOME/TradeNova${NC}"
fi

# ---- Step 7: Run config check ----
echo ""
echo "Running configuration check..."
echo ""
$PYTHON "$META_DIR/run_meta_engine.py" --check

# ---- Step 8: Setup launchd (optional) ----
echo ""
echo "============================================"
echo "📅 OPTIONAL: Auto-Schedule Setup"
echo "============================================"
echo ""
echo "To auto-run at 9:35 AM ET every day:"
echo ""
echo "  1. Copy the launchd plist:"
echo "     cp '$META_DIR/com.metaengine.daily.plist' ~/Library/LaunchAgents/"
echo ""
echo "  2. Load it:"
echo "     launchctl load ~/Library/LaunchAgents/com.metaengine.daily.plist"
echo ""
echo "  3. Verify:"
echo "     launchctl list | grep metaengine"
echo ""
echo "  Or use the Python scheduler:"
echo "     python '$META_DIR/scheduler.py' start"
echo ""

echo "============================================"
echo -e "${GREEN}🏛️  META ENGINE — Setup Complete!${NC}"
echo "============================================"
echo ""
echo "Quick Start:"
echo "  cd '$META_DIR'"
echo "  source venv/bin/activate"
echo "  python run_meta_engine.py --check     # Verify config"
echo "  python run_meta_engine.py --force     # Run now (even weekends)"
echo "  python run_meta_engine.py --schedule  # Start scheduler"
echo ""
