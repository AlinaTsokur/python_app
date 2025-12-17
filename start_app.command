#!/bin/bash
cd "$(dirname "$0")"

echo "========================================"
echo "   VANTA APP LAUNCHER"
echo "========================================"

# Check/Create venv
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate venv
source venv/bin/activate

# Install requirements if they exist
if [ -f "requirements.txt" ]; then
    echo "Checking dependencies..."
    pip install -q -r requirements.txt
fi

echo "Starting application..."
echo "Your browser should open automatically."
echo "If not, visit: http://localhost:8501"
echo "========================================"

# Run Streamlit
# --server.headless false: Forces browser to open
# --server.runOnSave true: Auto-reload on save (useful)
streamlit run app.py --server.port 8501 --server.headless false

# Keep window open if app crashes
echo "Application stopped."
read -p "Press Enter to exit..."
