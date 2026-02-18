#!/bin/bash
# Быстрый перезапуск Streamlit
# Использование: ./restart.sh

cd /Users/angrycat/.gemini/antigravity/scratch/my_app/python_app
pkill -f "streamlit" 2>/dev/null
sleep 1
source venv/bin/activate
streamlit run app.py --server.runOnSave true &
echo "✅ Streamlit перезапущен! Обнови страницу в браузере."
