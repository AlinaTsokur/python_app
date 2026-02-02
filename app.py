import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from supabase import create_client, Client
import base64
import os
from core import diver_engine
from core import levels_engine
from core import parsing_engine
from core.parsing_engine import parse_value_raw, extract, fmt_num, parse_raw_input, calculate_metrics
from core.report_generator import generate_xray, generate_composite, generate_full_report, generate_composite_report
from ui.tabs import tab_reports
from ui.tabs import tab_candles
from ui.tabs import tab_flow
from ui.tabs import tab_diver
from ui.tabs import tab_levels
from ui.tabs import tab_lab
from ui.tabs import tab_training

# --- Настройка страницы ---
st.set_page_config(
    page_title="VANTA",
    page_icon="🖤",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- 🔌 Подключение к Supabase ---
@st.cache_resource
def init_connection():
    """Инициализация клиента Supabase с приоритетом env-переменных."""
    try:
        # 1. Приоритет: Environment Variables (Railway/Production)
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")

        # 2. Fallback: Streamlit Secrets (локальная разработка)
        if not url or not key:
            try:
                if "SUPABASE_URL" in st.secrets:
                    url = st.secrets["SUPABASE_URL"]
                    key = st.secrets["SUPABASE_KEY"]
            except Exception:
                pass

        if not url or not key:
            st.error("❌ Не найдены credentials! Установите SUPABASE_URL и SUPABASE_KEY.")
            st.stop()
            
        return create_client(url, key)
    except Exception as e:
        st.error(f"❌ Ошибка подключения: {e}")
        st.stop()

supabase: Client = init_connection()

# --- 🗄️ Менеджер БД ---
from core.db_manager import DatabaseManager
db = DatabaseManager(supabase)

# --- 🔄 Процессор конвейера ---
from core.pipeline_processor import PipelineProcessor

# --- 🎨 Стили CSS ---
import styles
styles.apply_styles(st)

# --- ⚙️ Загрузка конфигураций из БД ---
@st.cache_data(ttl=300)
def load_configurations():
    """Загружает коэффициенты активов, пороги DOI и параметры TF из Supabase."""
    config = {}
    try:
        # Коэффициенты активов
        res_ac = supabase.table('asset_coeffs').select("*").execute()
        config['asset_coeffs'] = {row['asset']: row['coeff'] for row in res_ac.data} if res_ac.data else {}

        # Пороги DOI
        res_porog = supabase.table('porog_doi').select("*").execute()
        if res_porog.data:
            df = pd.DataFrame(res_porog.data)
            if 'tf' in df.columns:
                df = df.rename(columns={'tf': 'timeframe'})
            config['porog_doi'] = df
        else:
            config['porog_doi'] = pd.DataFrame()

        # Параметры TF
        res_tf = supabase.table('tf_params').select("*").execute()
        config['tf_params'] = {row['tf']: row for row in res_tf.data} if res_tf.data else {}

        # Порог squeeze
        res_liq = supabase.table('liqshare_thresholds').select("*").eq('name', 'squeeze').execute()
        config['global_squeeze_limit'] = float(res_liq.data[0]['value']) if res_liq.data else 0.3

        return config
    except Exception as e:
        st.error(f"Ошибка загрузки конфигураций из БД: {e}")
        return {}

# Инициализация процессора
processor = PipelineProcessor(db, load_configurations)


# --- 🖥️ ИНТЕРФЕЙС ---
def get_base64_image(image_path):
    with open(image_path, "rb") as f:
        data = f.read()
    return base64.b64encode(data).decode()

logo_path = "assets/logo.png"
if os.path.exists(logo_path):
    img_b64 = get_base64_image(logo_path)
    st.markdown(
        f"""
        <div style="display: flex; align-items: center; gap: 10px;">
            <img src="data:image/png;base64,{img_b64}" style="height: 50px; width: auto;">
            <h1 style="margin: 0; padding: 0; line-height: 1.0;">VANTA</h1>
        </div>
        """, 
        unsafe_allow_html=True
    )
else:
    st.title("🖤 VANTA")

# --- 📍 НАВИГАЦИЯ ---
from core import batch_parser
from offline import stage1_loader, stage2_features, stage3_bins, stage4_rules, stage5_bins_stats, stage6_mine_stats

# Активные и временно отключённые вкладки
TABS = ["Отчеты", "Поток", "Свечи", "Дивер", "Уровни", "Лаборатория", "Обучение"]
DISABLED_TABS = ["Дивер", "Уровни", "Лаборатория", "Обучение"]

# Получаем текущую вкладку из URL
query_params = st.query_params
default_tab = TABS[0]

if "tab" in query_params:
    qp_tab = query_params["tab"]
    if qp_tab in TABS:
        default_tab = qp_tab

# Callback для обновления URL при смене вкладки
def on_tab_change():
    st.query_params["tab"] = st.session_state.nav_radio

selected_tab = st.radio(
    "Navigation", 
    TABS, 
    index=TABS.index(default_tab), 
    key="nav_radio", 
    label_visibility="collapsed",
    horizontal=True,
    on_change=on_tab_change
)

# --- РЕНДЕРИНГ ВКЛАДОК ---
if selected_tab in DISABLED_TABS:
    st.warning(f"🚧 Вкладка **{selected_tab}** временно отключена")
elif selected_tab == "Отчеты":
    tab_reports.render(db, processor)
elif selected_tab == "Поток":
    tab_flow.render()
elif selected_tab == "Свечи":
    tab_candles.render(db)
elif selected_tab == "Дивер":
    tab_diver.render(db, processor, load_configurations, supabase)
elif selected_tab == "Уровни":
    tab_levels.render(supabase)
elif selected_tab == "Лаборатория":
    tab_lab.render(supabase, load_configurations)
elif selected_tab == "Обучение":
    tab_training.render()

