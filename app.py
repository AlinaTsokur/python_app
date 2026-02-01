import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from supabase import create_client, Client
import base64
import os
import diver_engine
import levels_engine
import parsing_engine
from parsing_engine import parse_value_raw, extract, fmt_num, parse_raw_input, calculate_metrics
from core.report_generator import generate_xray, generate_composite, generate_full_report, generate_composite_report
from ui.tabs import tab_reports
from ui.tabs import tab_candles
from ui.tabs import tab_diver
from ui.tabs import tab_levels
from ui.tabs import tab_lab

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


# --- �️ ИНТЕРФЕЙС ---
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
import batch_parser
from offline import stage1_loader, stage2_features, stage3_bins, stage4_rules, stage5_bins_stats, stage6_mine_stats

TABS = ["Отчеты", "Свечи", "Дивер", "Уровни", "Лаборатория", "Обучение"]

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

# === ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===

def _display_found_rules(symbol, tf, exchange):
    """Отображает найденные паттерны в удобном виде."""
    import json
    from pathlib import Path
    
    clean_symbol = symbol.replace("/", "").replace(":", "")
    clean_tf = tf.replace("/", "")
    clean_ex = exchange.replace("/", "")
    
    filepath = Path(f"offline/data/{clean_symbol}_{clean_tf}_{clean_ex}_rules.json")
    if not filepath.exists():
        return
    
    with open(filepath, "r") as f:
        data = json.load(f)
    
    rules = data.get("rules", [])
    meta = data.get("meta", {})
    
    if not rules:
        st.info("Паттерны не найдены (edge threshold слишком высокий для малого N)")
        return
    
    st.divider()
    st.subheader("📊 Найденные паттерны")
    
    # Метаданные
    cols = st.columns(5)
    cols[0].metric(
        "N сетапов", 
        meta.get("N_setups", "?"),
        help="Количество исторических сетапов, использованных для обучения"
    )
    cols[1].metric(
        "Найдено правил", 
        meta.get("n_rules", len(rules)),
        help="Количество паттернов, прошедших фильтры"
    )
    cols[2].metric(
        "Min support", 
        meta.get("min_support_abs", "?"),
        help="Минимальное количество сетапов, где должен встретиться паттерн"
    )
    cols[3].metric(
        "Min edge", 
        f"{meta.get('min_edge_threshold', 0):.1%}",
        help="Минимальное преимущество над базовой вероятностью для включения правила"
    )
    cols[4].metric(
        "Base P(UP)", 
        f"{meta.get('base_P_UP', 0.5):.1%}",
        help="Базовая вероятность роста (% сетапов с y_dir=UP)"
    )
    
    # Таблица правил
    for i, rule in enumerate(rules):
        pattern_str = " → ".join(rule.get("pattern", []))
        direction = "🔻 DOWN" if rule.get("edge_down", 0) > rule.get("edge_up", 0) else "🔺 UP"
        edge = max(rule.get("edge_up", 0), rule.get("edge_down", 0))
        
        with st.expander(f"**Правило {i+1}** | {direction} | Edge: {edge:.1%} | Support: {rule.get('support', 0)}"):
            st.caption("🔍 **Паттерн (последовательность токенов):**")
            st.code(pattern_str, language=None)
            
            st.caption("📈 **Вероятности:**")
            col1, col2, col3 = st.columns(3)
            col1.metric("P(UP)", f"{rule.get('p_up_smooth', 0):.1%}", help="Сглаженная вероятность роста")
            col2.metric("P(DOWN)", f"{rule.get('p_down_smooth', 0):.1%}", help="Сглаженная вероятность падения")
            col3.metric("Wins", f"{rule.get('wins_up', 0)}/{rule.get('wins_down', 0)}", help="Побед UP / Побед DOWN")
            
            tti = rule.get("tti_probs", {})
            st.caption("⏱️ **ETA (время до импульса):**")
            eta_cols = st.columns(3)
            eta_cols[0].metric("NEAR", f"{tti.get('NEAR', 0):.0%}", help="Импульс через 0-1 свечу")
            eta_cols[1].metric("MID", f"{tti.get('MID', 0):.0%}", help="Импульс через 2-4 свечи")
            eta_cols[2].metric("EARLY", f"{tti.get('EARLY', 0):.0%}", help="Импульс через 5+ свечей")

if selected_tab == "Обучение":
    st.header("🏁 Центр Обучения Модели (V2.1)")
    
    col_cfg, col_stat = st.columns([1, 2])
    
    with col_cfg:
        st.subheader("1. Параметры")
        tr_symbol = st.selectbox("Тикер", ["ETH", "BTC", "SOL", "BNB"], index=0)
        tr_tf = st.selectbox("Таймфрейм", ["1D", "4h", "1h", "15m"], index=0)
        tr_exchange = st.text_input("Биржа", "Binance")
        tr_profile = st.selectbox("Профиль токенов", ["STRICT", "SMALLN"], index=1, 
                                   help="STRICT: полные бины (Q1-Q5), SMALLN: сжатые зоны (LOW/MID/HIGH)")
        
        start_btn = st.button("🚀 ЗАПУСТИТЬ ОБУЧЕНИЕ", type="primary", use_container_width=True)
        
    with col_stat:
        st.subheader("2. Прогресс")
        
        if start_btn:
            status = st.status("Запуск конвейера...", expanded=True)
            
            # PHASE 1: LOADING
            status.write("📥 Шаг 1: Загрузка данных (Offline Pooling)...")
            success1, msg1, count1 = stage1_loader.run_pipeline(tr_symbol, tr_tf, tr_exchange)
            
            if not success1:
                status.update(label="❌ Ошибка на этапе загрузки!", state="error")
                st.error(msg1)
            else:
                status.write(f"✅ Данные загружены: {msg1}")
                
                # PHASE 2: FEATURES
                status.write("🧠 Шаг 2: Генерация признаков (Simulation)...")
                try:
                    success2, msg2, count2 = stage2_features.run_simulation(tr_symbol, tr_tf, tr_exchange)
                    
                    if not success2:
                        status.update(label="❌ Ошибка генерации признаков!", state="error")
                        st.error(msg2)
                    else:
                         status.write(f"✅ Признаки созданы: {msg2}")
                         
                         # PHASE 3: BINNING
                         status.write("📊 Шаг 3: Построение bins (квантили)...")
                         try:
                             success3, msg3 = stage3_bins.run_binning(tr_symbol, tr_tf, tr_exchange)
                             
                             if not success3:
                                 status.update(label="❌ Ошибка построения bins!", state="error")
                                 st.error(msg3)
                             else:
                                 status.write(f"✅ Bins созданы: {msg3}")
                                 
                                 # PHASE 4: MINING RULES
                                 status.write("🔍 Шаг 4: Поиск паттернов (Mining)...")
                                 try:
                                     success4, msg4 = stage4_rules.run_mining(tr_symbol, tr_tf, tr_exchange, profile=tr_profile)
                                     
                                     if not success4:
                                         status.update(label="❌ Ошибка поиска паттернов!", state="error")
                                         st.error(msg4)
                                     else:
                                         status.write(f"✅ Паттерны найдены: {msg4}")
                                         
                                         # PHASE 5: STATS BINS
                                         status.write("📈 Шаг 5: STATS квантили...")
                                         try:
                                             success5, msg5 = stage5_bins_stats.run_bins_stats(tr_symbol, tr_tf, tr_exchange)
                                             if not success5:
                                                 status.update(label="❌ Ошибка STATS bins!", state="error")
                                                 st.error(msg5)
                                             else:
                                                 status.write(f"✅ STATS bins: {msg5}")
                                                 
                                                 # PHASE 6: STATS RULES
                                                 status.write("🔬 Шаг 6: STATS правила...")
                                                 try:
                                                     success6, msg6 = stage6_mine_stats.run_mine_stats(tr_symbol, tr_tf, tr_exchange)
                                                     if not success6:
                                                         status.update(label="❌ Ошибка STATS правил!", state="error")
                                                         st.error(msg6)
                                                     else:
                                                         status.write(f"✅ STATS правила: {msg6}")
                                                         status.update(label="🎉 Обучение завершено!", state="complete")
                                                         st.balloons()
                                                         _display_found_rules(tr_symbol, tr_tf, tr_exchange)
                                                 except Exception as e:
                                                     status.update(label="❌ Ошибка Stage 6", state="error")
                                                     st.error(str(e))
                                         except Exception as e:
                                             status.update(label="❌ Ошибка Stage 5", state="error")
                                             st.error(str(e))
                                         
                                 except Exception as e:
                                     status.update(label="❌ Критическая ошибка (Stage 4)", state="error")
                                     st.error(str(e))
                                 
                         except Exception as e:
                             status.update(label="❌ Критическая ошибка (Stage 3)", state="error")
                             st.error(str(e))
                         
                except Exception as e:
                     status.update(label="❌ Критическая ошибка (Stage 2)", state="error")
                     st.error(str(e))

if selected_tab == "Отчеты":
    tab_reports.render(db, processor)


if selected_tab == "Свечи":
    tab_candles.render(db)


if selected_tab == "Дивер":
    tab_diver.render(db, processor, load_configurations, supabase)


if selected_tab == "Уровни":
    tab_levels.render(supabase)


if selected_tab == "Лаборатория":
    tab_lab.render(supabase, load_configurations)
