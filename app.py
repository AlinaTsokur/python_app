import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from supabase import create_client, Client
import base64
import os
import diver_engine
import levels_engine
import parsing_engine
# Reloads removed for production cleanliness
from parsing_engine import parse_value_raw, extract, fmt_num, parse_raw_input, calculate_metrics
from core.report_generator import generate_xray, generate_composite, generate_full_report, generate_composite_report
from ui.tabs import tab_reports
from ui.tabs import tab_candles
from ui.tabs import tab_diver

# --- Настройка страницы ---
st.set_page_config(
    page_title="VANTA Black",
    page_icon="🖤",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- 🔌 Подключение к Supabase ---
@st.cache_resource
def init_connection():
    try:
        # 1. Try OS Environment Variables (Railway/Production) - Prioritize this!
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")

        # 2. Fallback to Streamlit Secrets (Local)
        if not url or not key:
            try:
                # Accessing st.secrets triggers file check, so we wrap it
                if "SUPABASE_URL" in st.secrets:
                    url = st.secrets["SUPABASE_URL"]
                    key = st.secrets["SUPABASE_KEY"]
            except Exception:
                pass # secrets.toml missing, that's fine if we have env vars or handle it below

        if not url or not key:
            st.error("❌ Credentials missing! Set SUPABASE_URL and SUPABASE_KEY in Environment Variables (Railway) or .streamlit/secrets.toml (Local).")
            # Don't stop immediately if you want to allow limited functionality, but for now strict:
            st.stop()
            
        return create_client(url, key)
    except Exception as e:
        st.error(f"❌ Connection Error: {e}")
        st.stop()

supabase: Client = init_connection()

# --- 🗄️ Database Manager Instance ---
from core.db_manager import DatabaseManager
db = DatabaseManager(supabase)

# --- 🔄 Pipeline Processor Instance ---
from core.pipeline_processor import PipelineProcessor
# Note: processor initialized after load_configurations is defined

# --- 🎨 CSS: PREMIUM DESIGN ---
import styles
styles.apply_styles(st)

# --- ⚙️ Загрузка конфигураций из БД ---
@st.cache_data(ttl=300)
def load_configurations():
    """Загружает параметры чувствительности, коэффициенты и пороги из Supabase."""
    config = {}
    try:
        # 1. Asset Coeffs (Column: asset, coeff)
        res_ac = supabase.table('asset_coeffs').select("*").execute()
        config['asset_coeffs'] = {row['asset']: row['coeff'] for row in res_ac.data} if res_ac.data else {}

        # 2. Porog DOI (Column: tf, btc, eth...)
        res_porog = supabase.table('porog_doi').select("*").execute()
        if res_porog.data:
            df = pd.DataFrame(res_porog.data)
            if 'tf' in df.columns:
                df = df.rename(columns={'tf': 'timeframe'})
            config['porog_doi'] = df
        else:
            config['porog_doi'] = pd.DataFrame()

        # 3. TF Params (Column: tf, k_set, k_ctr...)
        res_tf = supabase.table('tf_params').select("*").execute()
        config['tf_params'] = {row['tf']: row for row in res_tf.data} if res_tf.data else {}

        # 4. Liqshare Thresholds
        res_liq = supabase.table('liqshare_thresholds').select("*").eq('name', 'squeeze').execute()
        config['global_squeeze_limit'] = float(res_liq.data[0]['value']) if res_liq.data else 0.3

        return config
    except Exception as e:
        st.error(f"Ошибка загрузки конфигураций из БД: {e}")
        return {}

# --- 🔄 Pipeline Processor Instance ---
processor = PipelineProcessor(db, load_configurations)


# --- 🛠 Хелперы Парсинга ---
# MOVED TO parsing_engine.py
# (Imports added at top)

# --- 🧠 ЯДРО: 1. RAW INPUT PARSING (ИСПРАВЛЕНО) ---
# MOVED TO parsing_engine.py

# --- 🧠 ЯДРО: 2. CALCULATED METRICS ---
# MOVED TO parsing_engine.py

# --- 💾 БД ---
# MOVED TO core/db_manager.py (DatabaseManager class)


# --- HELPER: CENTRALIZED BATCH PROCESSING ---
# MOVED TO core/pipeline_processor.py (PipelineProcessor class)


# --- 🖥 UI ---
# --- HEADER ---
def get_base64_image(image_path):
    with open(image_path, "rb") as f:
        data = f.read()
    return base64.b64encode(data).decode()

logo_path = "assets/logo.png"
if os.path.exists(logo_path):
    img_b64 = get_base64_image(logo_path)
    # Flex container to align image and text. 
    # adjust height via max-height or height in css. vanta text is usually h1.
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

# --- NAVIGATION LOGIC ---
import batch_parser
from offline import stage1_loader, stage2_features, stage3_bins, stage4_rules, stage5_bins_stats, stage6_mine_stats


TABS = ["Отчеты", "Свечи", "Дивер", "Уровни", "Лаборатория", "Обучение"]

# 1. Get current tab from URL or Session State
query_params = st.query_params
default_tab = TABS[0]

# Check if 'tab' is in query params
if "tab" in query_params:
    qp_tab = query_params["tab"]
    if qp_tab in TABS:
        default_tab = qp_tab

# 2. Render Navigation (Radio as Tabs)
# Use a callback to update URL immediately on change
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

# === HELPER FUNCTIONS ===

def _display_found_rules(symbol, tf, exchange):
    """Display found rules in a nice summary table."""
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
    
    # Meta info with tooltips
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
    
    # Rules table
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


# ==============================================================================
# TAB 5: LEVELS (УРОВНИ)
# ==============================================================================
if selected_tab == "Уровни":
    # 1. Filters (Same as Diver)
    c1, c2, c3 = st.columns([1, 1.5, 3], gap="small")
    
    with c1:
        # TF Multiselect
        all_tfs = ["1h", "4h", "1d", "1w"]
        selected_tfs_lvl = st.multiselect(
            "TF", 
            all_tfs, 
            default=["4h", "1d"], 
            placeholder="TF", 
            label_visibility="collapsed",
            key="levels_tf_filter"
        )
        
    with c2:
        # Date Range
        date_range_lvl = st.date_input(
            "Период", 
            value=[], 
            label_visibility="collapsed",
            key="levels_date_filter"
        )
        
    with c3:
        if st.button("🚀 Рассчитать уровни", type="primary"):
            st.session_state['levels_results'] = {} # Clear stale
            st.session_state['pine_script_dynamic'] = ""
            with st.spinner("Считаем уровни..."):
                try:
                    if not selected_tfs_lvl:
                        st.error("⚠️ Выберите хотя бы один таймфрейм!")
                    else:
                        d_start, d_end = None, None
                        if len(date_range_lvl) == 2:
                             d_start, d_end = date_range_lvl
                        elif len(date_range_lvl) == 1:
                             d_start = date_range_lvl[0]
                        
                        # Data Collection
                        levels_results = {}
                        
                        for tf in selected_tfs_lvl:
                             # Build Query on unified 'candles' table
                             # Handle case-sensitivity (try both '4h' and '4H')
                             query = supabase.table("candles").select("*").in_("tf", [tf.lower(), tf.upper()]).order("ts", desc=True)
                             
                             if d_start:
                                 query = query.gte("ts", d_start.isoformat())
                             if d_end:
                                 # End date + 1 day to cover the full day
                                 d_end_full = d_end + timedelta(days=1)
                                 query = query.lt("ts", d_end_full.isoformat())
                             
                             # Apply limit if no range (Specific Bot Defaults)
                             if not d_start:
                                 if tf == "4h":
                                     limit_val = 180
                                 elif tf == "1d":
                                     limit_val = 365
                                 else:
                                     limit_val = 300
                                 query = query.limit(limit_val)
                             else:
                                 query = query.limit(1000) # Hard limit for range safety

                             res = query.execute()
                             candles = res.data[::-1] if res.data else []
                             
                             if candles:
                                 # Dynamic Max Levels: 1D -> 8, others -> 10
                                 mx = 8 if tf == "1d" else 10
                                 lvls = levels_engine.build_levels(candles, lookback=len(candles), max_levels=mx, timeframe=tf)
                                 # Separate H/L clustering already done inside
                                 
                                 levels_results[tf.upper()] = lvls
                        
                        st.session_state['levels_results'] = levels_results
                            
                except Exception as e:
                    st.error(f"Ошибка: {e}")

    # Results
    if st.session_state.get('levels_results'):
        st.divider()
        
        if not any(st.session_state['levels_results'].values()):
             st.warning("⚠️ Уровни не найдены. Попробуйте увеличить историю (Limit) или выбрать другой период.")

        # 1. Text Report (Copyable)
        st.subheader("📋 Отчет (Copyable)")
        
        report_lines = []
        for tf, lvls in st.session_state['levels_results'].items():
            if not lvls:
                line = f"**{tf} LEVELS:** (Нет уровней. Мало данных или низкая волатильность)"
            else:
                # Format: 2945.50 (x2)
                segments = [f"{l['mid']:.2f} (x{l['touches']})" for l in lvls]
                line = f"{tf} LEVELS: " + " / ".join(segments)
            report_lines.append(line)
            
        full_report = "\n\n".join(report_lines)
        st.code(full_report, language="markdown")
        

        # Details Expander (Hidden, Debug)
        with st.expander("🔍 Детали (отладка)", expanded=False):
            for tf, lvls in st.session_state['levels_results'].items():
                st.markdown(f"**{tf} Debug Data:**")
                if lvls:
                    st.dataframe(pd.DataFrame(lvls), use_container_width=True)
                else:
                    st.text("No levels found.")



if selected_tab == "Лаборатория":
    # Text Area
    lab_text = st.text_area("Batch Input", label_visibility="collapsed", height=300, key="lab_text_area", placeholder="Вставьте свечи и метки (Strong Up/Down)...")
    
    # Action Columns
    col_lab_parse, col_lab_save, col_lab_status = st.columns([1, 3, 7])
    
    with col_lab_parse:
        if st.button("🐾 ", type="primary"):
            if not lab_text.strip():
                st.warning("Введите текст.")
            else:
                # Load config to ensure calculate_metrics works fully
                lab_config = load_configurations()
                st.session_state['lab_segments'], st.session_state['lab_candles'], st.session_state['lab_warnings'] = batch_parser.parse_batch_with_labels(lab_text, config=lab_config)
                st.session_state['lab_checked'] = True
                st.rerun()

    # Results Display
    if st.session_state.get('lab_checked'):
        st.divider()
        warnings = st.session_state.get('lab_warnings', [])
        segments = st.session_state.get('lab_segments', [])
        candles = st.session_state.get('lab_candles', [])
        
        # 1. Warnings (Critical)
        if warnings:
            st.error(f"⚠️ ОБНАРУЖЕНО {len(warnings)} ПРОБЛЕМ")
            for w in warnings:
                st.markdown(f"- {w}")
            st.warning("Рекомендуем исправить текст перед загрузкой, иначе проблемные сегменты будут пропущены.")
        
        # 2. Stats
        st.write(f"**Найдено свечей:** {len(candles)}")
        
        # 3. Segments Table
        if segments:
            # Display Segments Table
            seg_data = []
            for i, s in enumerate(segments): # Changed parsed_batch to segments
                meta = s['META']
                stats = s['CONTEXT']['STATS']
                imp = s['IMPULSE']
                
                row = {
                    "Symbol": meta.get('symbol', 'Unknown'), # Changed raw_symbol to symbol
                    "TF": meta.get('tf', 'Unknown'),
                    "Direction": imp.get('y_dir'), # "UP" / "DOWN"
                    "Strength": imp.get('y_size'), # "Weak" / "Medium" / "Strong"
                    "Candles": stats.get('candles_count'),
                    "Vol (M)": f"{stats.get('sum_volume', 0)/1_000_000:.2f}M",
                    "Liq Ratio": stats.get('liq_dominance_ratio')
                }
                seg_data.append(row)
            
            # Display Table
            if seg_data:
                st.dataframe(pd.DataFrame(seg_data), use_container_width=True)
            
            # Save Button (Only if segments exist)
            with col_lab_save:
                # Transactional Save
                if st.button(f"💾 Загрузить {len(segments)} сегментов в БД", type="primary"):
                    with st.spinner("Тотальная запись (Транзакция)..."):
                        try:
                            s_count, c_count = batch_parser.save_batch_transactionally(supabase, segments, candles)
                            with col_lab_status:
                                st.success(f"✅ УСПЕХ! Записано: {s_count} сегментов, {c_count} свечей.")
                            st.balloons()
                            # Clear state
                            st.session_state['lab_checked'] = False
                            st.session_state['lab_segments'] = []
                        except Exception as e:
                            st.error(f"❌ ОШИБКА ЗАПИСИ: {e}")
                            st.error("Транзакция отменена. Данные откатились (Rollback). База чиста.")
        else:
            st.info("Валидных сегментов не найдено.")


