import streamlit as st
import re
import pandas as pd
import uuid
from datetime import datetime, time
from supabase import create_client, Client
import math
import base64
import os
import diver_engine
import levels_engine
import altair as alt
import parsing_engine 
# Reloads removed for production cleanliness
from parsing_engine import parse_value_raw, extract, fmt_num, parse_raw_input, calculate_metrics, generate_full_report

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
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
        return create_client(url, key)
    except Exception as e:
        st.error(f"❌ Ошибка подключения к Supabase: {e}")
        st.stop()

supabase: Client = init_connection()

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

# --- 🛠 Хелперы Парсинга ---
# MOVED TO parsing_engine.py
# (Imports added at top)

# --- 🧠 ЯДРО: 1. RAW INPUT PARSING (ИСПРАВЛЕНО) ---
# MOVED TO parsing_engine.py

# --- 🧠 ЯДРО: 2. CALCULATED METRICS ---
# MOVED TO parsing_engine.py

# --- 🔄 СЛИЯНИЕ С БД (Merge-on-Parse) ---
def fetch_and_merge_db(batch_data, config):
    """
    1. Ищет существующие свечи в БД по (exchange, symbol, tf, ts).
    2. Объединяет новые данные с существующими.
    """
    if not batch_data: return []
    
    # Helper to normalize key for reliable matching
    def get_merge_key(ex, sym, tf, ts):
        # Normalize TS: "2025-12-10T12:00:00" -> "2025-12-10 12:00"
        # Handles various ISO formats and timezone offsets by taking first 16 chars
        clean_ts = str(ts).replace('T', ' ')[:16]
        return (ex, sym, tf, clean_ts)

    # 1. Группировка для оптимизации запросов
    # Нужно запросить диапазоны времени для каждого тикера
    groups = {} # (ex, sym, tf) -> [ts_list]
    for row in batch_data:
        key = (row.get('exchange'), row.get('symbol_clean'), row.get('tf'))
        if key not in groups: groups[key] = []
        groups[key].append(row.get('ts'))
        
    db_map = {} # (ex, sym, tf, ts) -> db_row
    
    # 2. Batch Fetching
    try:
        for (ex, sym, tf), ts_list in groups.items():
            if not ts_list: continue
            min_ts = min(ts_list)
            max_ts = max(ts_list)
            
            # Запрос к БД: exchange + symbol + tf + диапазон времени
            res = supabase.table('candles')\
                .select("*")\
                .eq('exchange', ex)\
                .eq('symbol_clean', sym)\
                .eq('tf', tf)\
                .gte('ts', min_ts)\
                .lte('ts', max_ts)\
                .execute()
                
            if res.data:
                for db_row in res.data:
                     # Use normalized key
                    k = get_merge_key(db_row.get('exchange'), db_row.get('symbol_clean'), db_row.get('tf'), db_row.get('ts'))
                    db_map[k] = db_row
                    
    except Exception as e:
        st.error(f"Ошибка получения данных из БД для слияния: {e}")
        pass 

    # 3. Merging
    merged_batch = []
    for new_row in batch_data:
        # Use normalized key
        k = get_merge_key(new_row.get('exchange'), new_row.get('symbol_clean'), new_row.get('tf'), new_row.get('ts'))
        existing = db_map.get(k)
        
        if existing:
            # Стратегия слияния:
            combined = existing.copy()
            
            for key, val in new_row.items():
                # Обновляем, если в базе пусто
                existing_val = combined.get(key)
                is_existing_empty = (existing_val is None) or (isinstance(existing_val, (int, float)) and existing_val == 0)
                
                if is_existing_empty:
                    combined[key] = val
            
            merged_batch.append(combined)
        else:
            merged_batch.append(new_row)
            
    return merged_batch

# --- 💾 БД ---
def save_candles_batch(candles_data):
    if not candles_data: return True
    
    # Deep copy to allow modification during retries
    current_data = [c.copy() for c in candles_data]
    
    # Ensure note exists and remove ID to rely on composite key upsert
    for row in current_data:
        if 'note' not in row: row['note'] = ""
        # Remove 'id' to prevent "null value in column id" error during mixed batch upserts
        row.pop('id', None)
            
    # Attempt loop
    attempt = 0
    max_attempts = 20 # Enough for many missing metrics
    dropped_columns = []
    
    while attempt < max_attempts:
        try:
            # Upsert WITHOUT ignore_duplicates to allow UPDATES
            res = supabase.table('candles').upsert(
                current_data, 
                on_conflict='exchange,symbol_clean,tf,ts'
            ).execute()
            
            return True
        except Exception as e:
            err_str = str(e)
            # Detect column error (PGRST204)
            match = re.search(r"Could not find the '(\w+)' column", err_str)
            if match:
                bad_col = match.group(1)
                if bad_col not in dropped_columns:
                    dropped_columns.append(bad_col)
                    # Remove this column from all rows
                    for row in current_data:
                        row.pop(bad_col, None)
                else:
                     # Loop detected?
                     st.error(f"Зацикливание на колонке {bad_col}: {e}")
                     return False
                attempt += 1
            else:
                # Other error
                st.error(f"Ошибка сохранения в БД: {e}")
                return False
                
    st.error("Не удалось сохранить после нескольких попыток удаления лишних полей.")
    return False

def load_candles_db(limit=100, start_date=None, end_date=None, tfs=None):
    try:
        query = supabase.table('candles').select("*").order('ts', desc=True)
        
        if start_date:
            query = query.gte('ts', start_date.isoformat())
        if end_date:
            # End date inclusive (until end of day)
            end_dt = datetime.combine(end_date, time(23, 59, 59))
            query = query.lte('ts', end_dt.isoformat())
            
        if tfs and len(tfs) > 0:
            # Case-insensitive filter hack: add both cases
            tfs_extended = list(set(tfs + [t.upper() for t in tfs] + [t.lower() for t in tfs]))
            query = query.in_('tf', tfs_extended)
            
        res = query.limit(limit).execute()
        return pd.DataFrame(res.data) if res.data else pd.DataFrame()
    except Exception as e:
        st.error(f"Ошибка чтения БД: {e}")
        return pd.DataFrame()

def delete_candles_db(ids):
    try:
        supabase.table('candles').delete().in_('id', ids).execute()
        return True
    except Exception as e:
        st.error(f"Ошибка удаления: {e}")
        return False

def update_candle_db(id, changes):
    try:
        supabase.table('candles').update(changes).eq('id', id).execute()
        return True
    except Exception as e:
        st.error(f"Ошибка обновления: {e}")
        return False

# --- 📝 REPORTING ---
# MOVED fmt_num, generate_full_report TO parsing_engine.py
# (Imports at top)



# --- 📊 ЛОГИКА КОМПОЗИТА (COMPOSITE) ---
def generate_composite_report(candles_list):
    """
    Считает взвешенный по объему (Volume) отчет для группы свечей.
    """
    # Минимум 3 биржи для расчета
    if not candles_list or len(candles_list) < 3: return None

    # Пороги (как в Google Sheets)
    THRESH = {
        'CVD': 1.0, 'TR': 0.5, 'TILT': 2.0,
        'DOI': 0.5, 'LIQ_HIGH': 0.30, 'LIQ_LOW': 0.10
    }

    # Tracking missing data for report warning
    missing_data_report = {}

    def get_val(d, key):
        v = d.get(key)
        # Return None if not numeric number (None is preserved)
        if v is None: return None
        return v if (isinstance(v, (int, float)) and not math.isnan(v)) else None

    def sign_char(val, thr):
        if val is None: return '?'
        if abs(val) < thr: return '0'
        return '+' if val > 0 else '-'

    def dispersion(values, thr):
        valid_vals = [v for v in values if v is not None]
        signs = set()
        for v in valid_vals:
            if v > thr: signs.add(1)
            elif v < -thr: signs.add(-1)
        return "смешанный" if (1 in signs and -1 in signs) else "ок"

    # 2. Взвешенное среднее (Smart Weighting)
    def weighted(key, metric_name_for_report):
        valid_candles = []
        missing_exchanges = []
        
        for c in candles_list:
            if get_val(c, key) is not None:
                valid_candles.append(c)
            else:
                missing_exchanges.append(c.get('exchange', 'Unknown'))
        
        # Log missing exchanges if any found
        if missing_exchanges:
            missing_data_report[metric_name_for_report] = missing_exchanges

        if not valid_candles: return None
        
        subset_vol = sum(get_val(c, 'volume') for c in valid_candles)
        if subset_vol == 0: return None
        
        return sum(get_val(c, key) * get_val(c, 'volume') for c in valid_candles) / subset_vol

    # 3. Расчет метрик
    comp = {
        'cvd':  weighted('cvd_pct', 'CVD'),
        'tr':   weighted('dtrades_pct', 'Trades'),
        'tilt': weighted('tilt_pct', 'Tilt'),
        'doi':  weighted('doi_pct', 'Delta OI'),
        'liq':  weighted('liq_share_pct', 'Liquidation'),
        'clv':  weighted('clv_pct', 'CLV'),
        'upper': weighted('upper_tail_pct', 'Upper Tail'),
        'lower': weighted('lower_tail_pct', 'Lower Tail'),
        'body':  weighted('body_pct', 'Body')
    }

    # 4. Интерпретация (Safe Evaluation)
    def safe_fmt(val, dec=2):
        return f"{val:.{dec}f}%" if val is not None else "—"

    if comp['liq'] is not None:
        if comp['liq'] > THRESH['LIQ_HIGH']: liq_eval = 'ведут ликвидации'
        elif comp['liq'] <= THRESH['LIQ_LOW']: liq_eval = 'фон'
        else: liq_eval = 'умеренно'
    else: liq_eval = '—'

    if comp['tilt'] is not None:
        if comp['tilt'] >= THRESH['TILT']: tilt_int = 'sell тяжелее'
        elif comp['tilt'] <= -THRESH['TILT']: tilt_int = 'buy тяжелее'
        else: tilt_int = 'нейтр'
    else: tilt_int = '—'

    if comp['clv'] is not None:
        if comp['clv'] >= 70: clv_int = 'принятие сверху'
        elif comp['clv'] <= 30: clv_int = 'принятие снизу'
        else: clv_int = 'середина диапазона'
    else: clv_int = '—'

    # Liq Tilt Sums
    ll_vals = [get_val(c, 'liq_long') for c in candles_list]
    ls_vals = [get_val(c, 'liq_short') for c in candles_list]
    sum_ll = sum(v for v in ll_vals if v is not None)
    sum_ls = sum(v for v in ls_vals if v is not None)
    
    # Check if we have ANY valid liquidation data
    has_liq_data = any(v is not None for v in ll_vals) or any(v is not None for v in ls_vals)
    
    if has_liq_data:
        liq_tilt = 'Long доминируют' if sum_ll > sum_ls else ('Short доминируют' if sum_ls > sum_ll else 'сбалансировано')
    else:
        liq_tilt = '—'

    disp_cvd = dispersion([get_val(c, 'cvd_pct') for c in candles_list], THRESH['CVD'])
    disp_doi = dispersion([get_val(c, 'doi_pct') for c in candles_list], THRESH['DOI'])

    # Детализация по биржам
    def fmt_item(c, key, thr):
        val = get_val(c, key)
        if val is None: return f"{c.get('exchange','?')} —"
        sign = '(+)' if val > thr else ('(−)' if val < -thr else '(0)')
        return f"{c.get('exchange','?')} {val:.2f}% {sign}"

    per_cvd = "; ".join([fmt_item(c, 'cvd_pct', THRESH['CVD']) for c in candles_list])
    per_tr  = "; ".join([fmt_item(c, 'dtrades_pct', THRESH['TR']) for c in candles_list])
    per_doi = "; ".join([fmt_item(c, 'doi_pct', THRESH['DOI']) for c in candles_list])

    instr = candles_list[0].get('raw_symbol', 'Unknown')

    tf = candles_list[0].get('tf', '-')
    exchanges_str = ", ".join([c.get('exchange','?') for c in candles_list])

    report = f"""КОМПОЗИТНАЯ СВОДКА
• Инструмент/TF: {instr} / {tf} • Биржи: {len(candles_list)} ({exchanges_str})

1) CVD (дельта активного объёма):
   - Композит: {safe_fmt(comp['cvd'])} , знак: {sign_char(comp['cvd'], THRESH['CVD'])} [дисперсия: {disp_cvd}]
   - По биржам: {per_cvd}
2) Δ по числу сделок (Trades):
   - Композит: {safe_fmt(comp['tr'])} , знак: {sign_char(comp['tr'], THRESH['TR'])}
   - По биржам: {per_tr}
3) Перекос среднего размера сделки (Tilt, sell vs buy):
   - Композит: {safe_fmt(comp['tilt'])} , интерпретация: {tilt_int}
4) Ликвидации:
   - Доля: {safe_fmt(comp['liq'])} • Перекос: {liq_tilt} • Оценка: {liq_eval}
5) Open Interest:
   - Композит ΔOI%: {safe_fmt(comp['doi'])} , знак: {sign_char(comp['doi'], THRESH['DOI'])} [дисперсия: {disp_doi}]
   - По биржам: {per_doi}

6) Геометрия свечи:
   - CLV: {safe_fmt(comp['clv'])} ({clv_int})
   - Тени: верхняя {safe_fmt(comp['upper'])} / нижняя {safe_fmt(comp['lower'])}
   - Тело: {safe_fmt(comp['body'])}
"""
    # Warning Section Append
    if missing_data_report:
        report += "\n⚠️ ВНИМАНИЕ: Неполные данные\n"
        for metric, bad_exchanges in missing_data_report.items():
            report += f"• {metric}: {', '.join(bad_exchanges)} (исключен)\n"

    return report

# --- HELPER: CENTRALIZED BATCH PROCESSING (Refactored) ---
def process_raw_text_batch(raw_text):
    """
    Central function to process raw text input (Tab 1 & Tab 3).
    Performs:
    1. Splitting by Exchange
    2. Parsing (parse_raw_input)
    3. Timestamp filtering/forwarding
    4. DB Enrichment (fetch_and_merge_db)
    5. Metric Calculation
    6. X-RAY Generation
    7. Composite Analysis (Grouping & Validation)
    
    Returns:
        batch (list): List of processed candle dictionaries (with metrics and reports).
        orphan_errors (list): List of validation error strings.
    """
    config = load_configurations()
    if not config:
        return [], ["Configuration load failed"]

    # 1. Split & Clean (Synced with batch_parser.py)
    # Split by Timestamp (DD.MM.YYYY HH:MM)
    # Use robust regex from batch_parser to keep TS at start of chunk
    raw_chunks = re.split(r'(?m)^(?=\d{1,2}\.\d{1,2}\.\d{4}\s+\d{1,2}:\d{2})', raw_text)
    raw_chunks = [x.strip() for x in raw_chunks if x.strip()]
    
    merged_groups = {}
    orphan_errors = [] 

    # 2. Iterate & Parse
    for chunk in raw_chunks:
        # Standard parsing (Engine expects TS at start)
        base_data = parse_raw_input(chunk)
    
        # STRICT CHECK: If TS is missing -> Error
        if not base_data.get('ts'):
             # Create error similar to orphan logic
             err = f"• {base_data.get('exchange', 'Unknown')} {base_data.get('symbol_clean', 'Unknown')} -> CRITICAL: Missing Timestamp/Exchange"
             orphan_errors.append(err) 
             continue # Skip processing for this candle

        # 2d. Grouping for DB Merge
        key = (base_data.get('exchange'), base_data.get('symbol_clean'), base_data.get('tf'), base_data.get('ts'))
        
        if key not in merged_groups:
            merged_groups[key] = base_data
        else:
            existing = merged_groups[key]
            for k, v in base_data.items():
                if v and (k not in existing or not existing[k]):
                    existing[k] = v
    
    local_batch = list(merged_groups.values())
    
    # 3. DB Enrichment
    final_batch_list = fetch_and_merge_db(local_batch, config)
    
    # 4. Metric Calculation & X-RAY
    temp_all_candles = []
    for raw_data in final_batch_list:
        full_data = calculate_metrics(raw_data, config)
        
        has_main = raw_data.get('buy_volume', 0) != 0
        if has_main: 
            full_data['x_ray'] = generate_full_report(full_data)
        else: 
            full_data['x_ray'] = None
        
        temp_all_candles.append(full_data)

    # 5. Composite Analysis (Strict Mode)
    final_save_list = []
    orphan_errors = [] 
    
    def get_comp_key(r):
        ts = str(r.get('ts', '')).replace('T', ' ')[:16]
        sym = str(r.get('symbol_clean', '')).upper()
        tf = str(r.get('tf', '')).upper()
        return (ts, sym, tf)

    comp_groups = {}
    for row in temp_all_candles:
        grp_key = get_comp_key(row)
        if grp_key not in comp_groups: comp_groups[grp_key] = []
        comp_groups[grp_key].append(row)

    # Separate Valid vs Orphans
    valid_groups = []
    orphans_groups = []

    for key, group in comp_groups.items():
        has_binance = any(c['exchange'] == 'Binance' for c in group)
        if has_binance:
            valid_groups.append(group)
        else:
            orphans_groups.append(group)

    # If orphans exist -> BLOCKING ERROR
    if orphans_groups:
        for grp in orphans_groups:
            orphan = grp[0]
            
            # Try to find "Best Match" to explain why it failed
            best_match = None
            min_diff = 3
            
            o_ts = get_comp_key(orphan)[0]
            o_sym = get_comp_key(orphan)[1]
            o_tf = get_comp_key(orphan)[2]

            for v_grp in valid_groups:
                target = next((c for c in v_grp if c['exchange'] == 'Binance'), v_grp[0])
                t_ts = get_comp_key(target)[0]
                t_sym = get_comp_key(target)[1]
                t_tf = get_comp_key(target)[2]
                
                curr_diff = 0
                if o_ts != t_ts: curr_diff += 1
                if o_sym != t_sym: curr_diff += 1
                if o_tf != t_tf: curr_diff += 1
                
                if curr_diff < min_diff:
                    min_diff = curr_diff
                    best_match = target
            
            err_msg = f"• {orphan.get('exchange')} {o_sym} {o_ts}"
            if best_match:
                reasons = []
                bm_ts = get_comp_key(best_match)[0]
                bm_sym = get_comp_key(best_match)[1]
                bm_tf = get_comp_key(best_match)[2]

                if o_ts != bm_ts: reasons.append(f"Время ({o_ts} vs {bm_ts})")
                if o_sym != bm_sym: reasons.append(f"Тикер ({o_sym} vs {bm_sym})")
                if o_tf != bm_tf: reasons.append(f"ТФ ({o_tf} vs {bm_tf})")
                
                err_msg += f" -> Не совпало с Binance: {', '.join(reasons)}"
            else:
                err_msg += " -> Не найдено пары на Binance (проверьте все параметры)"
            
            orphan_errors.append(err_msg)
            
        # If orphans, we do NOT return valid list? 
        # Tab 1 logic: "st.session_state.processed_batch = []" if orphans exist.
        # We adhere to this strict logic.
        return [], orphan_errors
        
    else:
        # No orphans - process valid groups
        for group in valid_groups:
            target_candle = next((c for c in group if c['exchange'] == 'Binance'), None)
            if not target_candle: target_candle = group[0]

            if target_candle:
                unique_exchanges = set(r['exchange'] for r in group)
                if len(unique_exchanges) >= 3:
                    # COMPOSITE REPORT using ALL group members
                    # Tab 1 passed 'group' NOT 'members' (variable naming)
                    # And likely function expects list of candles.
                    # We need to check generate_composite_report signature.
                    # Previous code: generate_composite_report(group) - only 1 arg?
                    # Let's check.
                    # Assuming it takes list.
                    
                    # Wait, Tab 3 logic had different call?
                    # No, I implemented detailed valid logic from Tab 1.
                    
                    # We pass 'group' to generate_composite_report
                    comp_report = generate_composite_report(group)
                    target_candle['x_ray_composite'] = comp_report # Assign to Composite field
                
                final_save_list.append(target_candle)

    return final_save_list, []

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
import importlib
import batch_parser
importlib.reload(batch_parser) # Force reload to apply fixes immediately

# Dynamic Import of Offline modules
from offline import stage1_loader, stage2_features, stage3_bins, stage4_rules, stage5_bins_stats, stage6_mine_stats
importlib.reload(stage1_loader)
importlib.reload(stage2_features)
importlib.reload(stage3_bins)
importlib.reload(stage4_rules)
importlib.reload(stage5_bins_stats)
importlib.reload(stage6_mine_stats)

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

# ... (Previous Tabs Code) ...

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
    # TAB 1 CONTENT
    input_text = st.text_area("Вставьте данные свечи", height=150, label_visibility="collapsed", placeholder="Вставьте свечи здесь...")
    
    # Скрываем выбор даты/времени, берем текущие
    user_date = datetime.now().date()
    user_time = datetime.now().time()
    
    col_action, col_save, _ = st.columns([1, 4, 20], gap="small")
    
    with col_action:
        process = st.button("🐾", type="primary")

    # Save button will be rendered into col_save downstream (after processing)

    if process and input_text:
        # --- REFACTORED CALL ---
        final_save_list, orphan_errors = process_raw_text_batch(input_text)
        
        # Save to session (Validation Mode)
        st.session_state.processed_batch = final_save_list
        st.session_state.validation_errors = orphan_errors
        st.rerun()

    if 'validation_errors' in st.session_state and st.session_state.validation_errors:
        st.error("⛔️ ОШИБКА ВАЛИДАЦИИ КОМПОЗИТА")
        st.warning("Обнаружены данные других бирж, которые не совпали с Binance. Сохранение заблокировано.")
        for msg in st.session_state.validation_errors:
            st.code(msg, language="text")
            
    if 'processed_batch' in st.session_state and st.session_state.processed_batch:
        batch = st.session_state.processed_batch
        
        # Deferred Render: Save button in the top column
        # This ensures it captures the FRESH state after "Parse" is clicked
        with col_save:
             if st.button(f"💾 Сохранить {len(batch)}", type="secondary", key="save_btn_top"):
                if save_candles_batch(batch):
                    st.toast("Успешно сохранено!", icon="💾")
                    st.cache_data.clear()
        
        # Clear logic previously handled inside the big block, now we iterate
        for idx, full_data in enumerate(batch):
            # Prepare Label
            try:
                ts_obj = datetime.fromisoformat(full_data['ts'])
                ts_str = ts_obj.strftime('%d.%m.%Y %H:%M')
            except:
                ts_str = str(full_data.get('ts'))
            
            # Minimalist Header in Expander Label
            warn_icon = " ⚠️" if full_data.get('missing_fields') else ""
            label = f"{ts_str} · {full_data.get('exchange')} · {full_data.get('symbol_clean')} · {full_data.get('tf')} · O {fmt_num(full_data.get('open'))}{warn_icon}"            
            with st.expander(label):
                if full_data.get('missing_fields'):
                    st.warning(f"⚠️ Отсутствуют данные: {', '.join(full_data['missing_fields'])}.\nЗначения заменены на 0, чтобы расчеты не упали.")
                
                with st.container(height=300):
                    # === DYNAMIC TABS (Option 1) ===
                    if full_data.get('x_ray_composite'):
                        t_xray, t_comp = st.tabs(["X-RAY", "⚡️ COMPOSITE"])
                        with t_xray:
                             if full_data.get('x_ray'): st.code(full_data['x_ray'], language="yaml")
                        with t_comp:
                             st.code(full_data['x_ray_composite'], language="yaml")
                    else:
                        # Standard View
                        if full_data.get('x_ray'):
                             st.code(full_data['x_ray'], language="yaml")

if selected_tab == "Свечи":
    
    # 0. Filters Toolbar
    f1, f2, f3 = st.columns([2, 2, 1])
    
    with f1:
        # TF Multiselect
        all_tfs = ["1m", "5m", "15m", "1h", "4h", "1d", "1w"]
        selected_tfs = st.multiselect("Таймфреймы", all_tfs, default=[], placeholder="Все TF", label_visibility="collapsed")
        
    with f2:
        # Date Range Picker
        date_range = st.date_input("Период", value=[], label_visibility="collapsed")
        start_d, end_d = None, None
        if len(date_range) == 2:
            start_d, end_d = date_range
        elif len(date_range) == 1:
            start_d = date_range[0]
            
    with f3:
        limit_rows = st.number_input("Limit", value=100, min_value=1, step=50, label_visibility="collapsed")

    # 1. Load Data
    df = load_candles_db(limit=limit_rows, start_date=start_d, end_date=end_d, tfs=selected_tfs)

    if not df.empty:
        if 'note' not in df.columns: df['note'] = ""
        df.insert(0, "delete", False)
        # Convert TS
        df['ts'] = pd.to_datetime(df['ts'], errors='coerce')

        # 2. Controls Toolbar (Top)
        # 2. Controls Toolbar (Top)
        c1, c2, c3 = st.columns([0.2, 0.2, 0.6], vertical_alignment="bottom")
        
        # SAVE BUTTON
        with c1:
             if st.button("💾 Сохранить", key="btn_save_top", type="primary"):
                 if "db_editor" in st.session_state and "edited_rows" in st.session_state["db_editor"]:
                     changes_map = st.session_state["db_editor"]["edited_rows"]
                     if changes_map:
                         count = 0
                         for idx, changes in changes_map.items():
                             valid_changes = {k: v for k, v in changes.items() if k != 'delete'}
                             if valid_changes:
                                 row_id = df.iloc[idx]['id']
                                 update_candle_db(row_id, valid_changes)
                                 count += 1
                         if count > 0:
                             st.toast(f"✅ Обновлено {count} свечей")
                             st.cache_data.clear()
                             st.rerun()
                         else:
                             st.info("Нет смысловых изменений.")
                     else:
                         st.info("Нет изменений.")
        
        # DELETE BUTTON
        with c2:
            if st.button("🗑 Удалить выделенные", key="btn_del_top", type="secondary"):
                # Find rows where delete=True in session state
                ids_to_del = []
                
                # 1. Check "Select All" state directly
                if st.session_state.get("select_all_del_top"):
                    ids_to_del = df['id'].tolist()
                
                # 2. Check individual checkboxes from Data Editor
                elif "db_editor" in st.session_state and "edited_rows" in st.session_state["db_editor"]:
                    changes_map = st.session_state["db_editor"]["edited_rows"]
                    for idx, changes in changes_map.items():
                         if changes.get("delete") is True:
                             # Ensure idx is valid for current df
                             if idx < len(df):
                                 ids_to_del.append(df.iloc[idx]['id'])

                ids_to_del = list(set(ids_to_del))

                if ids_to_del:
                    if delete_candles_db(ids_to_del):
                        st.toast(f"Удалено {len(ids_to_del)} записей!")
                        st.cache_data.clear()
                        st.rerun()
                else:
                    st.warning("Ничего не выделено.")

        # SELECT ALL CHECKBOX
        with c3:
             if st.checkbox("Выделить все", key="select_all_del_top"):
                  df['delete'] = True

        visible_cols = ['ts', 'tf', 'x_ray', 'x_ray_composite', 'report_diver', 'note', 'raw_data']
        
        # 4. Data Editor
        edited_df = st.data_editor(
            df,
            key="db_editor",
            column_order=["delete"] + visible_cols,
            use_container_width=True,
            hide_index=True,
            height=800,
            column_config={
                "delete": st.column_config.CheckboxColumn("🗑", default=False, width=30),
                "ts": st.column_config.DatetimeColumn("Time", format="DD.MM.YYYY HH:mm", width="small"),
                "x_ray": st.column_config.TextColumn("X-RAY", width="small"),
                "x_ray_composite": st.column_config.TextColumn("Composite", width="small"),
                "report_diver": st.column_config.TextColumn("Diver", width="small"),
                "note": st.column_config.TextColumn("Note ✏️", width="small"),
                "raw_data": st.column_config.TextColumn("Raw", width="medium"),
            }
        )
        
    else:
        st.markdown(
            """
            <div style="
                background-color: rgba(100, 181, 246, 0.1); 
                color: #64B5F6;
                padding: 8px 16px; 
                border-radius: 8px; 
                width: fit-content;
                border: 1px solid rgba(100, 181, 246, 0.2);
                margin-bottom: 10px;
            ">
                ℹ️ База данных пуста.
            </div>
            """, 
            unsafe_allow_html=True
        )
if selected_tab == "Дивер":
    # 1. Mode Selection
    mode = st.radio("Источник данных", ["Выбрать из базы данных", "Ручной ввод"], horizontal=True, label_visibility="collapsed")
    
    selected_metrics = None
    
    if "Ручной" in mode:
        raw_text = st.text_area("Вставьте данные свечи", height=150, label_visibility="collapsed", placeholder="Вставьте свечи здесь...", key="manual_candle_input")
        
        # Paw Button
        c_paw, _ = st.columns([1, 10])
        with c_paw:
            if st.button("🐾", key="btn_manual_paw", type="primary"):
                if raw_text:
                    # --- REFACTORED CALL ---
                    # We reuse the same robust function used in Tab 1
                    try:
                        # Use current time as default, similar to Tab 1
                        final_save_list, orphan_errors = process_raw_text_batch(raw_text)
                        
                        if orphan_errors:
                            st.error("\n".join(orphan_errors))
                        
                        if final_save_list:
                            # In Manual Mode we usually expect 1 candle.
                            # We take the first valid Result (which might be a Composite or Single)
                            m = final_save_list[0]
                            st.session_state['manual_diver_candle'] = m
                            st.rerun()
                        elif not orphan_errors:
                            st.warning("Не удалось распознать данные. Проверьте формат.")
                            
                    except Exception as e:
                        st.error(f"Системная ошибка: {e}")
        
        # Display Manual Result (Persisted)
        if st.session_state.get('manual_diver_candle'):
            
            # Split Screen Logic
            # c_left takes 50%, c_right takes 50%
            c_left, c_right = st.columns([1, 1])
            
            # --- LEFT HALF: EXPANDER ---
            with c_left:
                m_data = st.session_state['manual_diver_candle']
                try:
                    ts_obj = datetime.fromisoformat(m_data.get('ts'))
                    ts_str = ts_obj.strftime('%d.%m.%Y %H:%M')
                except:
                    ts_str = str(m_data.get('ts', '')).replace('T', ' ')[:16]
                
                warn_icon = " ⚠️" if m_data.get('missing_fields') else ""
                label = f"{ts_str} · {m_data.get('exchange')} · {m_data.get('symbol_clean')} · {m_data.get('tf')} · O {fmt_num(m_data.get('open'))}{warn_icon}"
                
                with st.expander(label, expanded=False):
                    if m_data.get('missing_fields'):
                         st.warning(f"Не найдены поля: {', '.join(m_data['missing_fields'])}")
                         
                    # === DYNAMIC TABS (For Manual Mode) ===
                    if m_data.get('x_ray_composite'):
                        t_xray, t_comp = st.tabs(["X-RAY", "⚡️ COMPOSITE"])
                        with t_xray:
                             if m_data.get('x_ray'): st.code(m_data['x_ray'], language="yaml")
                        with t_comp:
                             st.code(m_data['x_ray_composite'], language="yaml")
                    else:
                        if m_data.get('x_ray'):
                             st.code(m_data['x_ray'], language="yaml")

            # --- RIGHT HALF: CONTROLS ---
            with c_right:
                mk_base = "manu_diver"
                
                # Align Zone, Action, Button on one line in this right half
                r1, r2, r3 = st.columns([2, 2, 1.5], gap="small")
                
                with r1:
                    m_zone = st.selectbox(
                        "📍 Зона", 
                        ["🌪 В воздухе", "🟢 Поддержка", "🔴 Сопротивление"],
                        key=f"zone_{mk_base}",
                        label_visibility="collapsed",
                        index=None,
                        placeholder="📍 Зона"
                    )
                # Check disable condition
                is_air_m = (m_zone == "🌪 В воздухе")
                
                with r2:
                    m_action = st.selectbox(
                        "⚡️ Действие", 
                        [
                            "🛡 Удержание",
                            "⚔️ Пробой",
                            "🎣 Л.Пробой",
                            "🪜 На границе",
                            "🕯 Тело на уровне"
                        ],
                        key=f"act_{mk_base}",
                        label_visibility="collapsed",
                        index=None,
                        placeholder="⚡️ Действие" if not is_air_m else "⛔️ Недоступно в воздухе",
                        disabled=is_air_m
                    )
                with r3:
                    if st.button("🔮 Анализ", key=f"btn_{mk_base}", type="primary", use_container_width=True):
                         # Mapping Logic (clean internal codes)
                        z_map = {
                            "🌪 В воздухе": "Air",
                            "🟢 Поддержка": "Support",
                            "🔴 Сопротивление": "Resistance"
                        }

                        
                        a_map = {
                            "🛡 Удержание": "AT_EDGE",
                            "⚔️ Пробой": "BREAK",
                            "🎣 Л.Пробой": "PROBE",
                            "🪜 На границе": "AT_EDGE_BORDERLINE",
                            "🕯 Тело на уровне": "AT_EDGE_TAIL"
                        }
                        
                        zone_code = z_map.get(m_zone)
                        action_code = a_map.get(m_action)
                        
                        # Validate (Action is optional if Zone is Air)
                        if not zone_code or (not action_code and zone_code != "Air"):
                            st.toast("⚠️ Выберите Зону и Действие!", icon="⚠️")
                        else:
                            # If Air, action_code might be None, logic handles it
                            report = diver_engine.run_expert_analysis(m_data, zone_code, action_code)
                            st.session_state['manual_diver_report'] = report
                            st.rerun()
            
            # --- BOTTOM: REPORT (Full Width) ---
            if st.session_state.get('manual_diver_report'):
                # Report takes the LEFT HALF width to match the expander width?
                # User said: "Left field with report..."
                # Wait: "Left field with report and right... place 3 other forms".
                # This implies the Report should also be in the Left Half?
                # Or maybe user meant the Expander IS the report.
                # "Left field with report [Expander?] and right... place 3 buttons".
                # Where does the RESULT go?
                # Usually results go below.
                # Let's put the result in the Left Half below the expander.
                
                with c_left:
                    st.code(st.session_state['manual_diver_report'], language="text")

    else: # DB Mode
        # Single Row for Filters + Selector
        # Ratio: TF (small), Dates (med), Selector (wide)
        c_tf, c_date, c_sel = st.columns([1, 1.5, 3], gap="small")
        
        with c_tf:
            all_tfs = ["1m", "5m", "15m", "1h", "4h", "1d", "1w"]
            filter_tfs = st.multiselect(
                "TF", 
                all_tfs, 
                default=[], 
                placeholder="TF", 
                label_visibility="collapsed",
                key="diver_db_tf_filter"
            )
            
        with c_date:
            filter_dates = st.date_input(
                "Период", 
                value=[], 
                label_visibility="collapsed",
                key="diver_db_date_filter"
            )
        
        # Parse Dates & Load
        d_start, d_end = None, None
        if len(filter_dates) == 2:
            d_start, d_end = filter_dates
        elif len(filter_dates) == 1:
             d_start = filter_dates[0]
             
        db_df = load_candles_db(limit=500, start_date=d_start, end_date=d_end, tfs=filter_tfs)
        

        selected_metrics = None
        
        with c_sel:
            if not db_df.empty:
                # Create label map
                options_map = {}
                for idx, row in db_df.iterrows():
                    try:
                        ts_str = str(row['ts']).replace('T', ' ')[:16]
                        label = f"{ts_str} | {row.get('symbol_clean')} | {row.get('tf')} | O: {row.get('open')}"
                        options_map[label] = row.to_dict()
                    except:
                        continue
                
                sel_label = st.selectbox(
                    "Выберите свечу", 
                    list(options_map.keys()),
                    index=None,
                    placeholder="Выберите свечу для анализа",
                    label_visibility="collapsed"
                )
                
                if sel_label:
                    # 1. Get raw DB data
                    raw_db_metrics = options_map[sel_label]
                    
                    # 2. Restore missing 'tf_sens' from Config (since DB column might be missing)
                    # Use lighter update if possible, but calculate_metrics is safest
                    config = load_configurations() 
                    selected_metrics = calculate_metrics(raw_db_metrics, config)
            else:
                st.markdown(
                    """
                    <div style="
                        background-color: rgba(100, 181, 246, 0.1); 
                        color: #64B5F6;
                        padding: 8px 12px; 
                        border-radius: 4px; 
                        width: fit-content;
                        font-size: 14px;
                        border: 1px solid rgba(100, 181, 246, 0.2);
                    ">
                        ℹ️ Нет данных
                    </div>
                    """, 
                    unsafe_allow_html=True
                )

        if selected_metrics:
            # COPY OF MANUAL INPUT LAYOUT
            m_data = selected_metrics
            
            # Split Screen Logic
            d_left, d_right = st.columns([1, 1])
            
            # --- LEFT HALF: EXPANDER + REPORT ---
            with d_left:
                try:
                    ts_obj = datetime.fromisoformat(str(m_data.get('ts')))
                    ts_str = ts_obj.strftime('%d.%m.%Y %H:%M')
                except:
                    ts_str = str(m_data.get('ts', '')).replace('T', ' ')[:16]
                
                # Check missing fields? DB usually has them or not.
                missing_f = m_data.get('missing_fields', [])
                warn_icon = " ⚠️" if missing_f else ""
                
                label = f"{ts_str} · {m_data.get('exchange')} · {m_data.get('symbol_clean')} · {m_data.get('tf')} · O {m_data.get('open')}{warn_icon}"
                
                with st.expander(label, expanded=False):
                    # Tabs logic
                    xray_val = m_data.get('x_ray')
                    comp_val = m_data.get('x_ray_composite')
                    
                    if comp_val:
                        t_xray, t_comp = st.tabs(["X-RAY", "⚡️ COMPOSITE"])
                        with t_xray:
                             if xray_val: st.code(xray_val, language="yaml")
                        with t_comp:
                             st.code(comp_val, language="yaml")
                    else:
                        if xray_val:
                             st.code(xray_val, language="yaml")
                             
                # Show Report below expander
                if st.session_state.get('db_diver_report'):
                    report_txt = st.session_state['db_diver_report']
                    st.code(report_txt, language="text")
                    
                    if st.button("💾 Сохранить отчет в БД", key="save_diver_db_btn"):
                        c_id = m_data.get('id')
                        if c_id:
                            try:
                                supabase.table('candles').update({
                                    'report_diver': report_txt
                                }).eq('id', c_id).execute()
                                st.toast("Отчет сохранен в БД! ✅", icon="✅")
                            except Exception as e:
                                st.error(f"Ошибка сохранения: {e}")
                        else:
                            st.warning("Не найден ID свечи для сохранения.")

            # --- RIGHT HALF: CONTROLS ---
            with d_right:
                mk_base = "db_diver"
                
                # New Layout: [Zone (1.2), Action (1.2), Analyze (0.7), ITB (0.7)]
                r1, r2, r3, r4 = st.columns([1.2, 1.2, 0.7, 0.7], gap="small")
                
                with r1:
                    d_zone = st.selectbox(
                        "📍 Зона", 
                        ["🌪 В воздухе", "🟢 Поддержка", "🔴 Сопротивление"],
                        key=f"zone_{mk_base}",
                        label_visibility="collapsed",
                        index=None,
                        placeholder="📍 Зона"
                    )
                # Check disable condition
                is_air_d = (d_zone == "🌪 В воздухе")
                
                with r2:
                    d_action = st.selectbox(
                        "⚡️ Действие", 
                        [
                            "🛡 Удержание",
                            "⚔️ Пробой",
                            "🎣 Л.Пробой",
                            "🪜 На границе",
                            "🕯 Тело на уровне"
                        ],
                        key=f"act_{mk_base}",
                        label_visibility="collapsed",
                        index=None,
                        placeholder="⚡️ Действие" if not is_air_d else "⛔️ Недоступно в воздухе",
                        disabled=is_air_d
                    )
                # Define Maps (Shared Scope)
                z_map = {
                    "🌪 В воздухе": "Air",
                    "🟢 Поддержка": "Support",
                    "🔴 Сопротивление": "Resistance"
                }
                
                a_map = {
                    "🛡 Удержание": "AT_EDGE",
                    "⚔️ Пробой": "BREAK",
                    "🎣 Л.Пробой": "PROBE",
                    "🪜 На границе": "AT_EDGE_BORDERLINE",
                    "🕯 Тело на уровне": "AT_EDGE_TAIL"
                }

                with r3:
                    if st.button("🔮 Анализ", key=f"btn_{mk_base}", type="primary", use_container_width=True):
                        
                        zone_code = z_map.get(d_zone)
                        action_code = a_map.get(d_action)
                        
                        # Validate (Action is optional if Zone is Air)
                        if not zone_code or (not action_code and zone_code != "Air"):
                            st.toast("⚠️ Выберите Зону и Действие!", icon="⚠️")
                        else:
                            report = diver_engine.run_expert_analysis(selected_metrics, zone_code, action_code)
                            st.session_state['db_diver_report'] = report
                            st.rerun()

                with r4:
                    if st.button("🛠 ИТБ", type="secondary", key="btn_toggle_itb", use_container_width=True):
                        st.session_state['show_itb_form'] = not st.session_state.get('show_itb_form', False)

                # --- ITB FORM RENDER ---
                if st.session_state.get('show_itb_form'):
                     itb_ph = f"Вставьте данные нарезки ({str(m_data.get('ts'))})..."
                     itb_text = st.text_area("Данные нарезки", height=200, key="itb_input_area", label_visibility="collapsed", placeholder=itb_ph)
                     
                     if st.button("🚀 Запустить ITB Анализ", type="primary", key="btn_run_itb_real"):
                            if not itb_text.strip():
                                st.error("Пустой ввод!")
                            else:
                                slices = []
                                config = load_configurations()
                                lines = itb_text.strip().split('\n')
                                is_valid = True
                                
                                for i, line in enumerate(lines):
                                    if not line.strip(): continue
                                    try:
                                        raw_s = parse_raw_input(line)
                                        met_s = calculate_metrics(raw_s, config)
                                        slices.append(met_s)
                                    except Exception as e:
                                        st.error(f"Ошибка в строке {i+1}: {e}")
                                        is_valid = False
                                        break
                                
                                if is_valid:
                                    try:
                                        # Inject Base Analysis
                                        z_code = z_map.get(d_zone)
                                        a_code = a_map.get(d_action)
                                        if z_code and (a_code or z_code == "Air"):
                                            base_cls, base_prob = diver_engine.get_base_analysis(m_data, z_code, a_code)
                                            m_data['cls'] = base_cls
                                            m_data['prob_final'] = base_prob
                                        
                                        res_itb = diver_engine.run_intrabar_analysis(m_data, slices)
                                        st.session_state['itb_result'] = res_itb
                                    except Exception as e:
                                        st.error(f"Ошибка движка ITB: {e}")
                
                # Show Result Persistent
                if st.session_state.get('itb_result'):
                    st.code(st.session_state['itb_result'], language="text")


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
                        candles_data = {} # Store for visualization
                        
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
                                 candles_data[tf.upper()] = candles # Store for Viz
                        
                        st.session_state['levels_results'] = levels_results
                        st.session_state['candles_data'] = candles_data
                            
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
        
        # 2. Visualization (Candles + Levels)
        st.subheader("📊 Визуализация (Chart)")
        
        # Tabs for Timeframes
        tf_list = list(st.session_state['levels_results'].keys())
        if tf_list:
            tabs = st.tabs(tf_list)
            
            for i, tf in enumerate(tf_list):
                with tabs[i]:
                    lvls = st.session_state['levels_results'].get(tf, [])
                    c_data = st.session_state.get('candles_data', {}).get(tf, [])
                    
                    if not c_data:
                        st.info("Нет данных свечей для графика.")
                        continue
                        
                    # Prepare DataFrames
                    df_c = pd.DataFrame(c_data)
                    # Ensure numeric and date
                    # Helper to map keys if needed, but Supabase returns dicts matching columns usually
                    # Assuming h, l, c, o, ts/time
                    # Let's clean up column names just in case using extract_val logic or simpler mapping
                    # Assuming standard keys exist
                    
                    # Normalize columns
                    def get_col(row, keys):
                        for k in keys:
                            if k in row: return row[k]
                        return 0
                        
                    df_c['Time'] = pd.to_datetime(df_c['ts']) if 'ts' in df_c.columns else pd.to_datetime(df_c['time'])
                    df_c['Open'] = df_c.apply(lambda x: get_col(x, ['o', 'open']), axis=1)
                    df_c['High'] = df_c.apply(lambda x: get_col(x, ['h', 'high']), axis=1)
                    df_c['Low'] = df_c.apply(lambda x: get_col(x, ['l', 'low']), axis=1)
                    df_c['Close'] = df_c.apply(lambda x: get_col(x, ['c', 'close']), axis=1)
                    
                    # Candle Layer
                    base = alt.Chart(df_c).encode(
                        x=alt.X('Time:T', title=None, axis=alt.Axis(format='%d %b %H:%M'))
                    )
                    
                    rule = base.mark_rule().encode(
                        y=alt.Y('Low:Q', title='Price (USDT)', scale=alt.Scale(zero=False)),
                        y2='High:Q',
                        color=alt.condition("datum.Open <= datum.Close", alt.value("#00C853"), alt.value("#D50000"))
                    )
                    
                    bar = base.mark_bar().encode(
                        y='Open:Q',
                        y2='Close:Q',
                        color=alt.condition("datum.Open <= datum.Close", alt.value("#00C853"), alt.value("#D50000")),
                        tooltip=['Time', 'Open', 'High', 'Low', 'Close']
                    )
                    
                    chart_candles = rule + bar
                    
                    # Levels Layer
                    if lvls:
                        df_req_l = []
                        for l in lvls:
                            df_req_l.append({
                                "Price": l['mid'],
                                "Type": "R" if l['kind'] == 'R' else "S",
                                "Touches": l['touches']
                            })
                        df_l = pd.DataFrame(df_req_l)
                        
                        # Use a dummy base for levels to allow full width rules?
                        # Altair rules without X encoding span the width.
                        # But we need to layer them over time axis.
                        # Actually, if we just use 'y' encoding on a separate data source, it should work as annotation lines.
                        
                        base_l = alt.Chart(df_l).encode(
                            y=alt.Y('Price:Q')
                        )
                        
                        lvl_rules = base_l.mark_rule().encode(
                            color=alt.Color('Type:N', scale=alt.Scale(domain=['S', 'R'], range=['green', 'red']), legend=None),
                            size=alt.Size('Touches:Q', scale=alt.Scale(range=[1, 3]), legend=None),
                            opacity=alt.value(0.7),
                            tooltip=['Type', 'Price', 'Touches']
                        )
                        
                        lvl_text = base_l.mark_text(align='left', dx=2, dy=-5).encode(
                            text=alt.Text('Price', format=".2f"),
                            color=alt.value('white') # Assuming dark mode
                        )
                        
                        final_chart = (chart_candles + lvl_rules + lvl_text).properties(
                            title=f"{tf} Chart with Levels",
                            width='container',
                            height=600
                        ).interactive()
                        
                        st.altair_chart(final_chart, use_container_width=True)
                    else:
                        st.altair_chart(chart_candles.properties(width='container', height=600).interactive(), use_container_width=True)

        
        # Details Expander (Hidden, Debug)
        with st.expander("hidden details (debug)"): 
             # ... existing debug view code if needed
             pass



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
