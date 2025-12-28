import streamlit as st
import re
import pandas as pd
import uuid
from datetime import datetime, time
from supabase import create_client, Client
import math
import base64
import os
import importlib
import diver_engine
import levels_engine
import altair as alt
importlib.reload(diver_engine) # Force reload to apply fixes
importlib.reload(levels_engine)

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
st.markdown("""
    <style>
        .stApp {
            background-color: #0e1117;
            background-image: 
                radial-gradient(at 0% 0%, rgba(45, 55, 72, 0.6) 0px, transparent 50%),
                radial-gradient(at 100% 0%, rgba(20, 30, 60, 0.6) 0px, transparent 50%),
                radial-gradient(at 100% 100%, rgba(45, 55, 72, 0.6) 0px, transparent 50%),
                radial-gradient(at 0% 100%, rgba(20, 30, 60, 0.6) 0px, transparent 50%);
            background-attachment: fixed;
            color: #E0E0E0;
        }
        
        /* Remove ALL Container Borders (Nuclear Option) */
        [data-testid="stVerticalBlockBorderWrapper"], [data-testid="stVerticalBlockBorderWrapper"] > div {
            border: none !important;
            box-shadow: none !important;
            background: transparent !important;
        }

        .tf-badge {
            background: linear-gradient(135deg, #ECEFF1, #B0BEC5);
            color: #263238; padding: 3px 10px; border-radius: 12px;
            font-size: 0.85em; font-weight: 700; margin-left: 8px;
            border: 1px solid rgba(255,255,255,0.4);
            box-shadow: 0 0 10px rgba(176, 190, 197, 0.3);
        }
        /* Clean Tabs */
        .stTabs [data-baseweb="tab-list"] {
            gap: 8px;
            border-bottom: 0px solid transparent !important;
        }
        .stTabs [data-baseweb="tab"] {
            height: 40px;
            white-space: pre-wrap;
            background-color: transparent !important;
            border: none !important;
            color: #E0E0E0;
        }
        .stTabs [aria-selected="true"] {
             background-color: transparent !important;
             border-bottom: 2px solid #FAFAFA !important;
             color: #FFFFFF !important;
        }
        /* Remove the default grey line */
        .stTabs [data-baseweb="tab-border"] {
             display: none !important;
        }
        /* Remove Code Block frames */
        [data-testid="stCodeBlock"] {
            border: none !important;
            box-shadow: none !important;
        }
        [data-testid="stCodeBlock"] > div {
             border: none !important;
             background-color: transparent !important;
        }
        /* Clean Tab Panel */
        [data-baseweb="tab-panel"] {
             padding-top: 10px !important;
        }
        /* Make header transparent */
        header[data-testid="stHeader"] {
            background: transparent !important;
        }

        /* --- NAVIGATION TABS (Radio) --- */
        [data-testid="stRadio"] > div {
            flex-direction: row;
            gap: 20px; /* Space between textual tabs */
            background: transparent !important;
            padding: 0px;
            display: inline-flex;
            border-bottom: 0px solid rgba(255,255,255,0.1);
        }
        [data-testid="stRadio"] label {
            background: transparent !important;
            padding: 5px 0px; /* Minimal padding */
            color: #90A4AE; /* Muted text */
            font-weight: 500;
            transition: all 0.2s;
            margin-right: 0 !important;
            border: none;
            cursor: pointer;
            border-radius: 0px;
            border-bottom: 2px solid transparent; /* Prepare for underline */
        }
        /* Selected State */
        [data-testid="stRadio"] label[data-checked="true"] {
             color: #FFFFFF !important;
             font-weight: 600;
             border-bottom: 2px solid #FFFFFF !important; /* Simple underline */
             box-shadow: none !important;
        }
        /* Hover State */
        [data-testid="stRadio"] label:hover {
             color: #FFFFFF;
        }
        
    </style>
""", unsafe_allow_html=True)

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
        return None

# --- 🛠 Хелперы Парсинга ---
def parse_value_raw(val_str):
    """Парсит строки с K, M, B, %, запятыми в float."""
    if not val_str or val_str == '-' or val_str == '': return 0.0
    
    clean_str = str(val_str).replace(',', '').replace('%', '').strip()
    multiplier = 1.0
    
    if clean_str.upper().endswith('K'):
        multiplier = 1_000.0
        clean_str = clean_str[:-1]
    elif clean_str.upper().endswith('M'):
        multiplier = 1_000_000.0
        clean_str = clean_str[:-1]
    elif clean_str.upper().endswith('B'):
        multiplier = 1_000_000_000.0
        clean_str = clean_str[:-1]
        
    try:
        clean_str = re.sub(r'[^\d.-]', '', clean_str)
        return round(float(clean_str) * multiplier, 2)
    except:
        return 0.0

def extract(regex, text):
    # Добавили DOTALL, чтобы искать по всему тексту даже с переносами строк
    match = re.search(regex, text, re.IGNORECASE | re.DOTALL)
    if match: return parse_value_raw(match.group(1))
    return None

# --- 🧠 ЯДРО: 1. RAW INPUT PARSING (ИСПРАВЛЕНО) ---
def parse_raw_input(text):
    """Парсит сырой текст в словарь Raw Input согласно спецификации."""
    data = {}
    data['raw_data'] = text.strip()
    
    # Флаги для надежности: Игнор регистра и Точка=все символы (вкл перенос строки)
    REGEX_FLAGS = re.IGNORECASE | re.DOTALL

    # Метаданные
    header_match = re.search(r'(.+?) · (.+?) · (\w+)', text)
    data['exchange'] = header_match.group(1).strip() if header_match else 'Unknown'
    data['raw_symbol'] = header_match.group(2).strip() if header_match else 'Unknown'
    data['tf'] = header_match.group(3).strip() if header_match else '4h'
    
    # Очистка тикера
    data['symbol_clean'] = data['raw_symbol'].split(' ')[0].replace('USDT', '').replace('PERP', '')
    
    # Поиск явного таймстемпа в тексте (dd.mm.yyyy HH:MM:SS или HH:MM)
    ts_match = re.search(r'(\d{1,2}\.\d{1,2}\.\d{4}\s+\d{1,2}:\d{2}(?::\d{2})?)', text)
    if ts_match:
        ts_str = ts_match.group(1)
        try:
            # Try with seconds
            dt_obj = datetime.strptime(ts_str, "%d.%m.%Y %H:%M:%S")
            data['ts'] = dt_obj.isoformat()
            data['parsed_ts'] = data['ts'] 
        except ValueError:
            try:
                # Try without seconds
                dt_obj = datetime.strptime(ts_str, "%d.%m.%Y %H:%M")
                data['ts'] = dt_obj.isoformat()
                data['parsed_ts'] = data['ts'] 
            except:
                 raise ValueError("Неверный формат даты/времени. Ожидается DD.MM.YYYY HH:MM")
    else:
        # User requested NO silent default. 
        # But we must check if 'pending_ts' logic in caller handles this?
        # The caller (process_raw_text_batch) relies on data['ts'] or data['parsed_ts'].
        # If we return without TS, the caller might fill it from pending_ts.
        # So we should strictly NOT set 'ts' here if not found, to let caller decide or fail.
        # However, user said "let warning pop up that they are mandatory".
        # So we leave it empty here?
        pass

    # OHLC
    ohlc_match = re.search(r'O\s+([\d,.]+)\s+H\s+([\d,.]+)\s+L\s+([\d,.]+)\s+C\s+([\d,.]+)', text)
    if ohlc_match:
        data['open'] = parse_value_raw(ohlc_match.group(1))
        data['high'] = parse_value_raw(ohlc_match.group(2))
        data['low'] = parse_value_raw(ohlc_match.group(3))
        data['close'] = parse_value_raw(ohlc_match.group(4))
    else:
        # Заглушки, чтобы не ломать вычисления
        data['open'] = data['high'] = data['low'] = data['close'] = 0.0
    
    # Volume & Change
    # Volume
    data['volume'] = extract(r'V ([\d,.]+[MKB]?)', text)
    
    # Change & Amplitude (Compound parsing)
    # Ex: Change -3.81(-0.12%) Amplitude 29.72(0.92%)
    ch_match = re.search(r'Change\s+([+\-]?[\d,.]+)\s*\(([+\-]?[\d,.]+)%\)', text, REGEX_FLAGS)
    if ch_match:
        data['change_abs'] = parse_value_raw(ch_match.group(1))
        data['change_pct'] = parse_value_raw(ch_match.group(2))
    else:
        # Fallback if distinct
        data['change_abs'] = extract(r'Change\s+([+\-]?[\d,.]+)', text)
        data['change_pct'] = extract(r'Change.*?([+\-]?[\d,.]+)%', text)

    amp_match = re.search(r'Amplitude\s+([\d,.]+)\s*\(([\d,.]+)%\)', text, REGEX_FLAGS)
    if amp_match:
        data['amplitude_abs'] = parse_value_raw(amp_match.group(1))
        data['amplitude_pct'] = parse_value_raw(amp_match.group(2))
    else:
        data['amplitude_abs'] = extract(r'Amplitude\s+([\d,.]+)', text)
        data['amplitude_pct'] = extract(r'Amplitude.*?([\d,.]+)%', text)
    
    # Active Volume
    data['buy_volume'] = extract(r'Active Buy/Sell Volume.*?Buy\s+([+\-]?[\d,.]+[MKB]?)', text)
    data['sell_volume'] = extract(r'Active Buy/Sell Volume.*?Sell\s+([+\-]?[\d,.]+[MKB]?)', text)
    if data['sell_volume'] is not None: data['sell_volume'] = abs(data['sell_volume'])
    data['abv_delta'] = extract(r'Active Buy/Sell Volume.*?Delta\s+([+\-]?[\d,.]+[MKB]?)', text)
    data['abv_ratio'] = extract(r'Active Buy/Sell Volume.*?Ratio\s+([+\-]?[\d,.]+)', text)
    
    # Trades
    data['buy_trades'] = extract(r'Active Buy/Sell Trades.*?Buy ([+\-]?[\d,.]+[MKB]?)', text)
    data['sell_trades'] = extract(r'Active Buy/Sell Trades.*?Sell ([+\-]?[\d,.]+[MKB]?)', text)
    if data['sell_trades'] is not None: data['sell_trades'] = abs(data['sell_trades'])
    data['trades_delta'] = extract(r'Active Buy/Sell Trades.*?Delta ([+\-]?[\d,.]+[MKB]?)', text)
    data['trades_ratio'] = extract(r'Active Buy/Sell Trades.*?Ratio ([+\-]?[\d,.]+)', text)

    # Open Interest
    oi_match = re.search(r'Open Interest.*?O ([\d,.]+[MKB]?) H ([\d,.]+[MKB]?) L ([\d,.]+[MKB]?) C ([\d,.]+[MKB]?)', text, REGEX_FLAGS)
    if oi_match:
        data['oi_open'] = parse_value_raw(oi_match.group(1))
        data['oi_high'] = parse_value_raw(oi_match.group(2))
        data['oi_low'] = parse_value_raw(oi_match.group(3))
        data['oi_close'] = parse_value_raw(oi_match.group(4))

    # Liquidations
    data['liq_long'] = extract(r'Liquidation Long ([\d,.]+[MKB]?)', text)
    data['liq_short'] = extract(r'Liquidation.*?Short ([+\-]?[\d,.]+[MKB]?)', text)
    if data['liq_short'] is not None: data['liq_short'] = abs(data['liq_short'])

    # --- COINGLASS FIELDS PARSING (ИСПРАВЛЕНО С REGEX FLAGS) ---
    
    # Funding Rate (exclude Aggregated)
    fr_match = re.search(r'(?<!Aggregated )Funding Rate.*?O ([+\-]?[\d,.]+%?).*?H ([+\-]?[\d,.]+%?).*?L ([+\-]?[\d,.]+%?).*?C ([+\-]?[\d,.]+%?)', text, REGEX_FLAGS)
    if fr_match:
        data['fr_open'] = parse_value_raw(fr_match.group(1))
        data['fr_high'] = parse_value_raw(fr_match.group(2))
        data['fr_low'] = parse_value_raw(fr_match.group(3))
        data['fr_close'] = parse_value_raw(fr_match.group(4))
    
    # Aggregated Funding Rate
    agg_fr_match = re.search(r'Aggregated Funding Rate.*?O ([+\-]?[\d,.]+%?).*?H ([+\-]?[\d,.]+%?).*?L ([+\-]?[\d,.]+%?).*?C ([+\-]?[\d,.]+%?)', text, REGEX_FLAGS)
    if agg_fr_match:
        data['agg_fr_open'] = parse_value_raw(agg_fr_match.group(1))
        data['agg_fr_high'] = parse_value_raw(agg_fr_match.group(2))
        data['agg_fr_low'] = parse_value_raw(agg_fr_match.group(3))
        data['agg_fr_close'] = parse_value_raw(agg_fr_match.group(4))

    # Basis
    data['basis'] = extract(r'Basis\s+([+\-]?[\d,.]+)', text)

    # Long/Short Ratio
    ls_match = re.search(r'Long/Short Ratio.*?O ([+\-]?[\d,.]+).*?H ([+\-]?[\d,.]+).*?L ([+\-]?[\d,.]+).*?C ([+\-]?[\d,.]+)', text, REGEX_FLAGS)
    if ls_match:
        data['ls_ratio_open'] = parse_value_raw(ls_match.group(1))
        data['ls_ratio_high'] = parse_value_raw(ls_match.group(2))
        data['ls_ratio_low'] = parse_value_raw(ls_match.group(3))
        data['ls_ratio_close'] = parse_value_raw(ls_match.group(4))

    # Index Price
    idx_match = re.search(r'Index Price.*?O ([\d,.]+).*?H ([\d,.]+).*?L ([\d,.]+).*?C ([\d,.]+)', text, REGEX_FLAGS)
    if idx_match:
        data['idx_open'] = parse_value_raw(idx_match.group(1))
        data['idx_high'] = parse_value_raw(idx_match.group(2))
        data['idx_low'] = parse_value_raw(idx_match.group(3))
        data['idx_close'] = parse_value_raw(idx_match.group(4))

    # Net Longs (Используем .*? для надежности между числами)
    nl_match = re.search(r'Net Longs.*?O ([+\-]?[\d,.]+[MKB]?).*?C ([+\-]?[\d,.]+[MKB]?).*?(?:Delta|Δ) ([+\-]?[\d,.]+[MKB]?)', text, REGEX_FLAGS)
    if nl_match:
        data['net_longs_open'] = parse_value_raw(nl_match.group(1))
        data['net_longs_close'] = parse_value_raw(nl_match.group(2))
        data['net_longs_delta'] = parse_value_raw(nl_match.group(3))

    # Net Shorts
    ns_match = re.search(r'Net Shorts.*?O ([+\-]?[\d,.]+[MKB]?).*?C ([+\-]?[\d,.]+[MKB]?).*?(?:Delta|Δ) ([+\-]?[\d,.]+[MKB]?)', text, REGEX_FLAGS)
    if ns_match:
        data['net_shorts_open'] = parse_value_raw(ns_match.group(1))
        data['net_shorts_close'] = parse_value_raw(ns_match.group(2))
        data['net_shorts_delta'] = parse_value_raw(ns_match.group(3))
    
    
    # Check for missing critical fields    
    critical_fields = [
        'ts', 'exchange', 'raw_symbol', 'symbol_clean', 'tf', 
        'open', 'high', 'low', 'close', 'volume', 
        'change_abs', 'change_pct', 'amplitude_abs', 'amplitude_pct', 
        'buy_volume', 'sell_volume', 'abv_delta', 'abv_ratio', 
        'buy_trades', 'sell_trades', 'trades_delta', 'trades_ratio', 
        'oi_open', 'oi_high', 'oi_low', 'oi_close', 
        'liq_long', 'liq_short'
    ]
    missing = [f for f in critical_fields if data.get(f) is None]
    if missing:
        data['missing_fields'] = missing

    return data

# --- 🧠 ЯДРО: 2. CALCULATED METRICS ---
def calculate_metrics(raw_data, config):
    """Считает метрики на основе Raw Input и конфигов из БД."""
    m = raw_data.copy()
    
    # 1. Geometry
    m['range'] = m.get('high', 0) - m.get('low', 0)
    m['range_pct'] = (m['range'] / m['close'] * 100) if m.get('close') else 0
    
    # Body and CLV
    rng = m['range']
    o_px = m.get('open', 0)
    c_px = m.get('close', 0)
    h_px = m.get('high', 0)
    l_px = m.get('low', 0)
    
    if rng > 0:
        m['body_pct'] = (abs(c_px - o_px) / rng * 100)
        m['clv_pct'] = ((c_px - l_px) / rng * 100)
        m['upper_tail_pct'] = ((h_px - max(o_px, c_px)) / rng * 100)
        m['lower_tail_pct'] = ((min(o_px, c_px) - l_px) / rng * 100)
    else:
        m['body_pct'] = 0
        m['clv_pct'] = 50.0
        m['upper_tail_pct'] = 0
        m['lower_tail_pct'] = 0

    m['price_sign'] = 1 if m.get('close', 0) >= m.get('open', 0) else -1

    # 2. Volume & Trades Metrics
    total_active_vol = (m.get('buy_volume') or 0) + (m.get('sell_volume') or 0)
    
    # CVD defaults to None if no data, else calculates
    if m.get('abv_delta') is not None and total_active_vol > 0:
        m['cvd_pct'] = (m.get('abv_delta') / total_active_vol * 100)
    else:
        m['cvd_pct'] = None
    m['cvd_sign'] = 1 if m.get('abv_delta', 0) > 0 else -1
    m['cvd_small'] = abs(m['cvd_pct']) < 1.0 

    # Trades: Propagate None
    b_trades = m.get('buy_trades')
    s_trades = m.get('sell_trades')
    
    if b_trades is not None and s_trades is not None:
        # Recalculate precision delta instead of parsed partial
        m['trades_delta'] = b_trades - s_trades 
        total_trades = b_trades + s_trades
        m['dtrades_pct'] = (m['trades_delta'] / total_trades * 100) if total_trades else 0
    else:
        total_trades = None
        m['dtrades_pct'] = None
    
    sign_abv = (m.get('abv_delta', 0) > 0) - (m.get('abv_delta', 0) < 0)
    sign_trades = (m.get('trades_delta', 0) > 0) - (m.get('trades_delta', 0) < 0)
    m['ratio_stable'] = (sign_abv == sign_trades)

    m['avg_trade_buy'] = (m.get('buy_volume') / b_trades) if (m.get('buy_volume') is not None and b_trades) else None
    m['avg_trade_sell'] = (m.get('sell_volume') / s_trades) if (m.get('sell_volume') is not None and s_trades) else None
    
    if m.get('avg_trade_buy') and m.get('avg_trade_sell'):
        m['tilt_pct'] = ((m['avg_trade_sell'] / m['avg_trade_buy']) - 1) * 100
    else:
        m['tilt_pct'] = None

    m['implied_price'] = (m.get('volume', 0) / total_active_vol) if total_active_vol else 0
    m['dpx'] = m['price_sign'] * m['cvd_sign'] 
    
    if m['dpx'] == 1: m['price_vs_delta'] = "match"
    elif m['dpx'] == -1: m['price_vs_delta'] = "div"
    else: m['price_vs_delta'] = "neutral"

    # 3. Open Interest Calculations
    if m.get('oi_open') and m.get('oi_close') is not None:
         m['doi_pct'] = ((m.get('oi_close') - m.get('oi_open')) / m.get('oi_open') * 100)
    else:
         m['doi_pct'] = None
    
    oi_rng = m.get('oi_high', 0) - m.get('oi_low', 0)
    if oi_rng == 0: m['oipos'] = 0.5
    else:
        raw_pos = (m.get('oi_close', 0) - m.get('oi_low', 0)) / oi_rng
        m['oipos'] = max(0.0, min(1.0, raw_pos))

    # OI Path & OE (Restored & Safe)
    oh = m.get('oi_high')
    ol = m.get('oi_low')
    oo = m.get('oi_open')
    
    if oh is not None and ol is not None and oo is not None:
        up_move = abs(oh - oo)
        dn_move = abs(ol - oo)
        if up_move > dn_move: m['oi_path'] = "up"
        elif dn_move > up_move: m['oi_path'] = "down"
        else: m['oi_path'] = "neutral"
    else:
        m['oi_path'] = None

    c_pct = m.get('change_pct')
    # If change_pct came as 0.0 (from text parsing) but we have absolute change, recalculate precision
    if (c_pct == 0 or c_pct is None) and m.get('change_abs') and m.get('close'):
         c_pct = abs(m['change_abs']) / m['close'] * 100 * (1 if m.get('price_sign', 1) == 1 else -1)
         m['change_pct'] = c_pct

    if m.get('doi_pct') is not None and c_pct:
        m['oe'] = abs(m['doi_pct']) / abs(c_pct)
    else:
        m['oe'] = None

    # 4. Liquidations: Propagate None
    liq_l = m.get('liq_long')
    liq_s = m.get('liq_short')
    total_liq = None
    
    if liq_l is not None and liq_s is not None:
        total_liq = liq_l + liq_s
        m['liq_share_pct'] = (total_liq / m.get('volume', 0) * 100) if m.get('volume', 0) else 0
        m['limb_pct'] = ((liq_s - liq_l) / total_liq * 100) if total_liq else 0
    else:
        total_liq = None
        m['liq_share_pct'] = None
        m['limb_pct'] = None
        
    m['liq_squeeze'] = (m['liq_share_pct'] >= config['global_squeeze_limit']) if m.get('liq_share_pct') is not None else False
    m['liq_threshold'] = config.get('global_squeeze_limit', 0.30)



    # 5. Dominant Reject
    LT, UT, Body, CLV = m['lower_tail_pct'], m['upper_tail_pct'], m['body_pct'], m['clv_pct']
    dr = None
    if (LT >= 3 * Body) and (UT <= 10) and (CLV >= 85): dr = "bull_Ideal"
    elif (UT >= 3 * Body) and (LT <= 10) and (CLV <= 15): dr = "bear_Ideal"
    elif (LT >= 2 * Body) and (UT <= 25) and (CLV >= 75): dr = "bull_Valid"
    elif (UT >= 2 * Body) and (LT <= 25) and (CLV <= 25): dr = "bear_Valid"
    elif (LT >= 1.5 * Body) and (CLV >= 65) and (UT <= 0.5 * LT): dr = "bull_Loose"
    elif (UT >= 1.5 * Body) and (CLV <= 35) and (LT <= 0.5 * UT): dr = "bear_Loose"
    m['dominant_reject'] = dr

    # 6. Advanced Threshold Logic
    porog_df = config.get('porog_doi', pd.DataFrame())
    asset_coeffs = config.get('asset_coeffs', {})
    tf_params = config.get('tf_params', {})
    
    # Keys for lookup
    # 1. Porog Table: Columns are lowercase (btc, eth), TF column values might be mixed.
    symbol_key_lower = m.get('symbol_clean', '').lower()
    
    # 2. Asset Coeffs: Keys are Uppercase (BTC, ETH). 
    symbol_key_upper = m.get('symbol_clean', '').upper()
    
    tf_val = str(m.get('tf', '4h')) # Ensure string
    tf_key = tf_val # Restore compatibility for later lines
    
    # Default values (Fallbacks)
    base_sens = 0.5
    coeff = 1.0
    
    # Dynamic Lookup: Base Sensitivity
    if not porog_df.empty and symbol_key_lower in porog_df.columns and 'timeframe' in porog_df.columns:
        # Case-insensitive TF match
        # Convert both column and target value to lowercase for comparison
        try:
            # Create mask for matching timeframe
            mask = porog_df['timeframe'].astype(str).str.lower() == tf_val.lower()
            row = porog_df.loc[mask]
            
            if not row.empty:
                base_sens = float(row[symbol_key_lower].values[0])
        except Exception:
            pass # Keep default if matching fails
            
    # Dynamic Lookup: Asset Coefficient
    if symbol_key_upper in asset_coeffs:
        coeff = asset_coeffs[symbol_key_upper]
        
    m['porog_final'] = base_sens * coeff
    m['epsilon'] = 0.33 * m['porog_final']
    m['oi_in_sens'] = abs(m['doi_pct']) <= m['porog_final']
    
    # K Params: Case-insensitive lookup for tf_params
    k_set, k_ctr, k_unl = 1.0, 1.0, 1.0
    tf_sens_base = None # Default is None (Strict Validation)
    
    # Try exact match first
    tf_data = tf_params.get(tf_key)
    
    # If not found, try case-insensitive linear search
    if not tf_data:
        for k_tf, v_data in tf_params.items():
            if str(k_tf).lower() == tf_key.lower():
                tf_data = v_data
                break
    
    if tf_data:
        k_set = float(tf_data.get('k_set', 1.0))
        k_ctr = float(tf_data.get('k_ctr', 1.0))
        k_unl = float(tf_data.get('k_unl', 1.0))
        # User defined 'sens' in tf_params for these metrics
        if 'sens' in tf_data:
            tf_sens_base = float(tf_data['sens'])

    # Calculate T-thresholds using TF-specific Sens * K-factor
    # (Removed asset coeff per user request)
    
    # Strict validation: Only calculate if we found a base sensitivity
    if tf_sens_base is not None:
        t_base = tf_sens_base 
        
        m['t_set_pct'] = round(t_base * k_set, 2)
        m['oi_set'] = m['doi_pct'] >= m['t_set_pct']
        
        m['t_counter_pct'] = round(t_base * k_ctr, 2)
        m['oi_counter'] = (m['dpx'] == -1) and (m['doi_pct'] >= m['t_counter_pct'])
        
        m['t_unload_pct'] = round(-(t_base * k_unl), 2)
        m['oi_unload'] = m['doi_pct'] <= m['t_unload_pct']
        
        # Pass TF Sens to Diver Engine (as strictly requested)
        m['tf_sens'] = tf_sens_base
    else:
        # Propagate None to trigger validation error downstream
        m['t_set_pct'] = None
        m['oi_set'] = None
        m['t_counter_pct'] = None
        m['oi_counter'] = None
        m['t_unload_pct'] = None
        m['oi_unload'] = None
        m['tf_sens'] = None
    
    m['r_strength'] = abs(m['doi_pct']) / m['porog_final'] if m['porog_final'] else 0
    m['r'] = m['r_strength']
    
    return m

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
def fmt_num(val, decimals=2, is_pct=False):
    if val is None: return "−"
    if isinstance(val, bool): return "true" if val else "false"
    if isinstance(val, (int, float)):
        s = f"{val:,.{decimals}f}".replace(",", " ").replace(".", ",")
        if is_pct: s += "%"
        return s
    return str(val)

def generate_full_report(d):
    ts_obj = datetime.fromisoformat(d['ts'])
    ts_str = ts_obj.strftime("%d.%m.%Y %H:%M")
    dr = d.get('dominant_reject') or "−"
    
    lines = [
        f"ts: {ts_str}",
        f"exchange: {d.get('exchange')}",
        f"symbol: {d.get('raw_symbol')}",
        f"tf: {d.get('tf')}",
        f"open: {fmt_num(d.get('open'))}",
        f"high: {fmt_num(d.get('high'))}",
        f"low: {fmt_num(d.get('low'))}",
        f"close: {fmt_num(d.get('close'))}",
        f"volume: {fmt_num(d.get('volume'), 0)}",
        f"buy_volume: {fmt_num(d.get('buy_volume'), 0)}",
        f"sell_volume: {fmt_num(d.get('sell_volume'), 0)}",
        f"buy_trades: {fmt_num(d.get('buy_trades'), 0)}",
        f"sell_trades: {fmt_num(d.get('sell_trades'), 0)}",
        f"oi_open: {fmt_num(d.get('oi_open'), 0)}",
        f"oi_high: {fmt_num(d.get('oi_high'), 0)}",
        f"oi_low: {fmt_num(d.get('oi_low'), 0)}",
        f"oi_close: {fmt_num(d.get('oi_close'), 0)}",
        f"liq_long: {fmt_num(d.get('liq_long'), 0)}",
        f"liq_short: {fmt_num(d.get('liq_short'), 0)}",
        f"range: {fmt_num(d.get('range'))}",
        f"body_pct: {fmt_num(d.get('body_pct'), 2, True)}",
        f"clv_pct: {fmt_num(d.get('clv_pct'), 2, True)}",
        f"upper_tail_pct: {fmt_num(d.get('upper_tail_pct'), 2, True)}",
        f"lower_tail_pct: {fmt_num(d.get('lower_tail_pct'), 2, True)}",
        f"price_sign: {d.get('price_sign')}",
        f"dominant_reject: {dr}",
        f"cvd_pct: {fmt_num(d.get('cvd_pct'), 2, True)}",
        f"cvd_sign: {d.get('cvd_sign')}",
        f"cvd_small: {fmt_num(d.get('cvd_small'))}",
        f"dpx: {fmt_num(d.get('dpx'))}",
        f"price_vs_delta: {d.get('price_vs_delta')}",
        f"dtrades_pct: {fmt_num(d.get('dtrades_pct'), 2, True)}",
        f"ratio_stable: {fmt_num(d.get('ratio_stable'))}",
        f"tilt_pct: {fmt_num(d.get('tilt_pct'), 2, True)}",
        f"doi_pct: {fmt_num(d.get('doi_pct'), 2, True)}",
        f"oi_in_sens: {fmt_num(d.get('oi_in_sens'))}",
        f"oi_set: {fmt_num(d.get('oi_set'))}",
        f"oi_counter: {fmt_num(d.get('oi_counter'))}",
        f"oi_unload: {fmt_num(d.get('oi_unload'))}",
        f"oipos: {fmt_num(d.get('oipos'), 2, True)}",
        f"oi_path: {d.get('oi_path')}",
        f"oe: {fmt_num(d.get('oe'))}",
        f"liqshare_pct: {fmt_num(d.get('liq_share_pct'), 2, True)}",
        f"limb_pct: {fmt_num(d.get('limb_pct'), 2, True)}",
        f"liq_squeeze: {fmt_num(d.get('liq_squeeze'))}",
        f"range_pct: {fmt_num(d.get('range_pct'), 2, True)}",
        f"implied_price: {fmt_num(d.get('implied_price'))}",
        f"avg_trade_buy: {fmt_num(d.get('avg_trade_buy'))}",
        f"avg_trade_sell: {fmt_num(d.get('avg_trade_sell'))}"
    ]
    return "\n".join(lines)


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

    # 1. Split & Clean
    raw_chunks = re.split(r'(?=(?:Binance|Bybit|OKX)\s+·)', raw_text, flags=re.IGNORECASE)
    raw_chunks = [x.strip() for x in raw_chunks if x.strip()]
    
    merged_groups = {}
    pending_ts = None
    TS_REGEX_STREAM = r'(\d{1,2}\.\d{1,2}\.\d{4}\s+\d{1,2}:\d{2}(?::\d{2})?)'

    # 2. Iterate & Parse
    for chunk in raw_chunks:
        # 2a. Find trailing TS
        next_ts = None
        clean_chunk = chunk
        all_ts = list(re.finditer(TS_REGEX_STREAM, chunk))
        if all_ts:
            last_match = all_ts[-1]
            # Check if everything AFTER the match is just whitespace
            suffix = chunk[last_match.end():]
            if not suffix.strip(): 
                next_ts = last_match.group(1)
                clean_chunk = chunk[:last_match.start()].strip()

        # 2b. Parse
        # 2b. Parse
        base_data = parse_raw_input(clean_chunk)
        
        # 2c. TS Forwarding Logic
        if base_data.get('exchange') == 'Unknown':
            if next_ts:
                 pending_ts = next_ts
                 next_ts = None 
            elif base_data.get('parsed_ts'):
                 pending_ts = base_data['parsed_ts']
            continue

        if pending_ts:
            try:
                try:
                    dt = datetime.strptime(pending_ts, "%d.%m.%Y %H:%M:%S")
                except ValueError:
                    dt = datetime.strptime(pending_ts, "%d.%m.%Y %H:%M")
                base_data['ts'] = dt.isoformat()
                
                # If we successfully patched TS, remove it from missing_fields
                if 'missing_fields' in base_data and 'ts' in base_data['missing_fields']:
                    base_data['missing_fields'].remove('ts')
                    if not base_data['missing_fields']:
                        del base_data['missing_fields']
            except:
                pass
        
        if next_ts:
            pending_ts = next_ts
        else:
            pending_ts = None

        # STRICT CHECK: If TS is still missing -> Error
        if not base_data.get('ts'):
            # Convert to user friendly error, maybe skip or fail batch?
            # User wants "Warning mandatory".
            # We can add to orphan_errors or fail immediately?
            # Let's add strict error which will be returned in orphan_errors list (as general errors)
            # But the function returns (batch, errors).
            # We need a way to say "This specific candle failed".
            # For now, let's treat it as a critical error for this candle and not add it to merged_groups.
            pass # We will handle this by checking required keys later?
            # Or better, check here.
            pass

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
TABS = ["Отчеты", "Свечи", "Дивер", "Уровни"]

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



# tab1, tab2, tab3 = st.tabs(["Отчеты", "Свечи", "Дивер"]) - REMOVED

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
                    st.warning(f"Не найдены поля: {', '.join(full_data['missing_fields'])}")
                
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

