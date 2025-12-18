import streamlit as st
import re
import pandas as pd
import uuid
from datetime import datetime, time
from supabase import create_client, Client

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
        [data-testid="stVerticalBlockBorderWrapper"] > div {
            background: rgba(255, 255, 255, 0.03) !important; 
            backdrop-filter: blur(20px) !important;
            border: 1px solid rgba(255, 255, 255, 0.08) !important;
            border-top: 1px solid rgba(255, 255, 255, 0.15) !important;
            border-radius: 20px !important;
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.4) !important;
            margin-bottom: 24px;
            padding: 24px !important;
        }
        .stTextInput input, .stNumberInput input, .stDateInput input, .stTextArea textarea {
            background-color: rgba(0, 0, 0, 0.3) !important;
            color: white !important;
            border: 1px solid rgba(255, 255, 255, 0.1) !important;
            border-radius: 10px !important;
        }
        [data-testid="stDataFrame"] {
            background: rgba(0, 0, 0, 0.2);
            border-radius: 12px;
            padding: 10px;
        }
        .tf-badge {
            background: linear-gradient(135deg, #ff4b4b, #d10000);
            color: white; padding: 3px 10px; border-radius: 12px;
            font-size: 0.85em; font-weight: 700; margin-left: 8px;
            border: 1px solid rgba(255,255,255,0.2);
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
        return float(clean_str) * multiplier
    except:
        return 0.0

def extract(regex, text):
    # Добавили DOTALL, чтобы искать по всему тексту даже с переносами строк
    match = re.search(regex, text, re.IGNORECASE | re.DOTALL)
    if match: return parse_value_raw(match.group(1))
    return 0.0

# --- 🧠 ЯДРО: 1. RAW INPUT PARSING (ИСПРАВЛЕНО) ---
def parse_raw_input(text, user_date, user_time):
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
                 data['ts'] = datetime.combine(user_date, user_time).isoformat()
    else:
        data['ts'] = datetime.combine(user_date, user_time).isoformat()

    # OHLC
    ohlc_match = re.search(r'O ([\d,.]+) H ([\d,.]+) L ([\d,.]+) C ([\d,.]+)', text)
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
    data['buy_volume'] = extract(r'Active Buy/Sell Volume.*?Buy ([+\-]?[\d,.]+[MKB]?)', text)
    data['sell_volume'] = abs(extract(r'Active Buy/Sell Volume.*?Sell ([+\-]?[\d,.]+[MKB]?)', text))
    data['abv_delta'] = extract(r'Active Buy/Sell Volume.*?Delta ([+\-]?[\d,.]+[MKB]?)', text)
    data['abv_ratio'] = extract(r'Active Buy/Sell Volume.*?Ratio ([+\-]?[\d,.]+)', text)
    
    # Trades
    data['buy_trades'] = extract(r'Active Buy/Sell Trades.*?Buy ([+\-]?[\d,.]+[MKB]?)', text)
    data['sell_trades'] = abs(extract(r'Active Buy/Sell Trades.*?Sell ([+\-]?[\d,.]+[MKB]?)', text))
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
    data['liq_short'] = abs(extract(r'Liquidation.*?Short ([+\-]?[\d,.]+[MKB]?)', text))

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
    
    return data

# --- 🧠 ЯДРО: 2. CALCULATED METRICS ---
def calculate_metrics(raw_data, config):
    """Считает метрики на основе Raw Input и конфигов из БД."""
    m = raw_data.copy()
    
    # 1. Geometry
    m['range'] = m.get('high', 0) - m.get('low', 0)
    m['range_pct'] = (m['range'] / m['close'] * 100) if m.get('close') else 0
    m['body_pct'] = (abs(m.get('close', 0) - m.get('open', 0)) / m['range'] * 100) if m['range'] else 0
    
    if m['range'] == 0: m['clv_pct'] = 50.0
    else: m['clv_pct'] = (m.get('close', 0) - m.get('low', 0)) / m['range'] * 100
    
    if m['range'] == 0:
        m['upper_tail_pct'] = 0; m['lower_tail_pct'] = 0
    else:
        m['upper_tail_pct'] = (m.get('high', 0) - max(m.get('open', 0), m.get('close', 0))) / m['range'] * 100
        m['lower_tail_pct'] = (min(m.get('open', 0), m.get('close', 0)) - m.get('low', 0)) / m['range'] * 100

    m['price_sign'] = 1 if m.get('close', 0) >= m.get('open', 0) else -1

    # 2. Volume & Trades Metrics
    total_active_vol = m.get('buy_volume', 0) + m.get('sell_volume', 0)
    m['cvd_pct'] = (m.get('abv_delta', 0) / total_active_vol * 100) if total_active_vol else 0
    m['cvd_sign'] = 1 if m.get('abv_delta', 0) > 0 else -1
    m['cvd_small'] = abs(m['cvd_pct']) < 1.0 

    total_trades = m.get('buy_trades', 0) + m.get('sell_trades', 0)
    m['dtrades_pct'] = (m.get('trades_delta', 0) / total_trades * 100) if total_trades else 0
    
    sign_abv = (m.get('abv_delta', 0) > 0) - (m.get('abv_delta', 0) < 0)
    sign_trades = (m.get('trades_delta', 0) > 0) - (m.get('trades_delta', 0) < 0)
    m['ratio_stable'] = (sign_abv == sign_trades)

    m['avg_trade_buy'] = (m.get('buy_volume', 0) / m.get('buy_trades', 0)) if m.get('buy_trades', 0) else 0
    m['avg_trade_sell'] = (m.get('sell_volume', 0) / m.get('sell_trades', 0)) if m.get('sell_trades', 0) else 0
    
    if m['avg_trade_buy'] > 0:
        m['tilt_pct'] = ((m['avg_trade_sell'] / m['avg_trade_buy']) - 1) * 100
    else:
        m['tilt_pct'] = 0

    m['implied_price'] = (m.get('volume', 0) / total_active_vol) if total_active_vol else 0
    m['dpx'] = m['price_sign'] * m['cvd_sign'] 
    
    if m['dpx'] == 1: m['price_vs_delta'] = "match"
    elif m['dpx'] == -1: m['price_vs_delta'] = "div"
    else: m['price_vs_delta'] = "neutral"

    # 3. Open Interest Calculations
    m['doi_pct'] = ((m.get('oi_close', 0) - m.get('oi_open', 0)) / m.get('oi_open', 0) * 100) if m.get('oi_open', 0) else 0
    
    oi_rng = m.get('oi_high', 0) - m.get('oi_low', 0)
    if oi_rng == 0: m['oipos'] = 0.5
    else:
        raw_pos = (m.get('oi_close', 0) - m.get('oi_low', 0)) / oi_rng
        m['oipos'] = max(0.0, min(1.0, raw_pos))

    up_move = abs(m.get('oi_high', 0) - m.get('oi_open', 0))
    dn_move = abs(m.get('oi_low', 0) - m.get('oi_open', 0))
    if up_move > dn_move: m['oi_path'] = "up"
    elif dn_move > up_move: m['oi_path'] = "down"
    else: m['oi_path'] = "neutral"

    m['oe'] = abs(m['doi_pct']) / abs(m['change_pct']) if abs(m.get('change_pct', 0)) > 0 else 0

    # 4. Liquidations
    total_liq = m.get('liq_long', 0) + m.get('liq_short', 0)
    m['liq_share_pct'] = (total_liq / m.get('volume', 0) * 100) if m.get('volume', 0) else 0
    m['limb_pct'] = ((m.get('liq_short', 0) - m.get('liq_long', 0)) / total_liq * 100) if total_liq else 0
    m['liq_squeeze'] = m['liq_share_pct'] >= config['global_squeeze_limit']

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
    
    symbol_key = m.get('symbol_clean', '').lower()
    tf_key = m.get('tf', '4h')
    
    base_sens = 0.5
    coeff = 1.0
    
    if not porog_df.empty and symbol_key in porog_df.columns and 'timeframe' in porog_df.columns:
        row = porog_df.loc[porog_df['timeframe'] == tf_key]
        if not row.empty:
            base_sens = float(row[symbol_key].values[0])
            
    if m.get('symbol_clean') in asset_coeffs:
        coeff = asset_coeffs[m['symbol_clean']]
        
    m['porog_final'] = base_sens * coeff
    m['epsilon'] = 0.33 * m['porog_final']
    m['oi_in_sens'] = abs(m['doi_pct']) <= m['porog_final']
    
    k_set = tf_params.get(tf_key, {}).get('k_set', 1.0)
    k_ctr = tf_params.get(tf_key, {}).get('k_ctr', 1.0)
    k_unl = tf_params.get(tf_key, {}).get('k_unl', 1.0)
    
    m['t_set_pct'] = m['porog_final'] * k_set
    m['oi_set'] = m['doi_pct'] > m['t_set_pct']
    
    m['t_counter_pct'] = m['porog_final'] * k_ctr
    m['oi_counter'] = (m['dpx'] == -1) and (m['doi_pct'] > m['t_counter_pct'])
    
    m['t_unload_pct'] = -(m['porog_final'] * k_unl)
    m['oi_unload'] = m['doi_pct'] <= m['t_unload_pct']
    
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
    # Whitelist of fields that can be updated from Supplementary candles (CoinGlass)
    SUPPLEMENTARY_FIELDS = {
        'fr_open', 'fr_high', 'fr_low', 'fr_close',
        'agg_fr_open', 'agg_fr_high', 'agg_fr_low', 'agg_fr_close',
        'basis',
        'ls_ratio_open', 'ls_ratio_high', 'ls_ratio_low', 'ls_ratio_close',
        'idx_open', 'idx_high', 'idx_low', 'idx_close',
        'net_longs_open', 'net_longs_close', 'net_longs_delta',
        'net_shorts_open', 'net_shorts_close', 'net_shorts_delta'
    }

    merged_batch = []
    for new_row in batch_data:
        # Use normalized key
        k = get_merge_key(new_row.get('exchange'), new_row.get('symbol_clean'), new_row.get('tf'), new_row.get('ts'))
        existing = db_map.get(k)
        
        if existing:
            # Стратегия слияния (Whitelist + Source Check):
            combined = existing.copy()
            
            # Определяем тип входящих данных: это Доп. свеча (CoinGlass) или Основная?
            # Если есть 'fr_open', считаем это Доп. свечой.
            is_supp_source = 'fr_open' in new_row
            
            for key, val in new_row.items():
                if key in SUPPLEMENTARY_FIELDS:
                    # Дополнительные поля обновляем ТОЛЬКО если источник - Доп. свеча
                    if is_supp_source:
                        combined[key] = val
                elif not is_supp_source:
                    # Основные поля (OHLC, Volume, Trades, etc) НЕ трогаем, если источник - Доп. свеча
                    # Если источник - Основная свеча (не Supp), то обновляем, если в базе пусто
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

def load_candles_db(limit=500, tf_list=None, date_range=None):
    try:
        query = supabase.table('candles').select("*").order('ts', desc=True)
        
        # Apply Filters
        if tf_list:
            query = query.in_('tf', tf_list)
            
        if date_range:
            # date_range is typically (start_date, end_date) from st.date_input
            if len(date_range) == 2:
                start_date, end_date = date_range
                # Ensure we cover the whole end day
                import datetime as dt
                # Convert to string ISO format if they are date objects
                s_str = start_date.strftime('%Y-%m-%d 00:00:00')
                e_str = end_date.strftime('%Y-%m-%d 23:59:59')
                query = query.gte('ts', s_str).lte('ts', e_str)
            elif len(date_range) == 1:
                # Single day selected
                d_str = date_range[0].strftime('%Y-%m-%d')
                query = query.gte('ts', f"{d_str} 00:00:00").lte('ts', f"{d_str} 23:59:59")
        
        # Apply Limit
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

def generate_cg_report(d):
    lines = [
        "Funding Rate:",
        f"fr_open: {fmt_num(d.get('fr_open'), 6, True)}",
        f"fr_high: {fmt_num(d.get('fr_high'), 6, True)}",
        f"fr_low: {fmt_num(d.get('fr_low'), 6, True)}",
        f"fr_close: {fmt_num(d.get('fr_close'), 6, True)}",
        "",
        "Aggregated Funding Rate:",
        f"agg_fr_open: {fmt_num(d.get('agg_fr_open'), 6, True)}",
        f"agg_fr_high: {fmt_num(d.get('agg_fr_high'), 6, True)}",
        f"agg_fr_low: {fmt_num(d.get('agg_fr_low'), 6, True)}",
        f"agg_fr_close: {fmt_num(d.get('agg_fr_close'), 6, True)}",
        "",
        "Basis:",
        f"basis: {fmt_num(d.get('basis'))}",
        "",
        "Long/Short Ratio:",
        f"ls_ratio_open: {fmt_num(d.get('ls_ratio_open'))}",
        f"ls_ratio_high: {fmt_num(d.get('ls_ratio_high'))}",
        f"ls_ratio_low: {fmt_num(d.get('ls_ratio_low'))}",
        f"ls_ratio_close: {fmt_num(d.get('ls_ratio_close'))}",
        "",
        "Index Price:",
        f"idx_open: {fmt_num(d.get('idx_open'))}",
        f"idx_high: {fmt_num(d.get('idx_high'))}",
        f"idx_low: {fmt_num(d.get('idx_low'))}",
        f"idx_close: {fmt_num(d.get('idx_close'))}",
        "",
        "Net Longs:",
        f"net_longs_open: {fmt_num(d.get('net_longs_open'), 0)}",
        f"net_longs_close: {fmt_num(d.get('net_longs_close'), 0)}",
        f"net_longs_delta: {fmt_num(d.get('net_longs_delta'), 0)}",
        "",
        "Net Shorts:",
        f"net_shorts_open: {fmt_num(d.get('net_shorts_open'), 0)}",
        f"net_shorts_close: {fmt_num(d.get('net_shorts_close'), 0)}",
        f"net_shorts_delta: {fmt_num(d.get('net_shorts_delta'), 0)}"
    ]
    return "\n".join(lines)

# --- 🖥 UI ---
st.title("🖤 VANTA")
tab1, tab2 = st.tabs(["Отчеты", "БД"])

with tab1:
    input_text = st.text_area("Вставьте данные свечи", height=150, label_visibility="collapsed", placeholder="Вставьте свечи здесь...")
    
    # Скрываем выбор даты/времени, берем текущие
    user_date = datetime.now().date()
    user_time = datetime.now().time()
    
    process = st.button("Распарсить", type="primary")

    if process and input_text:
        config = load_configurations()
        if config:
            raw_chunks = re.split(r'(?=(?:Binance|Bybit|OKX)\s+·)', input_text, flags=re.IGNORECASE)
            raw_chunks = [x.strip() for x in raw_chunks if x.strip()]
            
            merged_groups = {}
            pending_ts = None
            TS_REGEX_STREAM = r'(\d{1,2}\.\d{1,2}\.\d{4}\s+\d{1,2}:\d{2}(?::\d{2})?)'

            for chunk in raw_chunks:
                # 1. Поиск "хвостового" таймстемпа для следующего чанка
                next_ts = None
                clean_chunk = chunk
                all_ts = list(re.finditer(TS_REGEX_STREAM, chunk))
                if all_ts:
                    last_match = all_ts[-1]
                    # Если дата в самом конце строки (с небольшим допуском) для валидного чанка
                    # Или если это Unknown чанк (где дата может быть единственным содержимым)
                    if last_match.end() >= len(chunk) - 5: 
                        next_ts = last_match.group(1)
                        clean_chunk = chunk[:last_match.start()].strip()

                # 2. Парсинг (очищенного) чанка
                base_data = parse_raw_input(clean_chunk, user_date, user_time)
                
                # 3. Логика переноса даты
                if base_data.get('exchange') == 'Unknown':
                    # Если это "мусорный" чанк, но он содержал дату
                    if next_ts:
                         pending_ts = next_ts
                         next_ts = None # Consumed
                    elif base_data.get('parsed_ts'):
                         pending_ts = base_data['parsed_ts']
                    continue

                # Применяем pending_ts, если есть
                if pending_ts:
                    # Пробуем распарсить pending_ts
                    try:
                        # Сначала пробуем с секундами, потом без
                        try:
                            dt = datetime.strptime(pending_ts, "%d.%m.%Y %H:%M:%S")
                        except ValueError:
                            dt = datetime.strptime(pending_ts, "%d.%m.%Y %H:%M")
                        base_data['ts'] = dt.isoformat()
                    except:
                        pass # Оставляем как есть, если не распарсилось
                
                # Обновляем pending_ts для следующего круга
                if next_ts:
                    pending_ts = next_ts
                else:
                    pending_ts = None

                key = (base_data.get('exchange'), base_data.get('symbol_clean'), base_data.get('tf'), base_data.get('ts'))
                
                if key not in merged_groups:
                    merged_groups[key] = base_data
                else:
                    existing = merged_groups[key]
                    for k, v in base_data.items():
                        if v and (k not in existing or not existing[k]):
                            existing[k] = v
            
            # --- LOCAL MERGED BATCH IS READY ---
            local_batch = list(merged_groups.values())
            
            # --- DB MERGE (ENRICHMENT) ---
            final_batch_list = fetch_and_merge_db(local_batch, config)
            
            processed_batch = []
            
            for raw_data in final_batch_list:
                full_data = calculate_metrics(raw_data, config)
                
                # Logic: Check for Main Data (Active Volume) and Supplementary Data (Funding Rate)
                has_main = raw_data.get('buy_volume', 0) != 0
                has_supp = 'fr_open' in raw_data
                
                # 1. X-RAY Report (Requires Main Data)
                if has_main:
                    full_data['x_ray'] = generate_full_report(full_data)
                else:
                    full_data['x_ray'] = None
                
                # 2. CoinGlass Report
                cg_text = generate_cg_report(full_data) if has_supp else ""
                
                if has_main and has_supp:
                    # Both: X-RAY + CoinGlass
                    full_data['x_ray_coinglass'] = full_data['x_ray'] + "\n\n" + cg_text
                elif has_supp and not has_main:
                    # Supp Only: Just CoinGlass
                    full_data['x_ray_coinglass'] = cg_text
                else:
                    # Main Only: x_ray_coinglass is None
                    full_data['x_ray_coinglass'] = None
                
                processed_batch.append(full_data)
            
            st.session_state.processed_batch = processed_batch

    if 'processed_batch' in st.session_state and st.session_state.processed_batch:
        batch = st.session_state.processed_batch
        
        # Save Button at the very top of the section
        if st.button(f"💾 Сохранить {len(batch)} свечей", type="primary"):
            if save_candles_batch(batch):
                st.success("Сохранено!")
                st.cache_data.clear()

        st.divider()

        for full_data in batch:
            # Prepare Label
            try:
                ts_obj = datetime.fromisoformat(full_data['ts'])
                ts_str = ts_obj.strftime('%d.%m.%Y %H:%M')
            except:
                ts_str = str(full_data.get('ts'))
            
            # Minimalist Header in Expander Label
            label = f"{ts_str} · {full_data.get('exchange')} · {full_data.get('symbol_clean')} · {full_data.get('tf')} · O {fmt_num(full_data.get('open'))}"
            
            with st.expander(label):
                with st.container(height=300):
                    # Logic to display exactly what is available
                    if full_data.get('x_ray_coinglass'):
                         # If this is present, it's either "Supp Only" (CG only) OR "Both" (XRAY+CG)
                         st.code(full_data['x_ray_coinglass'], language="yaml")
                    elif full_data.get('x_ray'):
                         # If x_ray_coinglass is None, but x_ray is present (Main Only)
                         st.code(full_data['x_ray'], language="yaml")

with tab2:
    
    # 1. Filters Toolbar
    with st.expander("🔎 Фильтры поиска", expanded=True):
        f_col1, f_col2, f_col3 = st.columns([1, 3, 1])
        
        with f_col1:
            # Common TFs
            tfs = ['1m', '5m', '15m', '1H', '4H', '1D', '1W', '1M']
            selected_tfs = st.multiselect("TF:", options=tfs, default=[], placeholder="Все")
        
        with f_col2:
            # Date Input with RU format
            selected_dates = st.date_input("Диапазон дат:", value=[], format="DD.MM.YYYY", help="Выберите начальную и конечную дату")
            
        with f_col3:
            limit_rows = st.selectbox("Лимит:", [100, 500, 1000, 5000], index=1)

    # 2. Load Data with Filters
    df = load_candles_db(limit=limit_rows, tf_list=selected_tfs, date_range=selected_dates)

    if not df.empty:
        if 'note' not in df.columns: df['note'] = ""
        df.insert(0, "delete", False)
        # Convert TS
        df['ts'] = pd.to_datetime(df['ts'], errors='coerce')

        # 3. Toolbar (Save / Delete)
        c1, c2 = st.columns([0.25, 0.75])
        
        # SAVE BUTTON
        with c1:
             if st.button("💾 Сохранить изменения", key="btn_save_top", type="primary"):
                  if "db_editor" in st.session_state and "edited_rows" in st.session_state["db_editor"]:
                      changes_map = st.session_state["db_editor"]["edited_rows"]
                      if changes_map:
                          count = 0
                          for idx, changes in changes_map.items():
                              valid_changes = {k: v for k, v in changes.items() if k != 'delete'}
                              if valid_changes:
                                  # Get ID from the original dataframe (careful with index if sorted)
                                  # st.data_editor uses the provided df index. 
                                  # If we filter or range, we must be consistent.
                                  # df is fresh from DB, so idx corresponds to this page's df.
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
        
        # SELECT ALL CHECKBOX
        with c2:
             st.write("") # spacer
             if st.checkbox("Выделить все на этой странице", key="select_all_del_top"):
                  df['delete'] = True

        visible_cols = ['ts', 'tf', 'x_ray', 'x_ray_coinglass', 'x_ray_composite', 'note', 'raw_data']
        
        # 4. Data Editor
        edited_df = st.data_editor(
            df,
            key="db_editor",
            column_order=["delete"] + visible_cols,
            use_container_width=True,
            hide_index=True,
            height=800, # Increased height
            column_config={
                "delete": st.column_config.CheckboxColumn("🗑", default=False, width="small"),
                "ts": st.column_config.DatetimeColumn("Time", format="DD.MM.YYYY HH:mm", width="small", disabled=True),
                "tf": st.column_config.TextColumn("tf", width="small", disabled=True),
                "x_ray": st.column_config.TextColumn("X-RAY", width="small"),
                "x_ray_coinglass": st.column_config.TextColumn("CG Report", width="small"),
                "x_ray_composite": st.column_config.TextColumn("Composite", width="small"),
                "note": st.column_config.TextColumn("Note ✏️", width="medium"),
                "raw_data": st.column_config.TextColumn("Raw", width="large"),
            }
        )
        
        # DELETE BUTTON (Below Table)
        to_delete = edited_df[edited_df.delete == True]
        if not to_delete.empty:
            if st.button(f"🗑 Удалить {len(to_delete)} выделенных", key="btn_del_bottom", type="secondary"):
                if delete_candles_db(to_delete['id'].tolist()):
                    st.toast("Удалено!")
                    st.cache_data.clear()
                    st.rerun()
    else:
        st.info("База данных пуста (или на этой странице нет данных).")