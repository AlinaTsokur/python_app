import re
import uuid
import sys
import os
import pandas as pd
from datetime import datetime, time
from supabase import create_client

# Try standard lib for python 3.11+, else toml
try:
    import tomllib as toml
except ImportError:
    try:
        import toml
    except ImportError:
        print("No toml library found.")
        sys.exit(1)

# --- MOCK STREAMLIT SECRETS & HELPERS ---
def load_secrets():
    path = ".streamlit/secrets.toml"
    with open(path, "rb") as f:
        return toml.load(f)

secrets = load_secrets()
url = secrets["SUPABASE_URL"]
key = secrets["SUPABASE_KEY"]
supabase = create_client(url, key)

class MockSt:
    def error(self, msg): print(f"❌ Error: {msg}")
    def warning(self, msg): print(f"⚠️ Warning: {msg}")
    def info(self, msg): print(f"ℹ️ Info: {msg}")
    def success(self, msg): print(f"✅ Success: {msg}")

st = MockSt()

# --- COPY FROM APP.PY (Key Logic) ---

def parse_value_raw(val_str):
    if not val_str or val_str == '-' or val_str == '': return 0.0
    clean_str = str(val_str).replace(',', '').replace('%', '').strip()
    multiplier = 1.0
    if clean_str.upper().endswith('K'): multiplier = 1_000.0; clean_str = clean_str[:-1]
    elif clean_str.upper().endswith('M'): multiplier = 1_000_000.0; clean_str = clean_str[:-1]
    elif clean_str.upper().endswith('B'): multiplier = 1_000_000_000.0; clean_str = clean_str[:-1]
    try:
        clean_str = re.sub(r'[^\d.-]', '', clean_str)
        return float(clean_str) * multiplier
    except: return 0.0

def extract(regex, text):
    match = re.search(regex, text, re.IGNORECASE | re.DOTALL)
    if match: return parse_value_raw(match.group(1))
    return 0.0

def load_configurations():
    config = {}
    try:
        res_ac = supabase.table('asset_coeffs').select("*").execute()
        config['asset_coeffs'] = {row['asset']: row['coeff'] for row in res_ac.data} if res_ac.data else {}
        res_porog = supabase.table('porog_doi').select("*").execute()
        if res_porog.data:
            df = pd.DataFrame(res_porog.data)
            if 'tf' in df.columns: df = df.rename(columns={'tf': 'timeframe'})
            config['porog_doi'] = df
        else: config['porog_doi'] = pd.DataFrame()
        res_tf = supabase.table('tf_params').select("*").execute()
        config['tf_params'] = {row['tf']: row for row in res_tf.data} if res_tf.data else {}
        res_liq = supabase.table('liqshare_thresholds').select("*").eq('name', 'squeeze').execute()
        config['global_squeeze_limit'] = float(res_liq.data[0]['value']) if res_liq.data else 0.3
        return config
    except Exception as e:
        st.error(f"Config Load Error: {e}")
        return None

def parse_raw_input(text, user_date, user_time):
    # (Copied from APP.PY Step 789/805 - Simplified for brevity but keeping logic)
    data = {}
    REGEX_FLAGS = re.IGNORECASE | re.DOTALL
    header_match = re.search(r'(.+?) · (.+?) · (\w+)', text)
    data['exchange'] = header_match.group(1).strip() if header_match else 'Unknown'
    data['raw_symbol'] = header_match.group(2).strip() if header_match else 'Unknown'
    data['tf'] = header_match.group(3).strip() if header_match else '4h'
    data['symbol_clean'] = data['raw_symbol'].split(' ')[0].replace('USDT', '').replace('PERP', '')
    
    ts_match = re.search(r'(\d{1,2}\.\d{1,2}\.\d{4}\s+\d{1,2}:\d{2}(?::\d{2})?)', text)
    if ts_match:
        ts_str = ts_match.group(1)
        try:
            dt_obj = datetime.strptime(ts_str, "%d.%m.%Y %H:%M:%S")
            data['ts'] = dt_obj.isoformat()
        except ValueError:
            try:
                dt_obj = datetime.strptime(ts_str, "%d.%m.%Y %H:%M")
                data['ts'] = dt_obj.isoformat()
            except: data['ts'] = datetime.combine(user_date, user_time).isoformat()
    else: data['ts'] = datetime.combine(user_date, user_time).isoformat()

    ohlc_match = re.search(r'O ([\d,.]+) H ([\d,.]+) L ([\d,.]+) C ([\d,.]+)', text)
    if ohlc_match:
        data['open'] = parse_value_raw(ohlc_match.group(1))
        data['high'] = parse_value_raw(ohlc_match.group(2))
        data['low'] = parse_value_raw(ohlc_match.group(3))
        data['close'] = parse_value_raw(ohlc_match.group(4))
    else: data['open'] = data['high'] = data['low'] = data['close'] = 0.0

    data['volume'] = extract(r'V ([\d,.]+[MKB]?)', text)
    data['change_pct'] = extract(r'Change ([\d,.]+)%', text)
    data['amplitude_pct'] = extract(r'Amplitude [\d,.]+\(([\d,.]+)%\)', text)
    data['buy_volume'] = extract(r'Active Buy/Sell Volume.*?Buy ([+\-]?[\d,.]+[MKB]?)', text)
    data['sell_volume'] = abs(extract(r'Active Buy/Sell Volume.*?Sell ([+\-]?[\d,.]+[MKB]?)', text))
    data['abv_delta'] = extract(r'Active Buy/Sell Volume.*?Delta ([+\-]?[\d,.]+[MKB]?)', text)
    data['buy_trades'] = extract(r'Active Buy/Sell Trades.*?Buy ([+\-]?[\d,.]+[MKB]?)', text)
    data['sell_trades'] = abs(extract(r'Active Buy/Sell Trades.*?Sell ([+\-]?[\d,.]+[MKB]?)', text))
    data['trades_delta'] = extract(r'Active Buy/Sell Trades.*?Delta ([+\-]?[\d,.]+[MKB]?)', text)
    
    oi_match = re.search(r'Open Interest.*?O ([\d,.]+[MKB]?) H ([\d,.]+[MKB]?) L ([\d,.]+[MKB]?) C ([\d,.]+[MKB]?)', text, REGEX_FLAGS)
    if oi_match:
        data['oi_open'] = parse_value_raw(oi_match.group(1))
        data['oi_high'] = parse_value_raw(oi_match.group(2))
        data['oi_low'] = parse_value_raw(oi_match.group(3))
        data['oi_close'] = parse_value_raw(oi_match.group(4))
    
    data['liq_long'] = extract(r'Liquidation Long ([\d,.]+[MKB]?)', text)
    data['liq_short'] = abs(extract(r'Liquidation.*?Short ([+\-]?[\d,.]+[MKB]?)', text))
    
    # Coinglass fields... skipping for brevity as they are optional/robustly handled by regex in full App
    # But adding minimal placeholders if needed
    
    return data

def calculate_metrics(raw_data, config):
    m = raw_data.copy()
    m['range'] = m.get('high', 0) - m.get('low', 0)
    m['range_pct'] = (m['range'] / m['close'] * 100) if m.get('close') else 0
    m['body_pct'] = (abs(m.get('close', 0) - m.get('open', 0)) / m['range'] * 100) if m['range'] else 0
    if m['range'] == 0: m['clv_pct'] = 50.0
    else: m['clv_pct'] = (m.get('close', 0) - m.get('low', 0)) / m['range'] * 100
    if m['range'] == 0: m['upper_tail_pct'] = 0; m['lower_tail_pct'] = 0
    else:
        m['upper_tail_pct'] = (m.get('high', 0) - max(m.get('open', 0), m.get('close', 0))) / m['range'] * 100
        m['lower_tail_pct'] = (min(m.get('open', 0), m.get('close', 0)) - m.get('low', 0)) / m['range'] * 100
    m['price_sign'] = 1 if m.get('close', 0) >= m.get('open', 0) else -1
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
    if m['avg_trade_buy'] > 0: m['tilt_pct'] = ((m['avg_trade_sell'] / m['avg_trade_buy']) - 1) * 100
    else: m['tilt_pct'] = 0
    m['implied_price'] = (m.get('volume', 0) / total_active_vol) if total_active_vol else 0
    m['dpx'] = m['price_sign'] * m['cvd_sign']
    if m['dpx'] == 1: m['price_vs_delta'] = "match"
    elif m['dpx'] == -1: m['price_vs_delta'] = "div"
    else: m['price_vs_delta'] = "neutral"
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
    total_liq = m.get('liq_long', 0) + m.get('liq_short', 0)
    m['liq_share_pct'] = (total_liq / m.get('volume', 0) * 100) if m.get('volume', 0) else 0
    m['limb_pct'] = ((m.get('liq_short', 0) - m.get('liq_long', 0)) / total_liq * 100) if total_liq else 0
    m['liq_squeeze'] = m['liq_share_pct'] >= config['global_squeeze_limit']
    
    # Thresholds
    porog_df = config.get('porog_doi', pd.DataFrame())
    asset_coeffs = config.get('asset_coeffs', {})
    tf_params = config.get('tf_params', {})
    symbol_key = m.get('symbol_clean', '').lower()
    tf_key = m.get('tf', '4h')
    base_sens = 0.5
    coeff = 1.0
    if not porog_df.empty and symbol_key in porog_df.columns and 'timeframe' in porog_df.columns:
        row = porog_df.loc[porog_df['timeframe'] == tf_key]
        if not row.empty: base_sens = float(row[symbol_key].values[0])
    if m.get('symbol_clean') in asset_coeffs: coeff = asset_coeffs[m['symbol_clean']]
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

def save_candles_batch(candles_data):
    # ROBUST SAVE LOGIC
    if not candles_data: return True
    current_data = [c.copy() for c in candles_data]
    for row in current_data:
        if 'note' not in row: row['note'] = ""
    
    attempt = 0
    max_attempts = 20
    dropped_columns = []
    
    while attempt < max_attempts:
        try:
            supabase.table('candles').insert(current_data).execute()
            if dropped_columns:
                print(f"⚠️ Warning: Partially saved. Dropped columns: {', '.join(dropped_columns)}")
            return True
        except Exception as e:
            err_str = str(e)
            match = re.search(r"Could not find the '(\w+)' column", err_str)
            if match:
                bad_col = match.group(1)
                if bad_col not in dropped_columns:
                    dropped_columns.append(bad_col)
                    print(f"🕵️ Missing DB Column detected: '{bad_col}'. Removing...")
                    for row in current_data:
                        row.pop(bad_col, None)
                else:
                     print(f"❌ Loop Error on {bad_col}")
                     return False
                attempt += 1
            else:
                print(f"❌ DB Error: {e}")
                return False
    return False

# --- MAIN EXECUTION ---
def run_virtual():
    print("🚀 Starting Virtual Run...")
    
    input_text = "10.12.2025 9:00:00\tBinance · ETH USDT Perp · 4H O 3,325.64 H 3,374.93 L 3,295.56 C 3,316.75 V 2.57B Change -8.89(-0.27%) Amplitude 79.37(2.39%) Active Buy/Sell Volume Buy 297.44K Sell -289.25K Delta 8.19K Ratio 0.01 Active Buy/Sell Trades Buy 590.50K Sell -596.02K Delta -5.52K Ratio -0.00 Open Interest O 2.06M H 2.07M L 2.04M C 2.04M Liquidation Long 781.77K Short -1.14M"
    
    print("1. Loading Config...")
    config = load_configurations()
    if not config:
        print("❌ Config Load Failed")
        return

    print("2. Parsing Raw Input...")
    raw = parse_raw_input(input_text, datetime.now().date(), datetime.now().time())
    print(f"   Parsed TS: {raw.get('ts')}, Symbol: {raw.get('symbol_clean')}, Open: {raw.get('open')}")
    
    print("3. Calculating Metrics...")
    final_data = calculate_metrics(raw, config)
    print(f"   Calculated r: {final_data.get('r')}, porog: {final_data.get('porog_final')}")
    print(f"   Calculated r_strength: {final_data.get('r_strength')}")
    
    # Add dummy report texts because app requires them
    final_data['x_ray'] = "Virtual Report"
    final_data['x_ray_coinglass'] = "Virtual CG Report"
    final_data['raw_data'] = input_text
    
    # We need to wrap it in a list for batch save
    batch = [final_data]
    
    print("4. Saving to Database...")
    success = save_candles_batch(batch)
    
    if success:
        print("✅ SUCCESS! Virtual Run completed perfectly.")
    else:
        print("❌ FAILED to save to DB.")
        
    # Cleanup
    print("Cleaning up test data...")
    supabase.table('candles').delete().eq('raw_data', input_text).execute()
    print("Cleanup done.")

if __name__ == "__main__":
    run_virtual()
