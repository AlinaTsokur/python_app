
import re
from datetime import datetime

# --- HELPER FUNCTIONS FROM APP.PY ---
def parse_value_raw(val_str):
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
    match = re.search(regex, text, re.IGNORECASE | re.DOTALL)
    if match: return parse_value_raw(match.group(1))
    return 0.0

def parse_raw_input(text):
    data = {}
    data['raw_data'] = text.strip()
    REGEX_FLAGS = re.IGNORECASE | re.DOTALL

    # Meta
    header_match = re.search(r'(.+?) · (.+?) · (\w+)', text)
    data['exchange'] = header_match.group(1).strip() if header_match else 'Unknown'
    
    # OHLC
    ohlc_match = re.search(r'O\s+([\d,.]+)\s+H\s+([\d,.]+)\s+L\s+([\d,.]+)\s+C\s+([\d,.]+)', text)
    if ohlc_match:
        data['open'] = parse_value_raw(ohlc_match.group(1))
        data['high'] = parse_value_raw(ohlc_match.group(2))
        data['low'] = parse_value_raw(ohlc_match.group(3))
        data['close'] = parse_value_raw(ohlc_match.group(4))
        
    # Volume
    data['volume'] = extract(r'V ([\d,.]+[MKB]?)', text) # Note: Space after V
    
    # Active Volume
    data['buy_volume'] = extract(r'Active Buy/Sell Volume.*?Buy\s+([+\-]?[\d,.]+[MKB]?)', text)
    data['sell_volume'] = abs(extract(r'Active Buy/Sell Volume.*?Sell\s+([+\-]?[\d,.]+[MKB]?)', text))
    data['abv_delta'] = extract(r'Active Buy/Sell Volume.*?Delta\s+([+\-]?[\d,.]+[MKB]?)', text)
    
    # Open Interest
    oi_match = re.search(r'Open Interest.*?O ([\d,.]+[MKB]?) H ([\d,.]+[MKB]?) L ([\d,.]+[MKB]?) C ([\d,.]+[MKB]?)', text, REGEX_FLAGS)
    if oi_match:
        data['oi_open'] = parse_value_raw(oi_match.group(1))
        data['oi_close'] = parse_value_raw(oi_match.group(4))
        
    return data

def calculate_metrics(m):
    # CVD
    total_active_vol = m.get('buy_volume', 0) + m.get('sell_volume', 0)
    m['cvd_pct'] = (m.get('abv_delta', 0) / total_active_vol * 100) if total_active_vol else 0
    
    # DOI
    m['doi_pct'] = ((m.get('oi_close', 0) - m.get('oi_open', 0)) / m.get('oi_open', 0) * 100) if m.get('oi_open', 0) else 0
    
    return m

# --- TEST DATA ---
raw_binance = "10.12.2025 12:00:00	Binance · ETH USDT Perp · 1H O 2,962.88 H 2,994.91 L 2,933.29 C 2,949.78 V 920.53M Change -13.10(-0.44%) Amplitude 61.62(2.08%) Active Buy/Sell Volume Buy 144.17K Sell -166.59K Delta -22.42K Ratio -0.07 Active Buy/Sell Trades Buy 235.51K Sell -240.95K Delta -5.44K Ratio -0.01 Open Interest O 1.93M H 1.93M L 1.93M C 1.93M Liquidation Long 577.10K Short -144.46K"
raw_okx = "10.12.2025 12:00:00	OKX · ETH USDT Perp · 1H O 2,962.91 H 2,992.20 L 2,932.07 C 2,949.39 V 684.11M Change -13.52(-0.46%) Amplitude 60.13(2.03%) Active Buy/Sell Volume Buy 1.06M Sell -1.25M Delta -191.93K Ratio -0.08 Active Buy/Sell Trades Buy 79.10K Sell -72.33K Delta 6.77K Ratio 0.04 Open Interest O 540.17K H 540.17K L 529.88K C 532.94K Liquidation Long 249.90K Short -35.45K"
raw_bybit = "10.12.2025 12:00:00	Bybit · ETH USDT Perp · 1H O 2,962.59 H 2,990.00 L 2,933.10 C 2,950.33 V 310.04M Change -12.26(-0.41%) Amplitude 56.90(1.92%) Active Buy/Sell Volume Buy 52.39K Sell -52.20K Delta 193.24 Ratio 0.00 Active Buy/Sell Trades Buy 55.10K Sell -65.78K Delta -10.68K Ratio -0.09 Open Interest O 717.50K H 718.20K L 708.01K C 714.59K Liquidation Long 163.26K Short -510.79K"

# --- EXECUTE ---
for txt in [raw_binance, raw_okx, raw_bybit]:
    d = parse_raw_input(txt)
    m = calculate_metrics(d)
    print(f"Ex: {m['exchange']}")
    print(f"  Vol: {m['volume']}")
    print(f"  ActiveVol: Buy {m['buy_volume']} Sell {m['sell_volume']} Delta {m['abv_delta']}")
    print(f"  CVD%: {m['cvd_pct']}")
    print(f"  OI: O {m['oi_open']} C {m['oi_close']}")
    print(f"  DOI%: {m['doi_pct']}")
    print("-" * 20)
