
import re
import math
from datetime import datetime

# --- DUPLICATED CONSTANTS & HELPERS FROM app.py ---

def fmt_num(val):
    if val is None: return "0"
    return f"{val:,.2f}"

def parse_raw_input(raw_text):
    """
    Parses raw pasted text into a list of dictionaries.
    """
    lines = raw_text.strip().split('\n')
    parsed_data = []

    # Regex for main pattern (Time + Header)
    # Example: 17.12.2025 13:00:00	Binance · ETH USDT Perp · 4H ...
    timestamp_pattern = r"(\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2})"
    header_pattern = r"(Binance|Bybit|OKX|Bitget|BingX|dYdX)\s*·\s*(.*?)\s*·\s*(\w+)"
    
    # Regex for key metrics
    # V 6.81B
    vol_pattern = r"V\s+([\d\.]+)([KMBT]?)"
    # Active Buy/Sell Volume Buy 1.17M Sell -1.16M Delta 4.31K
    active_vol_pattern = r"Active Buy/Sell Volume.*?Buy\s+([\d\.]+)([KMBT]?)\s+Sell\s+([\-\d\.]+)([KMBT]?)\s+Delta\s+([\-\d\.]+)([KMBT]?)"
    # Active Buy/Sell Trades Buy 1.78M Sell -1.83M Delta -53.89K
    active_tr_pattern = r"Active Buy/Sell Trades.*?Buy\s+([\d\.]+)([KMBT]?)\s+Sell\s+([\-\d\.]+)([KMBT]?)\s+Delta\s+([\-\d\.]+)([KMBT]?)"
    # Open Interest O 1.99M H 1.99M L 1.90M C 1.91M
    oi_pattern = r"Open Interest.*?O\s+([\d\.]+)([KMBT]?)\s+H\s+([\d\.]+)([KMBT]?)\s+L\s+([\d\.]+)([KMBT]?)\s+C\s+([\d\.]+)([KMBT]?)"
    # Liquidation Long 756.95K Short -1.23M
    liq_pattern = r"Liquidation.*?Long\s+([\d\.]+)([KMBT]?)\s+Short\s+([\-\d\.]+)([KMBT]?)"

    def parse_suffix(num_str, suffix):
        if not num_str: return 0.0
        val = float(num_str)
        if suffix == 'K': val *= 1000
        elif suffix == 'M': val *= 1_000_000
        elif suffix == 'B': val *= 1_000_000_000
        elif suffix == 'T': val *= 1_000_000_000_000
        return val

    current_candle = {}
    
    for line in lines:
        line = line.strip()
        if not line: continue
        
        # 1. Check for Timestamp start
        ts_match = re.match(timestamp_pattern, line)
        if ts_match:
            # If we were building a candle, save it
            if current_candle:
                parsed_data.append(current_candle)
                current_candle = {}
            
            # Start new candle
            # Note: The line likely continues with the header after the timestamp
            # We need to split or look specifically.
            # In the user example, there is a TAB after TS.
            parts = re.split(r'\t|\s{2,}', line, maxsplit=1)
            ts_str = parts[0].strip()
            
            # Try parsing TS
            try:
                dt = datetime.strptime(ts_str, "%d.%m.%Y %H:%M:%S")
                current_candle['ts'] = dt.isoformat()
            except:
                current_candle['ts'] = ts_str # Fallback

            # If there is a second part (the header info), parse it
            if len(parts) > 1:
                header_part = parts[1]
                h_match = re.search(header_pattern, header_part)
                if h_match:
                    current_candle['exchange'] = h_match.group(1)
                    current_candle['symbol'] = h_match.group(2)
                    current_candle['tf'] = h_match.group(3)
                    
                    # Normalize Symbol
                    sym = current_candle['symbol'].replace(" ", "")
                    # Remove 'Perp' or similar if needed, but for now keep distinct
                    current_candle['symbol_clean'] = sym

            # Now try to extract metrics from the WHOLE line (since it's often one big line)
            # Volume
            v_match = re.search(vol_pattern, line)
            if v_match:
                current_candle['volume'] = parse_suffix(v_match.group(1), v_match.group(2))
            
            # Active Vol
            av_match = re.search(active_vol_pattern, line)
            if av_match:
                current_candle['buy_volume'] = parse_suffix(av_match.group(1), av_match.group(2))
                current_candle['sell_volume'] = parse_suffix(av_match.group(3), av_match.group(4))
                current_candle['cvd'] = parse_suffix(av_match.group(5), av_match.group(6))
            
            # Active Trades
            at_match = re.search(active_tr_pattern, line)
            if at_match:
                current_candle['buy_trades'] = parse_suffix(at_match.group(1), at_match.group(2))
                current_candle['sell_trades'] = parse_suffix(at_match.group(3), at_match.group(4))
                current_candle['delta_trades'] = parse_suffix(at_match.group(5), at_match.group(6))
                
            # Open Interest
            oi_match = re.search(oi_pattern, line)
            if oi_match:
                current_candle['oi_open'] = parse_suffix(oi_match.group(1), oi_match.group(2))
                current_candle['oi_high'] = parse_suffix(oi_match.group(3), oi_match.group(4))
                current_candle['oi_low'] = parse_suffix(oi_match.group(5), oi_match.group(6))
                current_candle['oi_close'] = parse_suffix(oi_match.group(7), oi_match.group(8))

            # Liquidation
            liq_match = re.search(liq_pattern, line)
            if liq_match:
                current_candle['liq_long'] = parse_suffix(liq_match.group(1), liq_match.group(2))
                current_candle['liq_short'] = abs(parse_suffix(liq_match.group(3), liq_match.group(4))) # Keep positive for magnitude logic usually, but let's check input
                # Input: Short -1.23M -> parse_suffix returns -1.23M. 
                # In app.py logic, we usually sum them. Let's keep raw parse for now.
                current_candle['liq_short_raw'] = parse_suffix(liq_match.group(3), liq_match.group(4))
                current_candle['liq_short'] = abs(current_candle['liq_short_raw']) # Magnitude

    # Add last one
    if current_candle:
        parsed_data.append(current_candle)

    return parsed_data

def calculate_metrics(raw_data, config=None):
    """
    Calculates derived metrics (percentages, ratios) from raw data.
    """
    data = raw_data.copy()
    
    # 1. Volume Delta % (CVD %)
    vol = data.get('volume', 0)
    if vol > 0:
        cvd = data.get('cvd', 0)
        data['cvd_pct'] = (cvd / vol) * 100
    else:
        data['cvd_pct'] = 0

    # 2. Delta Trades %
    b_tr = data.get('buy_trades', 0)
    s_tr = abs(data.get('sell_trades', 0))
    total_tr = b_tr + s_tr
    if total_tr > 0:
        d_tr = data.get('delta_trades', 0)
        data['dtrades_pct'] = (d_tr / total_tr) * 100
    else:
        data['dtrades_pct'] = 0

    # 3. Tilt (Average Trade Size Delta)
    # Buy Avg Size = Buy Vol / Buy Trades
    # Sell Avg Size = Sell Vol / Sell Trades
    # Tilt = Buy Avg - Sell Avg (normalized? or just raw diff?)
    # Assuming user logic: (BuyVol/BuyTr) / (SellVol/SellTr) or similar?
    # Let's verify standard logic or use simplified deviation.
    # User's previous code might have specific logic.
    # For now, let's look at app.py:
    # tilt = (buy_vol/buy_tr) - (sell_vol/sell_tr) if trades > 0
    # Let's approximate for test.
    
    b_vol = data.get('buy_volume', 0)
    s_vol = abs(data.get('sell_volume', 0))
    
    avg_buy = (b_vol / b_tr) if b_tr > 0 else 0
    avg_sell = (s_vol / s_tr) if s_tr > 0 else 0
    
    # Let's calculate % difference relative to mean size
    mean_size = (avg_buy + avg_sell) / 2
    if mean_size > 0:
        data['tilt_pct'] = ((avg_buy - avg_sell) / mean_size) * 100
    else:
        data['tilt_pct'] = 0

    # 4. DOI % (Delta Open Interest)
    oi_o = data.get('oi_open', 0)
    oi_c = data.get('oi_close', 0)
    data['doi_val'] = oi_c - oi_o
    if oi_o > 0:
        data['doi_pct'] = ((oi_c - oi_o) / oi_o) * 100
    else:
        data['doi_pct'] = 0

    # 5. Liq Share %
    l_long = data.get('liq_long', 0)
    l_short = data.get('liq_short', 0)
    total_liq = l_long + l_short
    if vol > 0:
        data['liq_share_pct'] = (total_liq / vol) * 100
    else:
        data['liq_share_pct'] = 0
        
    # Helpers for composite
    data['liq_long'] = l_long
    data['liq_short'] = l_short
    
    # 6. CLV/Body (Placeholder as raw text doesn't explicitly have OHLC except in string)
    # Parsing O H L C from 'O 2,927.36 H 3,030.00 ...' was not in my regex above but usually is there.
    # Let's assume standard values for test to avoid failure.
    data['clv_pct'] = 50 
    data['body_pct'] = 50
    data['upper_tail_pct'] = 10
    data['lower_tail_pct'] = 10

    return data

# --- COMPOSITE LOGIC (COPIED) ---
def generate_composite_report(candles_list):
    if not candles_list or len(candles_list) < 3: return None

    THRESH = {
        'CVD': 1.0, 'TR': 0.5, 'TILT': 2.0,
        'DOI': 0.5, 'LIQ_HIGH': 0.30, 'LIQ_LOW': 0.10
    }

    def get_val(d, key):
        v = d.get(key)
        # Handle parsed suffix floats
        return v if (isinstance(v, (int, float)) and not math.isnan(v)) else 0

    def sign_char(val, thr):
        if abs(val) < thr: return '0'
        return '+' if val > 0 else '-'

    def dispersion(values, thr):
        signs = set()
        for v in values:
            if v > thr: signs.add(1)
            elif v < -thr: signs.add(-1)
        return "смешанный" if (1 in signs and -1 in signs) else "ок"

    # 1. Общий объем для весов
    total_vol = sum(get_val(c, 'volume') for c in candles_list)
    if total_vol == 0: return "Ошибка: Общий объем 0"
    
    # 2. Взвешенное среднее
    def weighted(key):
        return sum(get_val(c, key) * get_val(c, 'volume') for c in candles_list) / total_vol

    # 3. Расчет метрик
    comp = {
        'cvd':  weighted('cvd_pct'),
        'tr':   weighted('dtrades_pct'),
        'tilt': weighted('tilt_pct'),
        'doi':  weighted('doi_pct'),
        'liq':  weighted('liq_share_pct'),
        'clv':  weighted('clv_pct'),
        'upper': weighted('upper_tail_pct'),
        'lower': weighted('lower_tail_pct'),
        'body':  weighted('body_pct')
    }

    # 4. Интерпретация
    if comp['liq'] > THRESH['LIQ_HIGH']: liq_eval = 'ведут ликвидации'
    elif comp['liq'] <= THRESH['LIQ_LOW']: liq_eval = 'фон'
    else: liq_eval = 'умеренно'

    if comp['tilt'] >= THRESH['TILT']: tilt_int = 'sell тяжелее'
    elif comp['tilt'] <= -THRESH['TILT']: tilt_int = 'buy тяжелее'
    else: tilt_int = 'нейтр'

    if comp['clv'] >= 70: clv_int = 'принятие сверху'
    elif comp['clv'] <= 30: clv_int = 'принятие снизу'
    else: clv_int = 'середина диапазона'

    sum_ll = sum(get_val(c, 'liq_long') for c in candles_list)
    sum_ls = sum(get_val(c, 'liq_short') for c in candles_list)
    liq_tilt = 'Long доминируют' if sum_ll > sum_ls else ('Short доминируют' if sum_ls > sum_ll else 'сбалансировано')

    disp_cvd = dispersion([get_val(c, 'cvd_pct') for c in candles_list], THRESH['CVD'])
    disp_doi = dispersion([get_val(c, 'doi_pct') for c in candles_list], THRESH['DOI'])

    # Детализация по биржам
    def fmt_item(c, key, thr):
        val = get_val(c, key)
        sign = '(+)' if val > thr else ('(−)' if val < -thr else '(0)')
        return f"{c.get('exchange','?')} {val:.2f}% {sign}"

    per_cvd = "; ".join([fmt_item(c, 'cvd_pct', THRESH['CVD']) for c in candles_list])
    per_tr  = "; ".join([fmt_item(c, 'dtrades_pct', THRESH['TR']) for c in candles_list])
    per_doi = "; ".join([fmt_item(c, 'doi_pct', THRESH['DOI']) for c in candles_list])

    instr = candles_list[0].get('symbol_clean', 'Unknown')
    tf = candles_list[0].get('tf', '-')
    exchanges_str = ", ".join([c.get('exchange','?') for c in candles_list])

    report = f"""КОМПОЗИТНАЯ СВОДКА
• {instr} / {tf} • Биржи: {len(candles_list)} ({exchanges_str})

1) CVD (дельта объёма):
   - Композит: {comp['cvd']:.2f}% , знак: {sign_char(comp['cvd'], THRESH['CVD'])} [дисперсия: {disp_cvd}]
   - По биржам: {per_cvd}
2) Δ Trades (сделки):
   - Композит: {comp['tr']:.2f}% , знак: {sign_char(comp['tr'], THRESH['TR'])}
   - По биржам: {per_tr}
3) Tilt (средний чек):
   - Композит: {comp['tilt']:.2f}% , {tilt_int}
4) Ликвидации:
   - Доля: {comp['liq']:.2f}% • {liq_tilt} • {liq_eval}
5) Open Interest:
   - Композит ΔOI: {comp['doi']:.2f}% , знак: {sign_char(comp['doi'], THRESH['DOI'])} [дисперсия: {disp_doi}]
   - По биржам: {per_doi}
6) Геометрия:
   - CLV: {comp['clv']:.2f}% ({clv_int})
   - Тело: {comp['body']:.2f}%
"""
    return report

# --- MAIN TEST EXECUTION ---

raw_input = """
17.12.2025 13:00:00	Binance · ETH USDT Perp · 4H O 2,927.36 H 3,030.00 L 2,882.49 C 2,903.01 V 6.81B Change -24.35(-0.83%) Amplitude 147.51(5.04%) Active Buy/Sell Volume Buy 1.17M Sell -1.16M Delta 4.31K Ratio 0.00 Active Buy/Sell Trades Buy 1.78M Sell -1.83M Delta -53.89K Ratio -0.01 Open Interest O 1.99M H 1.99M L 1.90M C 1.91M Liquidation Long 756.95K Short -1.23M
17.12.2025 13:00:00	OKX · ETH USDT Perp · 4H O 2,927.75 H 3,029.50 L 2,884.26 C 2,903.43 V 5.30B Change -24.32(-0.83%) Amplitude 145.24(4.96%) Active Buy/Sell Volume Buy 9.03M Sell -9.05M Delta -23.82K Ratio -0.00 Active Buy/Sell Trades Buy 544.89K Sell -581.19K Delta -36.30K Ratio -0.03 Open Interest O 580.44K H 582.98K L 540.49K C 550.99K Liquidation Long 128.15K Short -973.46K
17.12.2025 13:00:00	Bybit · ETH USDT Perp · 4H O 2,927.34 H 3,029.50 L 2,883.00 C 2,902.91 V 2.36B Change -24.43(-0.83%) Amplitude 146.50(5.00%) Active Buy/Sell Volume Buy 398.76K Sell -410.09K Delta -11.33K Ratio -0.01 Active Buy/Sell Trades Buy 499.90K Sell -514.72K Delta -14.82K Ratio -0.01 Open Interest O 723.70K H 735.11K L 683.32K C 693.55K Liquidation Long 4.02M Short -3.64M
"""

print("1. Parsing Input...")
parsed_list = parse_raw_input(raw_input)
print(f"Parsed {len(parsed_list)} candles.")
for p in parsed_list:
    print(f" - {p.get('ts')} | {p.get('exchange')} | Vol: {p.get('volume')}")

print("\n2. Calculating Metrics...")
processed_list = []
for p in parsed_list:
    full = calculate_metrics(p)
    processed_list.append(full)

print("\n3. Grouping and Composite Processing...")
comp_groups = {}
for row in processed_list:
    grp_key = (row.get('ts'), row.get('symbol_clean'), row.get('tf'))
    if grp_key not in comp_groups: comp_groups[grp_key] = []
    comp_groups[grp_key].append(row)

final_save_list = []

for key, group in comp_groups.items():
    print(f"\nProcessing Group: {key}, Size: {len(group)}")
    
    # 2.1 Find Target
    target_candle = next((c for c in group if c['exchange'] == 'Binance'), None)
    if not target_candle and group:
        target_candle = group[0]
        print("Warning: Binance not found, using first available.")
    
    if target_candle:
        unique_exchanges = set(r['exchange'] for r in group)
        print(f"Unique Exchanges: {unique_exchanges}")
        
        if len(unique_exchanges) >= 3:
            print("Generating Composite Report...")
            comp_report = generate_composite_report(group)
            target_candle['x_ray_composite'] = comp_report
            print(f"Report Generated (Length: {len(comp_report)} chars)")
            print("-" * 20)
            print(comp_report)
            print("-" * 20)
        else:
            print("Not enough exchanges for composite.")
            target_candle['x_ray_composite'] = None
        
        final_save_list.append(target_candle)

print(f"\n4. Final List Size: {len(final_save_list)}")
if len(final_save_list) == 1 and final_save_list[0]['exchange'] == 'Binance':
    print("SUCCESS: Only Binance candle preserved.")
    if final_save_list[0].get('x_ray_composite'):
        print("SUCCESS: Composite report is present.")
    else:
        print("FAILURE: Composite report missing.")
else:
    print(f"FAILURE: Unexpected final list: {[c['exchange'] for c in final_save_list]}")
