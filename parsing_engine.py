
import re
import math
from datetime import datetime, time
import pandas as pd

# --- 1. CORE HELPERS ---

def parse_value_raw(val_str):
    """Parses strings with K, M, B, %, commas into float."""
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
        if not clean_str: return None
        return round(float(clean_str) * multiplier, 2)
    except (ValueError, TypeError) as e:
        print(f"⚠️ Warning: Failed to parse '{val_str}': {e}")
        return None

def extract(regex, text):
    match = re.search(regex, text, re.IGNORECASE | re.DOTALL)
    if match: return parse_value_raw(match.group(1))
    return None

def fmt_num(val, decimals=2, is_pct=False):
    if val is None: return "−"
    if isinstance(val, bool): return "true" if val else "false"
    if isinstance(val, (int, float)):
        s = f"{val:,.{decimals}f}".replace(",", " ").replace(".", ",")
        if is_pct: s += "%"
        return s
    return str(val)

# --- 2. RAW INPUT PARSING ---

def parse_raw_input(text):
    """Parses raw text into a Raw Input Dictionary."""
    data = {}
    data['raw_data'] = text.strip()
    REGEX_FLAGS = re.IGNORECASE | re.DOTALL

    # Meta
    # Fix: Anchor to start and consume timestamp to avoid capturing it in exchange name
    # Matches: DD.MM.YYYY HH:MM[:SS] Exchange · Symbol · TF
    header_match = re.search(r'^\s*\d{1,2}\.\d{1,2}\.\d{4}\s+\d{1,2}:\d{2}(?::\d{2})?\s+(.+?)\s+·\s+(.+?)\s+·\s+(\w+)', text)
    if header_match:
        data['exchange'] = header_match.group(1).strip()
        data['raw_symbol'] = header_match.group(2).strip()
        data['tf'] = header_match.group(3).strip()
    else:
        data['exchange'] = None
        data['raw_symbol'] = None
        data['tf'] = None
    
    if data['raw_symbol']:
        data['symbol_clean'] = data['raw_symbol'].split(' ')[0].replace('USDT', '').replace('PERP', '')
    else:
        data['symbol_clean'] = None
    
    # TS
    # TS
    # Capture parts to normalize single-digit hours
    ts_match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?', text)
    if ts_match:
        try:
            day, month, year, hour, minute, second = ts_match.groups()
            # Normalize to 2-digit components
            ts_norm = f"{int(day):02d}.{int(month):02d}.{year} {int(hour):02d}:{minute}"
            if second:
                ts_norm += f":{second}"
                fmt = "%d.%m.%Y %H:%M:%S"
            else:
                fmt = "%d.%m.%Y %H:%M"
            
            dt_obj = datetime.strptime(ts_norm, fmt)
            data['ts'] = dt_obj.isoformat()
            data['parsed_ts'] = data['ts'] 
        except Exception as e:
             # If strict parsing fails, don't crash, but don't fake data
             print(f"⚠️ Warning: Timestamp parse failed: {e}")
             data['ts'] = None
    else:
        pass

    # OHLC
    ohlc_match = re.search(r'O\s+([\d,.]+)\s+H\s+([\d,.]+)\s+L\s+([\d,.]+)\s+C\s+([\d,.]+)', text)
    if ohlc_match:
        data['open'] = parse_value_raw(ohlc_match.group(1))
        data['high'] = parse_value_raw(ohlc_match.group(2))
        data['low'] = parse_value_raw(ohlc_match.group(3))
        data['close'] = parse_value_raw(ohlc_match.group(4))
    else:
        # Strict: Do NOT zero-fill if OHLC is missing. 
        # Missing data should remain None so explicit checks fail safely.
        data['open'] = None
        data['high'] = None
        data['low'] = None
        data['close'] = None
    
    # Volume
    data['volume'] = extract(r'V ([\d,.]+[MKB]?)', text)
    
    # Change & Amplitude
    ch_match = re.search(r'Change\s+([+\-]?[\d,.]+)\s*\(([+\-]?[\d,.]+)%\)', text, REGEX_FLAGS)
    if ch_match:
        data['change_abs'] = parse_value_raw(ch_match.group(1))
        data['change_pct'] = parse_value_raw(ch_match.group(2))
    else:
        data['change_abs'] = extract(r'Change\s+([+\-]?[\d,.]+)', text)
        data['change_pct'] = extract(r'Change.*?([+\-]?[\d,.]+)%', text)

    amp_match = re.search(r'Amplitude\s+([\d,.]+)\s*\(([\d,.]+)%\)', text, REGEX_FLAGS)
    if amp_match:
        data['amplitude_abs'] = parse_value_raw(amp_match.group(1))
        data['amplitude_pct'] = parse_value_raw(amp_match.group(2))
    else:
        data['amplitude_abs'] = extract(r'Amplitude\s+([\d,.]+)', text)
        data['amplitude_pct'] = extract(r'Amplitude.*?([\d,.]+)%', text)
    
    # Active Vol
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

    # OI
    oi_match = re.search(r'Open Interest.*?O ([\d,.]+[MKB]?) H ([\d,.]+[MKB]?) L ([\d,.]+[MKB]?) C ([\d,.]+[MKB]?)', text, REGEX_FLAGS)
    if oi_match:
        data['oi_open'] = parse_value_raw(oi_match.group(1))
        data['oi_high'] = parse_value_raw(oi_match.group(2))
        data['oi_low'] = parse_value_raw(oi_match.group(3))
        data['oi_close'] = parse_value_raw(oi_match.group(4))

    # Liq
    data['liq_long'] = extract(r'Liquidation Long ([\d,.]+[MKB]?)', text)
    data['liq_short'] = extract(r'Liquidation.*?Short ([+\-]?[\d,.]+[MKB]?)', text)
    if data['liq_short'] is not None: data['liq_short'] = abs(data['liq_short'])

    # Coinglass Extras
    fr_match = re.search(r'(?<!Aggregated )Funding Rate.*?O ([+\-]?[\d,.]+%?).*?H ([+\-]?[\d,.]+%?).*?L ([+\-]?[\d,.]+%?).*?C ([+\-]?[\d,.]+%?)', text, REGEX_FLAGS)
    if fr_match:
        data['fr_open'] = parse_value_raw(fr_match.group(1))
        data['fr_high'] = parse_value_raw(fr_match.group(2))
        data['fr_low'] = parse_value_raw(fr_match.group(3))
        data['fr_close'] = parse_value_raw(fr_match.group(4))
    
    agg_fr_match = re.search(r'Aggregated Funding Rate.*?O ([+\-]?[\d,.]+%?).*?H ([+\-]?[\d,.]+%?).*?L ([+\-]?[\d,.]+%?).*?C ([+\-]?[\d,.]+%?)', text, REGEX_FLAGS)
    if agg_fr_match:
        data['agg_fr_open'] = parse_value_raw(agg_fr_match.group(1))
        data['agg_fr_high'] = parse_value_raw(agg_fr_match.group(2))
        data['agg_fr_low'] = parse_value_raw(agg_fr_match.group(3))
        data['agg_fr_close'] = parse_value_raw(agg_fr_match.group(4))

    data['basis'] = extract(r'Basis\s+([+\-]?[\d,.]+)', text)

    ls_match = re.search(r'Long/Short Ratio.*?O ([+\-]?[\d,.]+).*?H ([+\-]?[\d,.]+).*?L ([+\-]?[\d,.]+).*?C ([+\-]?[\d,.]+)', text, REGEX_FLAGS)
    if ls_match:
        data['ls_ratio_open'] = parse_value_raw(ls_match.group(1))
        data['ls_ratio_high'] = parse_value_raw(ls_match.group(2))
        data['ls_ratio_low'] = parse_value_raw(ls_match.group(3))
        data['ls_ratio_close'] = parse_value_raw(ls_match.group(4))

    idx_match = re.search(r'Index Price.*?O ([\d,.]+).*?H ([\d,.]+).*?L ([\d,.]+).*?C ([\d,.]+)', text, REGEX_FLAGS)
    if idx_match:
        data['idx_open'] = parse_value_raw(idx_match.group(1))
        data['idx_high'] = parse_value_raw(idx_match.group(2))
        data['idx_low'] = parse_value_raw(idx_match.group(3))
        data['idx_close'] = parse_value_raw(idx_match.group(4))

    nl_match = re.search(r'Net Longs.*?O ([+\-]?[\d,.]+[MKB]?).*?C ([+\-]?[\d,.]+[MKB]?).*?(?:Delta|Δ) ([+\-]?[\d,.]+[MKB]?)', text, REGEX_FLAGS)
    if nl_match:
        data['net_longs_open'] = parse_value_raw(nl_match.group(1))
        data['net_longs_close'] = parse_value_raw(nl_match.group(2))
        data['net_longs_delta'] = parse_value_raw(nl_match.group(3))

    ns_match = re.search(r'Net Shorts.*?O ([+\-]?[\d,.]+[MKB]?).*?C ([+\-]?[\d,.]+[MKB]?).*?(?:Delta|Δ) ([+\-]?[\d,.]+[MKB]?)', text, REGEX_FLAGS)
    if ns_match:
        data['net_shorts_open'] = parse_value_raw(ns_match.group(1))
        data['net_shorts_close'] = parse_value_raw(ns_match.group(2))
        data['net_shorts_delta'] = parse_value_raw(ns_match.group(3))

    liq_match = re.search(r'Liquidation.*?Long ([+\-]?[\d,.]+[MKB]?).*?Short ([+\-]?[\d,.]+[MKB]?)', text, REGEX_FLAGS)
    if liq_match:
        data['liq_long'] = abs(parse_value_raw(liq_match.group(1)))
        data['liq_short'] = abs(parse_value_raw(liq_match.group(2)))
    
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
        # Strict: Do NOT zero-fill. Leave as None.

    return data

# --- 3. METRIC CALCULATION ---

def calculate_metrics(raw_data, config):
    """Calculates all metrics (Geometry, CVD, OI Flags)."""
    m = raw_data.copy()
    if not config: config = {}
    
    # 1. Geometry
    o_px = m.get('open')
    c_px = m.get('close')
    h_px = m.get('high')
    l_px = m.get('low')

    m['range'] = None
    m['range_pct'] = None
    m['body_pct'] = None
    m['clv_pct'] = None
    m['upper_tail_pct'] = None
    m['lower_tail_pct'] = None

    if all(x is not None for x in [o_px, c_px, h_px, l_px]):
        rng = h_px - l_px
        m['range'] = rng
        m['range_pct'] = (rng / c_px * 100) if c_px else 0
        
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
            
        m['price_sign'] = 1 if c_px >= o_px else -1
    else:
         m['price_sign'] = 0 # Neutral if no data

    m['price_vs_delta'] = "neutral" 

    # 2. Volume & Trades Metrics
    buy_vol = m.get('buy_volume')
    sell_vol = m.get('sell_volume')
    
    total_active_vol = (buy_vol or 0) + (sell_vol or 0)
    
    if m.get('abv_delta') is not None and total_active_vol > 0:
        m['cvd_pct'] = (m.get('abv_delta') / total_active_vol * 100)
    else:
        m['cvd_pct'] = None
    
    delta = m.get('abv_delta')
    if delta is None or delta == 0:
        m['cvd_sign'] = 0
    else:
        m['cvd_sign'] = 1 if delta > 0 else -1
    m['cvd_small'] = abs(m['cvd_pct'] or 0) < 1.0 

    b_trades = m.get('buy_trades')
    s_trades = m.get('sell_trades')
    
    if b_trades is not None and s_trades is not None:
        m['trades_delta'] = b_trades - s_trades 
        total_trades = b_trades + s_trades
        m['dtrades_pct'] = (m['trades_delta'] / total_trades * 100) if total_trades else 0
    else:
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

    # 3. Open Interest
    oo = m.get('oi_open')
    oc = m.get('oi_close')
    
    if oo is not None and oc is not None and oo != 0:
         m['doi_pct'] = ((oc - oo) / oo * 100)
    else:
         m['doi_pct'] = None
    
    oi_rng = m.get('oi_high', 0) - m.get('oi_low', 0)
    if oi_rng == 0: m['oipos'] = 0.5
    else:
        raw_pos = (m.get('oi_close', 0) - m.get('oi_low', 0)) / oi_rng
        m['oipos'] = max(0.0, min(1.0, raw_pos))

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
    if (c_pct == 0 or c_pct is None) and m.get('change_abs') and m.get('close'):
         c_pct = abs(m['change_abs']) / m['close'] * 100 * (1 if m.get('price_sign', 1) == 1 else -1)
         m['change_pct'] = c_pct

    if m.get('doi_pct') is not None and c_pct:
        m['oe'] = abs(m['doi_pct']) / abs(c_pct)
    else:
        m['oe'] = None

    # 4. Liquidations
    liq_l = m.get('liq_long')
    liq_s = m.get('liq_short')
    total_liq = None
    
    if liq_l is not None and liq_s is not None:
        total_liq = liq_l + liq_s
        m['liq_share_pct'] = (total_liq / m.get('volume', 0) * 100) if m.get('volume', 0) else 0
        m['limb_pct'] = ((liq_s - liq_l) / total_liq * 100) if total_liq else 0
    else:
        m['liq_share_pct'] = None
        m['limb_pct'] = None
        
    m['liq_threshold'] = config.get('global_squeeze_limit', 0.30)
    m['liq_squeeze'] = (m['liq_share_pct'] >= m['liq_threshold']) if m.get('liq_share_pct') is not None else False

    # 5. Dominant Reject
    LT, UT, Body, CLV = m['lower_tail_pct'], m['upper_tail_pct'], m['body_pct'], m['clv_pct']
    dr = None
    
    if all(x is not None for x in [LT, UT, Body, CLV]):
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
    
    symbol_key_lower = str(m.get('symbol_clean') or '').lower()
    symbol_key_upper = str(m.get('symbol_clean') or '').upper()
    tf_val = str(m.get('tf') or '4h')
    
    base_sens = 0.5
    coeff = 1.0
    
    if not porog_df.empty and symbol_key_lower in porog_df.columns and 'timeframe' in porog_df.columns:
        try:
            mask = porog_df['timeframe'].astype(str).str.lower() == tf_val.lower()
            row = porog_df.loc[mask]
            if not row.empty:
                base_sens = float(row[symbol_key_lower].values[0])
        except Exception:
            pass
            
    if symbol_key_upper in asset_coeffs:
        coeff = asset_coeffs[symbol_key_upper]
        
    m['porog_final'] = base_sens * coeff
    m['epsilon'] = 0.33 * m['porog_final']
    m['oi_in_sens'] = abs(m['doi_pct'] or 0) <= m['porog_final']
    
    k_set, k_ctr, k_unl = 1.0, 1.0, 1.0
    tf_sens_base = None 
    
    tf_data = tf_params.get(tf_val)
    if not tf_data:
        for k_tf, v_data in tf_params.items():
            if str(k_tf).lower() == tf_val.lower():
                tf_data = v_data; break
    
    if tf_data:
        k_set = float(tf_data.get('k_set', 1.0))
        k_ctr = float(tf_data.get('k_ctr', 1.0))
        k_unl = float(tf_data.get('k_unl', 1.0))
        if 'sens' in tf_data:
            tf_sens_base = float(tf_data['sens'])

    if tf_sens_base is not None:
        t_base = tf_sens_base 
        m['t_set_pct'] = round(t_base * k_set, 2)
        m['oi_set'] = (m['doi_pct'] or 0) >= m['t_set_pct']
        m['t_counter_pct'] = round(t_base * k_ctr, 2)
        m['oi_counter'] = (m['dpx'] == -1) and ((m['doi_pct'] or 0) >= m['t_counter_pct'])
        m['t_unload_pct'] = round(-(t_base * k_unl), 2)
        m['oi_unload'] = (m['doi_pct'] or 0) <= m['t_unload_pct']
        m['tf_sens'] = tf_sens_base
    else:
        m['t_set_pct'] = None; m['oi_set'] = None
        m['t_counter_pct'] = None; m['oi_counter'] = None
        m['t_unload_pct'] = None; m['oi_unload'] = None
        m['tf_sens'] = None
    
    m['r_strength'] = abs(m['doi_pct'] or 0) / m['porog_final'] if m['porog_final'] else 0
    m['r'] = m['r_strength']
    
    return m
