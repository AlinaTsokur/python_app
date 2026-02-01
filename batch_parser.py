import re
import math
from datetime import datetime
import pandas as pd
from parsing_engine import parse_raw_input, calculate_metrics
from core.report_generator import generate_full_report

# --- 3. STATS CALCULATION (Architecture V5) ---
def calculate_stats_agg(candles):
    """Calculates aggregated STATS for a segment of candles."""
    if not candles: return {}
    
    c_first = candles[0]
    c_last = candles[-1]
    
    # Strict Sums Logic
    vols = [c.get('volume') for c in candles]
    cvds = [c.get('cvd_pct') for c in candles]
    liq_ls = [c.get('liq_long') for c in candles]
    liq_ss = [c.get('liq_short') for c in candles]
    
    # We rely on local_warnings being initialized later in the function?
    # No, we need it now. 
    # BUT existing code initializes output near return. 
    # Logic flow:
    # 1. Init warnings list at start of function.
    local_warnings = []

    if any(v is None for v in vols):
        sum_vol = None
        local_warnings.append("⚠️ Warning: Missing volume in candles")
    else:
        sum_vol = sum(vols)

    if any(c is None for c in cvds):
        sum_cvd = None
        local_warnings.append("⚠️ Warning: Missing cvd_pct in candles")
    else:
        sum_cvd = sum(cvds)

    if any(l is None for l in liq_ls):
        sum_liq_L = None
        local_warnings.append("⚠️ Warning: Missing liq_long in candles -> sum_liq_long=None, liq_ratio cannot be computed")
    else:
        sum_liq_L = sum(abs(l) for l in liq_ls)

    if any(s is None for s in liq_ss):
        sum_liq_S = None
        local_warnings.append("⚠️ Warning: Missing liq_short in candles -> sum_liq_short=None, liq_ratio cannot be computed")
    else:
        sum_liq_S = sum(abs(s) for s in liq_ss)
    
    start_px = c_first.get('open')
    end_px = c_last.get('close')
    
    body_rng_pct = None

    if start_px is None or end_px is None:
        local_warnings.append("⚠️ Warning: Missing open/close -> start/end_price=None, body_range_pct cannot be computed")
    else:
        # Body Range Pct
        # Need global High/Low of the segment
        highs = [c.get("high") for c in candles if c.get("high") is not None]
        lows  = [c.get("low")  for c in candles if c.get("low")  is not None]

        if not highs or not lows:
             local_warnings.append("⚠️ Warning: Missing high/low in candles -> body_range_pct cannot be computed")
             seg_high = None
             seg_low = None
        else:
             seg_high = max(highs)
             seg_low  = min(lows)
        
        seg_range = (seg_high - seg_low) if (seg_high is not None and seg_low is not None) else 0
        
        body_rng_pct = 0.0 # Default if range is 0 but data exists
        if seg_range > 0:
            body_rng_pct = abs(end_px - start_px) / seg_range * 100.0
        elif seg_high is None: # If missing data, explicit None
            body_rng_pct = None
        
    # Tail logic (already has local_warnings usage)
    # Ensure we don't overwrite local_warnings from line 15
    # Remove 'local_warnings = []' from strict tail logic block if it exists
    uppers = [c.get('upper_tail_pct') for c in candles]
    lowers = [c.get('lower_tail_pct') for c in candles]
    
    # local_warnings already init at top
    
    if any(u is None for u in uppers):
        avg_upper = None
        local_warnings.append("⚠️ Warning: Missing upper_tail_pct in candles, avg set to None")
    else:
        avg_upper = sum(uppers) / len(candles)

    if any(l is None for l in lowers):
        avg_lower = None
        local_warnings.append("⚠️ Warning: Missing lower_tail_pct in candles, avg set to None")
    else:
        avg_lower = sum(lowers) / len(candles)
    
    # Strict Net OI Logic
    oi_close = c_last.get("oi_close")
    oi_open  = c_first.get("oi_open")
    net_oi = None if (oi_close is None or oi_open is None) else (oi_close - oi_open)

    # Liquidity Dominance (Strict Logic - Deterministic)
    liq_ratio = None
    if sum_liq_L is not None and sum_liq_S is not None:
        if sum_liq_S > 0:
            liq_ratio = sum_liq_L / sum_liq_S
        elif sum_liq_L == 0:  # and sum_liq_S == 0
            liq_ratio = 1.0
        else:
            # sum_liq_S == 0 but sum_liq_L > 0
            liq_ratio = None
            local_warnings.append("⚠️ Warning: liq_dominance_ratio undefined (sum_liq_S=0, sum_liq_L>0)")

    stats = {
        "candles_count": len(candles),
        "sum_volume": sum_vol,
        "sum_cvd_pct": sum_cvd,
        "sum_liq_long": sum_liq_L,
        "sum_liq_short": sum_liq_S,
        "net_oi_change": net_oi,
        "start_price": start_px,
        "end_price": end_px,
        "avg_upper_tail_pct": avg_upper,
        "avg_lower_tail_pct": avg_lower,
        "liq_dominance_ratio": liq_ratio,
        "body_range_pct": body_rng_pct
    }
    return stats, local_warnings

# --- 4. DATA SAVING LOGIC (Step 1.2) ---

def save_to_candles(supabase, candles_list):
    """
    Saves parsed candles to 'candles' table.
    Uses 'id' based on exchange+symbol+tf+ts to handle upserts.
    """
    if not candles_list: return 0
    
    seen_keys = set()
    data_for_upsert = []
    
    for c in candles_list:
        # Generate composite key for local deduplication
        # (Must match the UNIQUE constraint in DB: ts, symbol_clean, tf, exchange)
        clean_s = c.get('symbol_clean')
        ts_val = c.get('ts')
        tf_val = c.get('tf')
        exch_val = c.get('exchange')
        
        unique_key = (ts_val, clean_s, tf_val, exch_val)
        
        if unique_key in seen_keys:
            continue
        seen_keys.add(unique_key)
        
        # Safe raw_data truncation
        raw_val = c.get('raw_data')
        raw_truncated = raw_val[:1000] if (raw_val and isinstance(raw_val, str)) else None

        # Prepare row consistent with DB schema
        row = {
            "ts": ts_val,
            "symbol_clean": clean_s, # FIX: key matches DB column
            "raw_symbol": c.get('raw_symbol'),     # FIX: added column
            "exchange": exch_val,
            "tf": tf_val,
            "open": c.get('open'),
            "high": c.get('high'),
            "low": c.get('low'),
            "close": c.get('close'),
            "volume": c.get('volume'),
            "buy_volume": c.get('buy_volume'),
            "sell_volume": c.get('sell_volume'),
            "abv_delta": c.get('abv_delta'),
            "abv_ratio": c.get('abv_ratio'),
            "buy_trades": c.get('buy_trades'),
            "sell_trades": c.get('sell_trades'),
            "trades_delta": c.get('trades_delta'),
            "trades_ratio": c.get('trades_ratio'),
            "oi_open": c.get('oi_open'),
            "oi_high": c.get('oi_high'),
            "oi_low": c.get('oi_low'),
            "oi_close": c.get('oi_close'),
            "liq_long": c.get('liq_long'),
            "liq_short": c.get('liq_short'),
            "change_abs": c.get('change_abs'),
            "change_pct": c.get('change_pct'),
            "amplitude_abs": c.get('amplitude_abs'),
            "amplitude_pct": c.get('amplitude_pct'),
            "cvd_pct": c.get('cvd_pct'),
            "cvd_sign": c.get('cvd_sign'),
            "cvd_small": c.get('cvd_small'),
            "dtrades_pct": c.get('dtrades_pct'),
            "ratio_stable": c.get('ratio_stable'),
            "avg_trade_buy": c.get('avg_trade_buy'),
            "avg_trade_sell": c.get('avg_trade_sell'),
            "tilt_pct": c.get('tilt_pct'),
            "implied_price": c.get('implied_price'),
            "dpx": c.get('dpx'),
            "price_vs_delta": c.get('price_vs_delta'),
            "doi_pct": c.get('doi_pct'),
            "oipos": c.get('oipos'),
            "oi_path": c.get('oi_path'),
            "oe": c.get('oe'),
            "liq_share_pct": c.get('liq_share_pct'),
            "limb_pct": c.get('limb_pct'),
            "liq_squeeze": c.get('liq_squeeze'),
            "porog_final": c.get('porog_final'),
            "r": c.get('r'),
            "epsilon": c.get('epsilon'),
            "oi_in_sens": c.get('oi_in_sens'),
            "t_set_pct": c.get('t_set_pct'),
            "t_counter_pct": c.get('t_counter_pct'),
            "t_unload_pct": c.get('t_unload_pct'),
            "oi_set": c.get('oi_set'),
            "oi_counter": c.get('oi_counter'),
            "oi_unload": c.get('oi_unload'),
            "r_strength": c.get('r_strength'),
            "dominant_reject": c.get('dominant_reject'),
            "price_sign": c.get('price_sign'),
            "range": c.get('range'),
            "range_pct": c.get('range_pct'),
            "body_pct": c.get('body_pct'),
            "clv_pct": c.get('clv_pct'),
            "upper_tail_pct": c.get('upper_tail_pct'),
            "lower_tail_pct": c.get('lower_tail_pct'),
            "x_ray": c.get('x_ray'), 
            "raw_data": raw_truncated
        }
        
        # Filter None
        row = {k: v for k, v in row.items() if v is not None}
        data_for_upsert.append(row)

    count = 0
    if data_for_upsert:
        # Upsert: on_conflict adjusted to match actual columns
        try:
            res = supabase.table("candles").upsert(
                data_for_upsert, 
                on_conflict="ts,symbol_clean,tf,exchange"
            ).execute()
            count = len(res.data) if res.data else 0
        except Exception as e:
            print(f"Candle Save Error: {e}")
            raise e
            
    return count

def save_to_segments(supabase, segments_list):
    """
    Saves T2.1 Segments to 'segments' table.
    Schema:
      id, created_at (auto)
      exchange, symbol, tf
      ts_start, ts_end
      y_dir, y_size
      data (jsonb)
    """
    if not segments_list: return 0
    
    rows = []
    for s in segments_list:
        meta = s.get('META', {})
        imp = s.get('IMPULSE', {})
        ctx = s.get('CONTEXT', {})
        candles = ctx.get('DATA', [])
        
        # Prepare DATA blob (META + CONTEXT)
        data_blob = {
            "META": meta,
            "CONTEXT": ctx
        }
        
        # Extract Timestamps
        t_start = None
        t_end = None
        if candles:
            # Assumes candles are sorted by time in parse_batch_with_labels
            t_start = candles[0].get('ts')
            t_end = candles[-1].get('ts')
        
        row = {
            "exchange": meta.get('exchange'),
            "symbol": meta.get('symbol'),
            "tf": meta.get('tf'),
            "ts_start": t_start,
            "ts_end": t_end,
            "y_dir": imp.get('y_dir'),
            "y_size": imp.get('y_size'),
            "data": data_blob
        }
        rows.append(row)
        
    count = 0
    if rows:
        try:
            res = supabase.table("segments").insert(rows).execute()
            count = len(res.data) if res.data else 0
        except Exception as e:
            print(f"Segment Save Error: {e}")
            return 0
            
    return count

# --- 5. BATCH PARSER WITH LABELS (Original Function) ---
def parse_batch_with_labels(full_text, config=None):
    # ... (Same as before)
    segments = []
    all_candles = []
    warnings = []
    
    LABEL_REGEX = r'(Weak|Medium|Strong)\s+(Up|Down)'
    labels_iter = list(re.finditer(LABEL_REGEX, full_text, re.IGNORECASE))
    
    # --- VALIDATION: Check for incomplete labels ---
    # Find lines that contain ONLY keywords but didn't match the strict regex
    SUSPECT_REGEX = r'(?m)^\s*(Weak|Medium|Strong|Up|Down)\s*$'
    suspect_iter = re.finditer(SUSPECT_REGEX, full_text, re.IGNORECASE)
    
    # Create mask of valid label ranges
    valid_ranges = [(m.start(), m.end()) for m in labels_iter]
    
    for sm in suspect_iter:
        s_start, s_end = sm.start(), sm.end()
        
        # Check if matched text is part of a valid label range (e.g. "Medium" inside "Medium\nUp")
        # Relaxed check: if suspect overlaps ANY valid range, it's covered.
        is_covered = False
        for v_start, v_end in valid_ranges:
            # Overlap logic: start < end and end > start
            if s_start < v_end and s_end > v_start:
                is_covered = True
                break
        
        if not is_covered:
            warnings.append(f"⚠️ POTENTIAL ERROR: Found incomplete label '{sm.group(1)}' at char {s_start}. Expected format: 'Strength Direction' (e.g. 'Strong Up'). This segment might be parsed incorrectly.")
    # -----------------------------------------------
    labels_iter = list(re.finditer(LABEL_REGEX, full_text, re.IGNORECASE))
    current_pos = 0
    
    for match in labels_iter:
        label_start = match.start()
        label_end = match.end()
        
        # Strength (Group 1) and Direction (Group 2)
        strength = match.group(1).title() # e.g. "Strong"
        direction = match.group(2).upper() # e.g. "UP"
        
        chunk_text = full_text[current_pos:label_start].strip()
        
        if not chunk_text:
            warnings.append(f"Label {strength} {direction} at char {label_start} has no preceding candles.")
        else:
            # Corrected Split Regex (by Timestamp)
            # Support single-digit hour (e.g. 0:00:00, 4:00:00)
            raw_candles = re.split(r'(?m)^(?=\d{2}\.\d{2}\.\d{4}\s+\d{1,2}:\d{2}:\d{2})', chunk_text)
            raw_candles = [c.strip() for c in raw_candles if c.strip()]
            
            parsed_segment_candles = []
            
            for rc in raw_candles:
                base = parse_raw_input(rc)
                if not base.get('open'): 
                    if len(rc) > 20: warnings.append(f"Failed to parse candle: {rc[:50]}...")
                    continue
                
                full = calculate_metrics(base, config)
                # Need to generate X-Ray for consistency if saving to 'candles'
                # Attempt to generate X-Ray for ALL candles (safe wrapper)
                try:
                    full['x_ray'] = generate_full_report(full) if full else None
                except Exception as e:
                    # Log warning but don't crash
                    print(f"Warning: Failed to generate x_ray: {e}") 
                    full['x_ray'] = None
                
                parsed_segment_candles.append(full)
            
            if parsed_segment_candles:
                # Safe sort: Handle None if strict parsing failed to avoid crash
                parsed_segment_candles.sort(key=lambda x: (x.get('ts') or ''))
                all_candles.extend(parsed_segment_candles)
                
                # Use Tuple Return (Variant A)
                stats, stats_warnings = calculate_stats_agg(parsed_segment_candles)
                
                # Extend main warnings cleanly
                if stats_warnings:
                    warnings.extend(stats_warnings)

                first = parsed_segment_candles[0]
                meta = {
                    "symbol": first.get('symbol_clean'), 
                    "tf": first.get('tf'),
                    "exchange": first.get('exchange'),
                    "total_candles": len(parsed_segment_candles),
                    "impulse_split_index": len(parsed_segment_candles) - 1
                }
                
                segment_obj = {
                    "META": meta,
                    "CONTEXT": {
                        "STATS": stats,
                        "DATA": parsed_segment_candles
                    },
                    "IMPULSE": {
                        "y_dir": direction,
                        "y_size": strength
                    }
                }
                segments.append(segment_obj)
            else:
                warnings.append(f"Label {strength} {direction} had valid text but no valid candles parsed.")
        
        current_pos = label_end
        
    tail_text = full_text[current_pos:].strip()
    if tail_text:
         tail_chunks = re.split(r'(?=(?:Binance|Bybit|OKX)\s+·)', tail_text, flags=re.IGNORECASE)
         tail_chunks = [c for c in tail_chunks if c.strip()]
         if tail_chunks:
             warnings.append(f"Found {len(tail_chunks)} candles at the end WITHOUT a label. They were ignored.")

    return segments, all_candles, warnings

def save_batch_transactionally(supabase, segments_list, candles_list):
    """
    Supabase handles transactions server-side if RLS is configured.
    Just do the inserts without manual rollback.
    """
    try:
        seg_count = 0
        candles_count = 0
        
        # 1. Save Segments
        if segments_list:
            print("Saving segments...")
            rows = []
            for s in segments_list:
                meta = s.get('META', {})
                imp = s.get('IMPULSE', {})
                ctx = s.get('CONTEXT', {})
                candles = ctx.get('DATA', [])
                
                # Filter logic for JSON optimization
                filtered_candles = []
                keep_keys = {
                    "ts", "missing_fields", "tf", "exchange", "symbol_clean",
                    "open", "high", "low", "close", 
                    "volume", "buy_volume", "sell_volume", "buy_trades", "sell_trades", 
                    "oi_open", "oi_high", "oi_low", "oi_close", "liq_long", "liq_short", 
                    "range", "body_pct", "clv_pct", "upper_tail_pct", "lower_tail_pct", 
                    "price_sign", "dominant_reject", "cvd_pct", "cvd_sign", "cvd_small", 
                    "dpx", "price_vs_delta", "dtrades_pct", "ratio_stable", "tilt_pct", 
                    "doi_pct", "oi_in_sens", "oi_set", "oi_counter", "oi_unload", "oipos", 
                    "oi_path", "oe", "liq_share_pct", "limb_pct", "liq_squeeze", 
                    "range_pct", "implied_price", "avg_trade_buy", "avg_trade_sell"
                }
                
                for c in candles:
                    filtered_c = {k: v for k, v in c.items() if k in keep_keys}
                    filtered_candles.append(filtered_c)

                # Enforce key order: STATS first, then DATA
                filtered_ctx = {
                    "STATS": ctx.get("STATS", {}),
                    "DATA": filtered_candles
                }

                # Validation: Require Metadata
                m_exch = meta.get('exchange')
                m_sym  = meta.get('symbol')
                m_tf   = meta.get('tf')
                
                if not m_exch or not m_sym or not m_tf:
                    print(f"⚠️ Skipping segment with missing meta: {meta}")
                    continue

                rows.append({
                    "exchange": m_exch,
                    "symbol": m_sym,
                    "tf": m_tf,
                    "ts_start": candles[0].get('ts') if candles else None,
                    "ts_end": candles[-1].get('ts') if candles else None,
                    "y_dir": imp.get('y_dir'),
                    "y_size": imp.get('y_size'),
                    "data": {
                        "META": meta, 
                        "CONTEXT": filtered_ctx
                    }
                })
            
            res_seg = supabase.table("segments").insert(rows).execute()
            seg_count = len(res_seg.data) if res_seg.data else 0
            print(f"✅ Saved {seg_count} segments")
        
        # 2. Save Candles
        if candles_list:
            print("Saving candles...")
            candles_count = save_to_candles(supabase, candles_list)
            print(f"✅ Saved {candles_count} candles")
        
        return seg_count, candles_count
        
    except Exception as e:
        print(f"❌ Save Error: {e}")
        # Supabase rollback happens automatically
        raise e
