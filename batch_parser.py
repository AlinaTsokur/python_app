import re
import math
from datetime import datetime
import pandas as pd
from parsing_engine import parse_raw_input, calculate_metrics

# --- 3. STATS CALCULATION (Architecture V5) ---
def calculate_stats_agg(candles):
    """Calculates aggregated STATS for a segment of candles."""
    if not candles: return {}
    
    c_first = candles[0]
    c_last = candles[-1]
    
    sum_vol = sum((c.get('volume') or 0) for c in candles)
    sum_cvd = sum((c.get('cvd_pct') or 0) for c in candles) 
    
    sum_liq_L = sum((c.get('liq_long') or 0) for c in candles)
    sum_liq_S = sum((c.get('liq_short') or 0) for c in candles)
    
    # Avoid zero division
    liq_ratio = 999.0
    if sum_liq_S > 0:
        liq_ratio = round(sum_liq_L / sum_liq_S, 2)
    elif sum_liq_L > 0:
        liq_ratio = 999.0 # Dominance
    else:
        liq_ratio = 0.0

    start_px = c_first.get('open', 0) or 0
    end_px = c_last.get('close', 0) or 0
    
    # Body Range Pct
    # Need global High/Low of the segment
    seg_high = max((c.get('high') or 0) for c in candles)
    seg_low = min((c.get('low') or 99999999) for c in candles)
    seg_range = seg_high - seg_low
    
    body_rng_pct = 0.0
    if seg_range > 0:
        body_rng_pct = abs(end_px - start_px) / seg_range * 100
        
    avg_upper = sum((c.get('upper_tail_pct') or 0) for c in candles) / len(candles)
    avg_lower = sum((c.get('lower_tail_pct') or 0) for c in candles) / len(candles)
    
    net_oi = ((c_last.get('oi_close') or 0) - (c_first.get('oi_open') or 0))

    stats = {
        "candles_count": len(candles),
        "sum_volume": sum_vol,
        "sum_cvd_pct": round(sum_cvd, 2),
        "sum_liq_long": sum_liq_L,
        "sum_liq_short": sum_liq_S,
        "net_oi_change": net_oi,
        "start_price": start_px,
        "end_price": end_px,
        "avg_upper_tail_pct": round(avg_upper, 2),
        "avg_lower_tail_pct": round(avg_lower, 2),
        "liq_dominance_ratio": liq_ratio,
        "body_range_pct": round(body_rng_pct, 2)
    }
    return stats

# --- 4. DATA SAVING LOGIC (Step 1.2) ---

def save_to_candles(supabase, candles_list):
    """
    Saves parsed candles to 'candles' table.
    Uses 'id' based on exchange+symbol+tf+ts to handle upserts.
    """
    if not candles_list: return 0
    
    data_for_upsert = []
    
    for c in candles_list:
        # Generate composite composite ID for exact deduplication
        # (Assuming app logic uses same ID generation, usually done via hash or composite unique index)
        # Note: If Supabase table has AUTO-ID, we might need a unique constraint columns.
        # User's 'candles' table typically uses `ts` + `symbol` + `timeframe` + `exchange` as unique
        # We will let Supabase handle conflict if Unique constraint exists, or just insert.
        # IF current app uses `update_candle_db` by ID, it implies IDs are fetched first.
        # Here we are inserting NEW data.
        
        # Prepare row consistent with DB schema
        row = {
            "ts": c.get('ts'),
            "symbol": c.get('symbol_clean'),
            "exchange": c.get('exchange'),
            "tf": c.get('tf'),
            "open": c.get('open'),
            "high": c.get('high'),
            "low": c.get('low'),
            "close": c.get('close'),
            "volume": c.get('volume'),
            "note": c.get('x_ray'), # We save X-Ray text into 'note' or separate column? 
                                    # App uses 'x_ray' column.
            "x_ray": c.get('x_ray'), # Ensure we save 'x_ray' content
            "raw_data": c.get('raw_data')[:1000] # Truncate if too long?
            # Add other metrics if columns exist in DB
        }
        
        # Filter None
        row = {k: v for k, v in row.items() if v is not None}
        data_for_upsert.append(row)

    count = 0
    if data_for_upsert:
        # Upsert: on_conflict=["ts", "symbol", "tf", "exchange"]
        # Need to know exact constraint name or columns.
        try:
            # Try plain upsert using columns that define uniqueness
            res = supabase.table("candles").upsert(
                data_for_upsert, 
                on_conflict="ts, symbol, tf, exchange" 
            ).execute()
            count = len(res.data) if res.data else 0
        except Exception as e:
            # Fallback if constraint differs or error
            print(f"Save Error: {e}")
            return 0
            
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
        is_covered = any(v_start <= s_start and s_end <= v_end for v_start, v_end in valid_ranges)
        
        if not is_covered:
            warnings.append(f"⚠️ POTENTIAL ERROR: Found incomplete label '{sm.group(1)}' at char {s_start}. Expected format: 'Strength Direction' (e.g. 'Strong Up'). This segment might be parsed incorrectly.")
    # -----------------------------------------------
    labels_iter = list(re.finditer(LABEL_REGEX, full_text, re.IGNORECASE))
    current_pos = 0
    
    for match in labels_iter:
        label_start = match.start()
        label_end = match.end()
        y_size = match.group(1).title() 
        y_dir = match.group(2).upper()  
        
        chunk_text = full_text[current_pos:label_start].strip()
        
        if not chunk_text:
            warnings.append(f"Label {y_size} {y_dir} at char {label_start} has no preceding candles.")
        else:
            raw_candles = re.split(r'(?=(?:Binance|Bybit|OKX)\s+·)', chunk_text, flags=re.IGNORECASE)
            raw_candles = [c.strip() for c in raw_candles if c.strip()]
            
            parsed_segment_candles = []
            
            for rc in raw_candles:
                base = parse_raw_input(rc)
                if not base.get('open'): 
                    if len(rc) > 20: warnings.append(f"Failed to parse candle: {rc[:50]}...")
                    continue
                
                full = calculate_metrics(base, config)
                # Need to generate X-Ray for consistency if saving to 'candles'
                from parsing_engine import generate_full_report # Local import to avoid circular defaults
                # Check for volume
                if full.get('buy_volume', 0) != 0:
                     full['x_ray'] = generate_full_report(full)
                
                parsed_segment_candles.append(full)
            
            if parsed_segment_candles:
                parsed_segment_candles.sort(key=lambda x: x.get('ts', ''))
                all_candles.extend(parsed_segment_candles)
                stats = calculate_stats_agg(parsed_segment_candles)
                
                first = parsed_segment_candles[0]
                meta = {
                    "symbol": first.get('symbol_clean', 'Unknown') + " Perp", 
                    "tf": first.get('tf', '4h'),
                    "exchange": first.get('exchange', 'Binance'),
                    "total_candles": len(parsed_segment_candles)
                }
                
                segment_obj = {
                    "META": meta,
                    "CONTEXT": {
                        "STATS": stats,
                        "DATA": parsed_segment_candles
                    },
                    "IMPULSE": {
                        "y_dir": y_dir,
                        "y_size": y_size
                    }
                }
                segments.append(segment_obj)
            else:
                warnings.append(f"Label {y_size} {y_dir} had valid text but no valid candles parsed.")
        
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
    Transactions Logic (Python-side Best Effort).
    1. Insert Segments -> Get IDs.
    2. Upsert Candles.
    3. If Candles fail -> Delete Segments (Rollback).
    
    Returns: (segments_count, candles_count) or raises Exception.
    """
    # 1. Save Segments first (since they are new entries, easier to rollback delete)
    seg_ids = []
    seg_count = 0
    
    if segments_list:
        try:
            # Manually constructing rows as save_to_segments doesn't return IDs
            print("Saving segments...")
            rows = []
            for s in segments_list:
                meta = s.get('META', {})
                imp = s.get('IMPULSE', {})
                ctx = s.get('CONTEXT', {})
                candles = ctx.get('DATA', [])
                data_blob = {"META": meta, "CONTEXT": ctx}
                t_start = candles[0].get('ts') if candles else None
                t_end = candles[-1].get('ts') if candles else None
                
                rows.append({
                    "exchange": meta.get('exchange'),
                    "symbol": meta.get('symbol'),
                    "tf": meta.get('tf'),
                    "ts_start": t_start,
                    "ts_end": t_end,
                    "y_dir": imp.get('y_dir'),
                    "y_size": imp.get('y_size'),
                    "data": data_blob
                })
            
            if rows:
                res = supabase.table("segments").insert(rows).select("id").execute()
                if res.data:
                    seg_ids = [r['id'] for r in res.data]
                    seg_count = len(seg_ids)
            
        except Exception as e:
            print(f"Transaction Aborted: Segments Save Failed: {e}")
            raise e # Abort immediately

    # 2. Save Candles
    if candles_list:
        try:
            print("Saving candles...")
            save_to_candles(supabase, candles_list) 
        except Exception as e:
             print(f"Candle Save Error: {e}")
             if seg_ids:
                 print(f"ROLLBACK: Deleting {len(seg_ids)} segments...")
                 try:
                     supabase.table("segments").delete().in_("id", seg_ids).execute()
                 except Exception as rb_e:
                     print(f"Rollback Failed: {rb_e}")
             raise e
    
    return seg_count, len(candles_list)
