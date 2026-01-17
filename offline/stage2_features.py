"""
Stage 2: Feature Engineering (2_simulate_states.py)
Simulates online mode: processes each setup step-by-step, creating CORE_STATE and BOOST metrics.

Per ТЗ v2.1 + PATCH-10:
- CORE_STATE: div_type, oi_flags, cvd_pct, clv_pct, td
- BOOST: vol_rank, doi_rank, liq_rank (+ top1_share variants)
- rank = None if i < 5
- BOOST fields (volume/doi_pct/liq_*) use None-safe handling (no fake zeros)
- td = tail dominance (U/L/N) calculated via get_tail_dom()
"""

import json
import math
import sys
from pathlib import Path

# Add offline directory to path for tokenizer import
_offline_dir = Path(__file__).parent
if str(_offline_dir) not in sys.path:
    sys.path.insert(0, str(_offline_dir))

from tokenizer import get_tail_dom

# --- HELPERS ---

def safe_get(c, field):
    """
    Safe getter that returns None for missing, None, or NaN values.
    CONTRACT: Never substitute fake zeros for missing BOOST data.
    """
    val = c.get(field)
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    return val


def get_div_type(price_sign, cvd_sign):
    """
    Calculates div_type from price_sign and cvd_sign.
    Returns one of 5 classes per ТЗ.
    """
    if price_sign > 0 and cvd_sign > 0:
        return "match_up"
    if price_sign < 0 and cvd_sign < 0:
        return "match_down"
    if price_sign > 0 and cvd_sign < 0:
        return "div_price_up_delta_down"
    if price_sign < 0 and cvd_sign > 0:
        return "div_price_down_delta_up"
    return "neutral_or_zero"


def get_oi_flags(c):
    """
    Calculates OI flags as 4-bit mask.
    Bit 0: oi_set, Bit 1: oi_unload, Bit 2: oi_counter, Bit 3: oi_in_sens
    """
    return (
        (int(bool(c.get("oi_set", 0))) << 0) |
        (int(bool(c.get("oi_unload", 0))) << 1) |
        (int(bool(c.get("oi_counter", 0))) << 2) |
        (int(bool(c.get("oi_in_sens", 0))) << 3)
    )


def percentile_rank(x, values):
    """
    Calculates percentile rank using strict '<' comparison.
    Returns None if x is None or not enough valid values.
    
    Per ТЗ 3.5a:
    - rank = count(v < x for v in values) / len(values)
    - Only count valid (non-None) values
    """
    if x is None:
        return None
    
    valid = [v for v in values if v is not None]
    if len(valid) < 5:
        return None
    
    count_less = sum(1 for v in valid if v < x)
    return count_less / len(valid)


def top1_share(values):
    """
    Calculates share of maximum value in total sum.
    Returns 0 if sum is 0 or no valid values.
    """
    valid = [v for v in values if v is not None]
    if not valid:
        return 0.0
    
    total = sum(valid)
    if total == 0:
        return 0.0
    
    return max(valid) / total


# --- MAIN LOGIC ---

def load_clean_data(symbol, tf, exchange):
    """Load cleaned segments from Stage 1 output."""
    clean_symbol = symbol.replace("/", "").replace(":", "")
    clean_tf = tf.replace("/", "")
    clean_ex = exchange.replace("/", "")
    filepath = Path(__file__).parent / "data" / f"{clean_symbol}_{clean_tf}_{clean_ex}_clean.json"
    
    if not filepath.exists():
        return None, f"File not found: {filepath}"
    
    with open(filepath, "r") as f:
        data = json.load(f)
    return data, None


def process_segment(seg):
    """
    Process a single segment: iterate through candles, build CORE_STATE and BOOST.
    Returns enriched segment with 'steps' array.
    """
    candles = seg.get("data", {}).get("CONTEXT", {}).get("DATA", [])
    if not candles:
        return None
    
    # History for BOOST metrics (None-safe)
    V_hist = []    # volume
    DOI_hist = []  # abs(doi_pct)
    LIQ_hist = []  # liq_long + liq_short
    
    # Warnings counter for missing BOOST fields
    warnings = {"volume": 0, "doi_pct": 0, "liq_long": 0, "liq_short": 0}
    
    steps = []
    
    for i, c in enumerate(candles):
        # === CORE_STATE ===
        # TD: fallback to "N" if calculation fails (NaN, type errors, etc.)
        try:
            td = get_tail_dom(c)
        except Exception:
            td = "N"
        
        core_state = {
            "div_type": get_div_type(c.get("price_sign"), c.get("cvd_sign")),
            "oi_flags": get_oi_flags(c),
            "cvd_pct": c.get("cvd_pct"),
            "clv_pct": c.get("clv_pct"),
            "td": td,  # PATCH-10: tail dominance (U/L/N)
        }
        
        # === BOOST: Update history (None-safe) ===
        vol = safe_get(c, "volume")
        doi = safe_get(c, "doi_pct")
        liq_long = safe_get(c, "liq_long")
        liq_short = safe_get(c, "liq_short")
        
        # Track missing fields
        if vol is None: warnings["volume"] += 1
        if doi is None: warnings["doi_pct"] += 1
        if liq_long is None: warnings["liq_long"] += 1
        if liq_short is None: warnings["liq_short"] += 1
        
        # Add to history
        V_hist.append(vol)
        DOI_hist.append(abs(doi) if doi is not None else None)
        
        # For liquidations: use 0 if missing (user decision)
        liq_total = (liq_long or 0) + (liq_short or 0)
        LIQ_hist.append(liq_total)
        
        # === BOOST: Calculate metrics ===
        # rank = None for i < 5 per ТЗ
        boost = {
            "vol_top1_share": top1_share(V_hist),
            "vol_rank": None,
            "doi_top1_share": top1_share(DOI_hist),
            "doi_rank": None,
            "liq_top1_share": top1_share(LIQ_hist),
            "liq_rank": None,
        }
        
        if i >= 5:
            boost["vol_rank"] = percentile_rank(vol, V_hist)
            boost["doi_rank"] = percentile_rank(DOI_hist[-1], DOI_hist)
            boost["liq_rank"] = percentile_rank(liq_total, LIQ_hist)
        
        steps.append({
            "i": i,
            "ts": c.get("ts"),
            "core_state": core_state,
            "boost": boost,
        })
    
    return {
        "id": seg.get("id"),
        "y_dir": seg.get("y_dir"),
        "y_size": seg.get("y_size"),
        "steps": steps,
        "warnings": warnings,
    }


def run_simulation(symbol, tf, exchange="Binance"):
    """
    Executes Step 1.2: Feature Engineering.
    Returns: (success: bool, message: str, count: int)
    """
    print(f"[START] Feature Engineering for {symbol} {tf} ({exchange})...")
    
    # 1. Load
    segments, err = load_clean_data(symbol, tf, exchange)
    if err:
        return False, err, 0
    if segments is None or len(segments) == 0:
        return False, "No segments loaded (empty clean file)", 0
    
    print(f"[INFO] Loaded {len(segments)} segments.")
    
    # 2. Process
    enriched = []
    total_steps = 0
    total_warnings = {"volume": 0, "doi_pct": 0, "liq_long": 0, "liq_short": 0}
    
    for seg in segments:
        result = process_segment(seg)
        if result:
            enriched.append(result)
            total_steps += len(result["steps"])
            
            # Aggregate warnings
            for key in total_warnings:
                total_warnings[key] += result["warnings"].get(key, 0)
    
    # 3. Save
    clean_symbol = symbol.replace("/", "").replace(":", "")
    clean_tf = tf.replace("/", "")
    clean_ex = exchange.replace("/", "")
    outfile = Path(__file__).parent / "data" / f"{clean_symbol}_{clean_tf}_{clean_ex}_features.json"
    
    outfile.parent.mkdir(parents=True, exist_ok=True)
    with open(outfile, "w") as f:
        json.dump(enriched, f, indent=2, default=str)
    
    # Log warnings
    if any(v > 0 for v in total_warnings.values()):
        print(f"[WARN] Missing BOOST fields: {total_warnings}")
    
    report = f"Обработано {len(segments)} сегментов → {total_steps} шагов."
    return True, report, len(enriched)


if __name__ == "__main__":
    res, msg, cnt = run_simulation("ETH", "1D", "Binance")
    print(f"[{'OK' if res else 'ERROR'}] {msg}")
