"""
Shared STATS Calculation Library
Used by: Stage 5 (bins_stats), Stage 6 (mine_stats), Online Detector

Per ТЗ v2.1 PATCH-03 + PATCH-08:
- 8 STATS fields calculated from buffer of RAW candles
- NULL in field → per-feature None (not error)
- NaN/string → raise ValueError (Stage 1 bug)
- PATCH-08: net_oi_change uses only first/last candle (NULL in middle allowed)
"""

import math
from typing import List, Dict, Optional, Any

# --- CONSTANTS ---

STATS_FIELDS = [
    "sum_cvd_pct",
    "net_oi_change", 
    "sum_liq_long",
    "sum_liq_short",
    "avg_upper_tail_pct",
    "avg_lower_tail_pct",
    "body_range_pct",
    "liq_dominance_ratio",
]

MAX_SEGMENT_LENGTH = 30


# --- HELPERS ---

def safe_float(val: Any) -> Optional[float]:
    """Convert value to float or None.
    
    Per PATCH-03:
    - None/missing → None (allowed for STATS fields)
    - NaN → raise ValueError (Stage 1 bug)
    - string → raise ValueError (data corruption)
    - bool → raise ValueError (wrong field passed)
    
    Returns:
        float | None: Valid value or None if missing
    Raises:
        ValueError: If value is NaN, wrong type, or bool
    """
    # None/missing → allowed
    if val is None:
        return None
    
    # Bool check FIRST (before float conversion, since bool is subclass of int)
    # This catches cases where someone accidentally passes oi_set instead of oi_close
    if isinstance(val, bool):
        raise ValueError(f"Unexpected bool '{val}' in STATS field - likely wrong field passed (e.g., oi_set instead of oi_close)")
    
    # String → data corruption
    if isinstance(val, str):
        raise ValueError(f"Unexpected string '{val}' in cleaned data")
    
    # Convert to float first, then check NaN (works with numpy types too)
    try:
        x = float(val)
    except (TypeError, ValueError) as e:
        raise ValueError(f"Cannot convert {type(val).__name__} to float: {e}")
    
    # NaN check after conversion (catches both float('nan') and np.nan)
    if math.isnan(x):
        raise ValueError("Unexpected NaN in cleaned data - Stage 1 bug")
    
    return x


def safe_get_all(buffer: List[Dict], field: str) -> Optional[List[float]]:
    """Get all values for a field from buffer.
    
    Per PATCH-03: If any value is missing → entire result is None.
    
    Returns:
        list[float] | None: List of values or None if any missing
    Raises:
        ValueError: If NaN or wrong type found
    """
    values = []
    for i, candle in enumerate(buffer):
        val = candle.get(field)
        converted = safe_float(val)
        if converted is None:
            return None  # Per PATCH-03: whole field = None for this step
        values.append(converted)
    return values


# --- STATS FORMULAS (PATCH-03) ---

def calc_sum_cvd_pct(buffer: List[Dict]) -> Optional[float]:
    """Sum of cvd_pct across buffer."""
    values = safe_get_all(buffer, "cvd_pct")
    if values is None:
        return None
    return sum(values)


def calc_net_oi_change(buffer: List[Dict]) -> Optional[float]:
    """Percentage change in OI from first to last candle.
    
    PATCH-08: Uses only buffer[0] and buffer[-1]. NULL in intermediate
    candles is allowed (unlike other STATS fields per PATCH-03).
    """
    if len(buffer) == 0:
        return None
    
    oi_first = safe_float(buffer[0].get("oi_close"))
    oi_last = safe_float(buffer[-1].get("oi_close"))
    
    if oi_first is None or oi_last is None:
        return None
    
    if oi_first == 0:
        return 0.0
    
    return ((oi_last - oi_first) / oi_first) * 100


def calc_sum_liq_long(buffer: List[Dict]) -> Optional[float]:
    """Sum of liq_long across buffer."""
    values = safe_get_all(buffer, "liq_long")
    if values is None:
        return None
    return sum(values)


def calc_sum_liq_short(buffer: List[Dict]) -> Optional[float]:
    """Sum of liq_short across buffer."""
    values = safe_get_all(buffer, "liq_short")
    if values is None:
        return None
    return sum(values)


def calc_avg_upper_tail_pct(buffer: List[Dict]) -> Optional[float]:
    """Average upper_tail_pct across buffer."""
    values = safe_get_all(buffer, "upper_tail_pct")
    if values is None or len(values) == 0:
        return None
    return sum(values) / len(values)


def calc_avg_lower_tail_pct(buffer: List[Dict]) -> Optional[float]:
    """Average lower_tail_pct across buffer."""
    values = safe_get_all(buffer, "lower_tail_pct")
    if values is None or len(values) == 0:
        return None
    return sum(values) / len(values)


def calc_body_range_pct(buffer: List[Dict]) -> Optional[float]:
    """Body range as percentage of total range.
    
    Formula (0-indexed):
    - MAX_high = max(buffer[j].high for j in 0..w-1)
    - MIN_low = min(buffer[j].low for j in 0..w-1)
    - body_start = buffer[0].close
    - body_end = buffer[w-1].close
    - body_range_pct = |body_end - body_start| / (MAX_high - MIN_low) * 100
    """
    if len(buffer) == 0:
        return None
    
    highs = safe_get_all(buffer, "high")
    lows = safe_get_all(buffer, "low")
    
    if highs is None or lows is None:
        return None
    
    body_start = safe_float(buffer[0].get("close"))
    body_end = safe_float(buffer[-1].get("close"))
    
    if body_start is None or body_end is None:
        return None
    
    max_high = max(highs)
    min_low = min(lows)
    
    if max_high == min_low:
        return 0.0
    
    return abs(body_end - body_start) / (max_high - min_low) * 100


def calc_liq_dominance_ratio(buffer: List[Dict]) -> Optional[float]:
    """Ratio of liq_long to liq_short.
    
    Per ТЗ:
    - short > 0 → long/short
    - short == 0 and long > 0 → None (undefined)
    - short == 0 and long == 0 → 1.0
    """
    sum_long = calc_sum_liq_long(buffer)
    sum_short = calc_sum_liq_short(buffer)
    
    if sum_long is None or sum_short is None:
        return None
    
    if sum_short > 0:
        return sum_long / sum_short
    elif sum_long > 0:
        return None  # Undefined - cannot divide by zero
    else:
        return 1.0  # Both zero


def calculate_stats(buffer: List[Dict]) -> Dict[str, Optional[float]]:
    """Calculate all 8 STATS fields for a buffer.
    
    Args:
        buffer: List of RAW candle dicts
        
    Returns:
        Dict with all STATS fields. Values can be None if data missing.
    """
    return {
        "sum_cvd_pct": calc_sum_cvd_pct(buffer),
        "net_oi_change": calc_net_oi_change(buffer),
        "sum_liq_long": calc_sum_liq_long(buffer),
        "sum_liq_short": calc_sum_liq_short(buffer),
        "avg_upper_tail_pct": calc_avg_upper_tail_pct(buffer),
        "avg_lower_tail_pct": calc_avg_lower_tail_pct(buffer),
        "body_range_pct": calc_body_range_pct(buffer),
        "liq_dominance_ratio": calc_liq_dominance_ratio(buffer),
    }
