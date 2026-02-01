import math
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Union
from collections import defaultdict
from dataclasses import dataclass

# ==============================================================================
# 1. CORE MATH & LOGIC
# ==============================================================================

@dataclass
class Level:
    price: float
    price_sum: float # For accurate re-referencing
    kind: str      
    touches: int
    last_i: int

def parse_candle_data(candles_list: List[Dict]) -> List[object]:
    """
    Converts list of dicts (from DB) into a lightweight object/namespace 
    for dot-notation access if needed, or just validates structure.
    For this engine, we'll just use the dicts directly but ensure numeric types.
    """
    return candles_list

def atr_14(candles: List[Dict], n: int = 14) -> Optional[float]:
    if len(candles) < n + 1:
        return None
    trs = []
    # Logic note: snippet iterated range(-n, 0).
    # We must ensure candles are sorted old->new. Assuming DB returns them sorted.
    for i in range(-n, 0):
        hi = extract_val(candles[i], 'h')
        lo = extract_val(candles[i], 'l')
        prev_close = extract_val(candles[i - 1], 'c')
        tr = max(hi - lo, abs(hi - prev_close), abs(lo - prev_close))
        trs.append(tr)
    return sum(trs) / n

def extract_val(c: Dict, key: str) -> float:
    # Helper to clean numeric strings commonly found in raw DB data
    # Support aliases for h/l/c/o
    aliases = {
        'h': ['high', 'High', 'H'],
        'l': ['low', 'Low', 'L'],
        'c': ['close', 'Close', 'C'],
        'o': ['open', 'Open', 'O'],
        'v': ['volume', 'Volume', 'V']
    }
    
    val = c.get(key)
    
    # Try aliases if primary key missing
    if val is None and key in aliases:
        for alias in aliases[key]:
            val = c.get(alias)
            if val is not None:
                break
                
    if isinstance(val, (int, float)): return float(val)
    if isinstance(val, str): 
        try:
            return float(val.replace(',', ''))
        except:
            return 0.0
    return 0.0

def detect_pivots(candles: List[Dict], k: int = 2, min_vol_pct: float = 0.5) -> List[Tuple[int, str, float]]:
    pivots = []
    n = len(candles)
    
    # Volume Filter Calculation
    vols = [extract_val(c, 'v') for c in candles]
    avg_vol = sum(vols) / len(vols) if vols else 1.0
    
    for i in range(k, n - k):
        vol = extract_val(candles[i], 'v')
        # Filter low volume candles (potentially noise)
        if vol < avg_vol * min_vol_pct:
            continue
            
        hi = extract_val(candles[i], 'h')
        lo = extract_val(candles[i], 'l')
        
        # Left/Right windows
        left_h = [extract_val(candles[j], 'h') for j in range(i - k, i)]
        right_h = [extract_val(candles[j], 'h') for j in range(i + 1, i + k + 1)]
        left_l = [extract_val(candles[j], 'l') for j in range(i - k, i)]
        right_l = [extract_val(candles[j], 'l') for j in range(i + 1, i + k + 1)]

        # Pivot High
        if hi > max(left_h) and hi >= max(right_h):
            pivots.append((i, "H", hi))
        # Pivot Low
        if lo < min(left_l) and lo <= min(right_l):
            pivots.append((i, "L", lo))
            
    return pivots

def get_pivot_window(timeframe: str) -> int:
    """
    Selects k window based on TF.
    """
    tf = timeframe.lower() if timeframe else "1h"
    mapping = {
        '1m': 2, '3m': 2, '5m': 2,
        '15m': 3, '30m': 3,
        '1h': 4, '2h': 4, '3h': 4,
        '4h': 5, '6h': 5, '8h': 5,
        '1d': 8, '3d': 8,
        '1w': 12, '1y': 12
    }
    return mapping.get(tf, 4)

def build_levels(
    candles: List[Dict],
    lookback: int,
    max_levels: int,
    pct_tol: float = 0.0015,
    atr_mult: float = 0.25,
    atr_period: int = 14,
    timeframe: str = "4h",
    k: Optional[int] = None
) -> List[Dict]:
    
    if not candles: return []

    # Slice lookback
    c = candles[-lookback:] if len(candles) > lookback else candles
    
    # Calculate ATR (Safe)
    last_close = extract_val(c[-1], 'c')
    safe_close = last_close if last_close > 0 else 0.1
    
    raw_atr = atr_14(c, atr_period)
    atr_val = raw_atr if (raw_atr is not None and raw_atr > 0) else (safe_close * 0.01)
    
    # Dynamic Window if not forced
    final_k = k if k is not None else get_pivot_window(timeframe)

    # Detect Pivots
    piv = detect_pivots(c, k=final_k)
    if not piv: return []
    
    # Cluster Levels (New Logic: Separate H and L, sort by price)
    levels: List[Level] = []
    
    def get_dynamic_tol(level_price: float) -> float:
        return max(atr_val * atr_mult, level_price * pct_tol)

    def cluster_group(pivots_subset: List[Tuple[int, str, float]]):
        if not pivots_subset: return
        
        # Sort by PRICE to find tight clusters
        # Logic: iterate sorted, if close to current cluster -> add, else new
        sorted_p = sorted(pivots_subset, key=lambda x: x[2]) 
        
        # Clusters: List of lists of pivots
        clusters = []
        current_cluster = [sorted_p[0]]
        
        for i in range(1, len(sorted_p)):
            p = sorted_p[i]
            # Check distance to the 'center' or last element of current cluster?
            # Using center of current cluster is safer
            cluster_prices = [x[2] for x in current_cluster]
            avg_price = sum(cluster_prices) / len(cluster_prices)
            
            tol = get_dynamic_tol(avg_price)
            
            if abs(p[2] - avg_price) <= tol:
                 current_cluster.append(p)
            else:
                 clusters.append(current_cluster)
                 current_cluster = [p]
        
        if current_cluster:
            clusters.append(current_cluster)
            
        # Create Level objects from clusters
        for cl in clusters:
            # Weighted average price? Or simple average?
            # v2 uses simple average of prices.
            # Using simple average is robust enough for small clusters.
            prices = [x[2] for x in cl]
            avg_price = sum(prices) / len(prices)
            indices = [x[0] for x in cl]
            last_i = max(indices)
            kind = cl[0][1] # All same kind
            kind_str = "R" if kind == "H" else "S"
            
            # Store sum for consistency if we were doing incremental, 
            # here we do batched so price is final.
            levels.append(Level(
                price=avg_price, 
                price_sum=sum(prices), 
                kind=kind_str, 
                touches=len(cl), 
                last_i=last_i
            ))

    # 1. Separate Highs and Lows
    highs = [p for p in piv if p[1] == "H"]
    lows = [p for p in piv if p[1] == "L"]
    
    # 2. Cluster each group independently
    cluster_group(highs)
    cluster_group(lows)
            
    # Scoring (Multi-factor)
    last_idx = len(c) - 1
    
    def score(lv: Level) -> float:
        # Factor 1: Touches (sqrt for diminishing returns)
        touches_score = math.sqrt(lv.touches)
        
        # Factor 2: Recency (Exponential Decay)
        bars_ago = max(0, last_idx - lv.last_i)
        recency_score = math.exp(-bars_ago / 20.0) 
        
        # Factor 3: Proximity to current price
        # Levels closer to current price are more relevant
        price_dist = abs(lv.price - last_close)
        proximity_score = 1.0 / (1.0 + (price_dist / safe_close) * 100.0)
        
        # Weighted Total
        # Touches: 40%, Recency: 35%, Proximity: 25%
        return (touches_score * 0.4) + (recency_score * 0.35) + (proximity_score * 0.25)

    levels.sort(key=score, reverse=True)
    levels = levels[:max_levels]
    levels.sort(key=lambda x: x.price)
    
    # Output
    out = []
    
    MIN_ZONE_WIDTH = last_close * 0.0005 # Ensure at least 0.05% width visibility
    
    for lv in levels:
        final_tol = get_dynamic_tol(lv.price)
        z_low = lv.price - final_tol
        z_high = lv.price + final_tol
        
        # Ensure minimum width
        if (z_high - z_low) < MIN_ZONE_WIDTH:
            mid = (z_high + z_low) / 2
            z_low = mid - MIN_ZONE_WIDTH / 2
            z_high = mid + MIN_ZONE_WIDTH / 2
        
        out.append({
            "kind": lv.kind,
            "mid": round(lv.price, 2),
            "zone_low": round(z_low, 2),
            "zone_high": round(z_high, 2),
            "touches": lv.touches,
            "tol": final_tol
        })
        
    return out



