"""
Stage 6: Mine STATS Rules (6_mine_rules_stats.py)
Finds combinations of STATS-conditions that predict impulse direction.

Per ТЗ v2.1 + Step 1.6 spec:
- Apriori-style combinations (1-3 conditions)
- Support/wins by unique setups (presence semantic)
- Same thresholds as Stage 4 (min_support, min_edge)
- Canonization: sorted by feat, no duplicate feats
- NULL conditions excluded from itemsets
"""

import json
import os
import math
import sys
import tomllib
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict
from itertools import combinations
from supabase import create_client, Client

# Import shared STATS calculations
try:
    from .stats_calc import calculate_stats, STATS_FIELDS, MAX_SEGMENT_LENGTH
except ImportError:
    from stats_calc import calculate_stats, STATS_FIELDS, MAX_SEGMENT_LENGTH

# --- CONFIG ---
PATCHLOG_VERSION = "PATCHLOG_v2.1@2026-01-10"
BUILD_VERSION = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# Mining parameters (same as Stage 4)
PRIOR_STRENGTH = 10
MAX_CONDITIONS = 3


# --- HELPERS ---

def load_secrets():
    """Load Supabase credentials from env vars or .streamlit/secrets.toml."""
    # 1. Try Env Vars (Railway)
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if url and key:
        return url, key

    # 2. Try Secrets File (Local)
    secrets_path = Path(__file__).parent.parent / ".streamlit/secrets.toml"
    if not secrets_path.exists():
        raise FileNotFoundError(f"Secrets file not found at: {secrets_path} and no ENV vars set.")
    
    with open(secrets_path, "rb") as f:
        secrets = tomllib.load(f)
    
    return secrets["SUPABASE_URL"], secrets["SUPABASE_KEY"]


def load_clean_data(symbol: str, tf: str, exchange: str):
    """Load cleaned segments from Stage 1 output."""
    clean_symbol = symbol.replace("/", "").replace(":", "")
    clean_tf = tf.replace("/", "")
    clean_ex = exchange.replace("/", "")
    filename = f"{clean_symbol}_{clean_tf}_{clean_ex}_clean.json"
    filepath = Path(__file__).parent / "data" / filename
     
    if not filepath.exists():
        return None, f"File not found: {filepath}"
    
    with open(filepath, "r") as f:
        segments = json.load(f)
    
    # Validate: must be list
    if not isinstance(segments, list):
        return None, f"Clean file must contain list of segments, got {type(segments).__name__}"
    
    return segments, None


def load_bins_stats(symbol: str, tf: str, exchange: str):
    """Load STATS quantiles from Stage 5 output."""
    clean_symbol = symbol.replace("/", "").replace(":", "")
    clean_tf = tf.replace("/", "")
    clean_ex = exchange.replace("/", "")
    filename = f"{clean_symbol}_{clean_tf}_{clean_ex}_bins_stats.json"
    filepath = Path(__file__).parent / "data" / filename
    
    if not filepath.exists():
        return None, f"File not found: {filepath}"
    
    with open(filepath, "r") as f:
        bins_stats = json.load(f)
    
    return bins_stats, None


def assign_bin(value, quantiles):
    """Assign bin Q1-Q5 based on quantiles. None/NaN/invalid → None."""
    if value is None:
        return None
    if quantiles is None:
        return None
    
    # Convert to float for safe comparison
    try:
        v = float(value)
        q20 = float(quantiles["q20"])
        q40 = float(quantiles["q40"])
        q60 = float(quantiles["q60"])
        q80 = float(quantiles["q80"])
    except (ValueError, TypeError, KeyError):
        return None
    
    # Check for NaN
    if np.isnan(v):
        return None
    
    if v <= q20:
        return "Q1"
    if v <= q40:
        return "Q2"
    if v <= q60:
        return "Q3"
    if v <= q80:
        return "Q4"
    return "Q5"


def canonize(conditions):
    """Canonize rule: sorted by feat. Returns None if duplicate feats."""
    sorted_conds = tuple(sorted(conditions, key=lambda x: x[0]))
    feats = [c[0] for c in sorted_conds]
    
    # Check: one feat = one condition
    if len(feats) != len(set(feats)):
        return None  # Invalid rule
    
    return sorted_conds


def parse_key(rule_key):
    """Convert tuple → list of dicts for JSON."""
    return [{"feat": f, "bin": b} for f, b in rule_key]


# --- MAIN ---

def run_mine_stats(symbol: str, tf: str, exchange: str):
    """Main function to mine STATS rules."""
    print(f"[START] Mining STATS rules for {symbol} {tf} ({exchange})...")
    
    # 1. Load data
    segments, err = load_clean_data(symbol, tf, exchange)
    if err:
        return False, err
    
    bins_stats, err = load_bins_stats(symbol, tf, exchange)
    if err:
        return False, err
    
    # Validate bins_stats format
    if not isinstance(bins_stats, dict):
        return False, "bins_stats invalid format: not a dict"
    if "fields" not in bins_stats or not isinstance(bins_stats.get("fields"), dict):
        return False, "bins_stats invalid format: missing 'fields' dict"
    
    # Validate all STATS_FIELDS have quantiles with required keys
    fields_data = bins_stats["fields"]
    required_q_keys = {"q20", "q40", "q60", "q80"}
    for f in STATS_FIELDS:
        if f not in fields_data or fields_data[f] is None:
            return False, f"bins_stats missing quantiles for: {f}"
        q_data = fields_data[f]
        missing_keys = required_q_keys - set(q_data.keys())
        if missing_keys:
            return False, f"bins_stats.{f} missing keys: {sorted(missing_keys)}"
    
    print(f"[INFO] Loaded {len(segments)} segments.")
    
    # 2. Extract valid setups (id + y_dir in {UP, DOWN})
    y_dirs = {}
    seen_ids = set()  # For duplicate detection
    skipped_no_id = 0
    skipped_invalid_y = 0
    for segment in segments:
        setup_id = segment.get("id")
        if setup_id is None:
            skipped_no_id += 1
            continue
        
        # Check for duplicate ID (strict, like Stage 4)
        if setup_id in seen_ids:
            return False, f"DUPLICATE_ID: segment id '{setup_id}' appears multiple times"
        seen_ids.add(setup_id)
        
        y_dir = segment.get("y_dir")
        if y_dir not in ("UP", "DOWN"):
            skipped_invalid_y += 1
            continue
        y_dirs[setup_id] = y_dir
    
    # N = number of VALID setups only
    N = len(y_dirs)
    if N == 0:
        return False, "No valid setups with id and y_dir in {UP, DOWN}"
    
    if skipped_no_id > 0 or skipped_invalid_y > 0:
        print(f"[INFO] Skipped: {skipped_no_id} no id, {skipped_invalid_y} invalid y_dir")
    
    base_P_UP = sum(1 for y in y_dirs.values() if y == "UP") / N
    print(f"[INFO] Valid setups: {N}, Base P(UP) = {base_P_UP:.4f}")
    
    # 3. Calculate adaptive thresholds (same as Stage 4)
    min_support_abs = max(3, math.ceil(0.02 * N))
    min_edge_threshold = max(0.03, 1 / math.sqrt(N))
    print(f"[INFO] Thresholds: min_support={min_support_abs}, min_edge={min_edge_threshold:.4f}")
    
    # 4. Collect rule coverage (presence semantic: rule counted once per setup)
    rule_setups = defaultdict(set)  # rule_key → set(setup_ids)
    
    for segment in segments:
        setup_id = segment.get("id")
        # Only process valid setups (already in y_dirs)
        if setup_id not in y_dirs:
            continue
        
        candles = segment.get("data", {}).get("CONTEXT", {}).get("DATA", [])
        
        if not isinstance(candles, list):
            print(f"[WARN] Segment {setup_id}: CONTEXT.DATA is not list, skipping")
            continue
        
        # Strict check: segment too long is a Stage 1 bug
        if len(candles) > MAX_SEGMENT_LENGTH:
            raise ValueError(f"Segment {setup_id} too long ({len(candles)} > {MAX_SEGMENT_LENGTH}) - Stage 1 bug")
        
        seen_in_setup = set()  # Dedup within setup (presence semantic)
        
        for i in range(len(candles)):
            # Buffer as in Stage 5
            start = max(0, i - (MAX_SEGMENT_LENGTH - 1))
            buffer = candles[start:i+1]
            
            # Calculate STATS (with None protection)
            stats = calculate_stats(buffer)
            if not isinstance(stats, dict):
                continue  # skip if calculate_stats returned None or invalid
            
            # Binning → itemset (exclude None, iterate by STATS_FIELDS for contract)
            itemset = []
            for feat in STATS_FIELDS:
                value = stats.get(feat)
                quantiles = bins_stats.get("fields", {}).get(feat)
                bin_val = assign_bin(value, quantiles)
                if bin_val is not None:
                    itemset.append((feat, bin_val))
            
            # Dedup itemset (defensive)
            itemset = sorted(set(itemset))
            
            if len(itemset) == 0:
                continue
            
            # Generate 1-3 combinations
            for length in range(1, min(MAX_CONDITIONS + 1, len(itemset) + 1)):
                for combo in combinations(itemset, length):
                    rule_key = canonize(combo)
                    
                    if rule_key is None:
                        continue
                    
                    if rule_key not in seen_in_setup:
                        seen_in_setup.add(rule_key)
                        rule_setups[rule_key].add(setup_id)
    
    print(f"[INFO] Found {len(rule_setups)} unique rule candidates.")
    
    # 5. Calculate metrics AFTER full pass
    candidates = []
    rejected_by_support = 0
    rejected_by_edge = 0
    
    alpha_prior = base_P_UP * PRIOR_STRENGTH + 1
    beta_prior = (1 - base_P_UP) * PRIOR_STRENGTH + 1
    
    for rule_key, setups in rule_setups.items():
        support = len(setups)
        
        if support < min_support_abs:
            rejected_by_support += 1
            continue
        
        # wins_up: count HERE, not on-the-fly (use [] not .get() to catch bugs)
        wins_up = sum(1 for s in setups if y_dirs[s] == "UP")
        wins_down = support - wins_up
        
        # Beta-prior smoothing
        p_up_smooth = (wins_up + alpha_prior) / (support + alpha_prior + beta_prior)
        
        # Edge (explicit formulas for audit)
        edge_up = p_up_smooth - base_P_UP
        edge_down = (1 - p_up_smooth) - (1 - base_P_UP)
        
        if max(edge_up, edge_down) >= min_edge_threshold:
            candidates.append({
                "conditions": parse_key(rule_key),
                "support": support,
                "wins_up": wins_up,
                "wins_down": wins_down,
                "p_up_smooth": round(p_up_smooth, 4),
                "edge_up": round(edge_up, 4),
                "edge_down": round(edge_down, 4),
                "direction": "UP" if p_up_smooth >= 0.5 else "DOWN"
            })
        else:
            rejected_by_edge += 1
    
    # Sort by edge (descending)
    candidates.sort(key=lambda x: -max(x["edge_up"], x["edge_down"]))
    
    print(f"[INFO] {len(candidates)} rules pass, rejected: {rejected_by_support} by support, {rejected_by_edge} by edge.")
    
    # 6. Build artifact
    clean_symbol = symbol.replace("/", "").replace(":", "")
    clean_tf = tf.replace("/", "")
    clean_ex = exchange.replace("/", "")
    
    artifact = {
        "version": BUILD_VERSION,
        "patchlog_version": PATCHLOG_VERSION,
        "symbol": symbol,
        "tf": tf,
        "exchange": exchange,
        "metadata": {
            "N_setups": N,
            "base_P_UP": round(base_P_UP, 4),
            "min_edge_threshold": round(min_edge_threshold, 4),
            "min_support_abs": min_support_abs
        },
        "rules": candidates
    }
    
    # 7. Save locally
    local_filename = f"{clean_symbol}_{clean_tf}_{clean_ex}_rules_stats.json"
    local_path = Path(__file__).parent / "data" / local_filename
    
    with open(local_path, "w") as f:
        json.dump(artifact, f, indent=2)
    print(f"[INFO] Saved locally: {local_path}")
    
    # 7.1 Save mining log to separate file
    log_data = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "tf": tf,
        "exchange": exchange,
        "N_setups": N,
        "base_P_UP": round(base_P_UP, 4),
        "min_edge_threshold": round(min_edge_threshold, 4),
        "min_support_abs": min_support_abs,
        "n_candidates": len(rule_setups),
        "n_accepted": len(candidates),
        "rejected_by_support": rejected_by_support,
        "rejected_by_edge": rejected_by_edge
    }
    log_filename = f"{clean_symbol}_{clean_tf}_{clean_ex}_mining_log.json"
    log_path = Path(__file__).parent / "data" / log_filename
    with open(log_path, "w") as f:
        json.dump(log_data, f, indent=2)
    print(f"[INFO] Mining log: {log_path}")
    
    # 8. Save to Supabase
    url, key = load_secrets()
    supabase: Client = create_client(url, key)
    
    artifact_key = f"rules_stats_{clean_symbol}_{clean_tf}_{clean_ex}"
    
    record = {
        "artifact_key": artifact_key,
        "version": BUILD_VERSION,
        "patchlog_version": PATCHLOG_VERSION,
        "data_json": artifact,
        "meta": {
            "symbol": symbol,
            "tf": tf,
            "exchange": exchange,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "n_rules": len(candidates),
        }
    }
    
    try:
        supabase.table("training_artifacts")\
            .upsert(record, on_conflict="artifact_key,version")\
            .execute()
        print(f"[INFO] Saved to Supabase: {artifact_key}")
    except Exception as e:
        print(f"[WARN] Supabase save failed: {e}")
    
    print(f"[OK] STATS rules mining complete. Found {len(candidates)} rules.")
    
    # Detailed message for UI
    msg = (
        f"✓ {len(candidates)} правил | "
        f"Кандидатов: {len(rule_setups)} | "
        f"Откл. support<{min_support_abs}: {rejected_by_support} | "
        f"Откл. edge<{min_edge_threshold:.4f}: {rejected_by_edge} | "
        f"P(UP)={base_P_UP:.1%}"
    )
    return True, msg


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python stage6_mine_stats.py <symbol> <tf> <exchange>")
        print("Example: python stage6_mine_stats.py ETH 1D Binance")
        sys.exit(1)
    
    symbol = sys.argv[1]
    tf = sys.argv[2]
    exchange = sys.argv[3]
    
    success, error = run_mine_stats(symbol, tf, exchange)
    
    if not success:
        print(f"[ERROR] {error}")
        sys.exit(1)
