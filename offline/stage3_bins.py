"""
Stage 3: Binning (3_build_bins.py)
Builds global quantile thresholds (Q1-Q5) for all features.

Per ТЗ v2.1:
- CORE: cvd_pct, clv_pct
- BOOST: vol_top1_share, vol_rank, doi_top1_share, doi_rank, liq_top1_share, liq_rank
- Quantiles: q20, q40, q60, q80 via numpy.quantile(method='linear')
- NULL values are skipped in quantile calculation
- Artifact saved locally + Supabase upsert
"""

import json
import os
import math
import tomllib
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
from supabase import create_client, Client

# --- CONFIG ---
PATCHLOG_VERSION = "PATCHLOG_v2.1@2026-01-10"
BUILD_VERSION = datetime.now(timezone.utc).strftime("%Y-%m-%d")  # Auto-version by date

# Fields to bin
CORE_FIELDS = ["cvd_pct", "clv_pct"]
BOOST_FIELDS = ["vol_top1_share", "vol_rank", "doi_top1_share", "doi_rank", "liq_top1_share", "liq_rank"]
ALL_FIELDS = CORE_FIELDS + BOOST_FIELDS

# Rank fields require more samples for stable bins (prevent pattern fragmentation)
RANK_FIELDS = ["vol_rank", "doi_rank", "liq_rank"]
MIN_SAMPLE_FOR_BINS_RANK = 50

# Minimum sample warning threshold for other fields (not blocking!)
MIN_SAMPLE_WARNING = 20


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


def load_features(symbol, tf, exchange):
    """Load features from Stage 2 output."""
    clean_symbol = symbol.replace("/", "").replace(":", "")
    clean_tf = tf.replace("/", "")
    clean_ex = exchange.replace("/", "")
    filepath = Path(__file__).parent / "data" / f"{clean_symbol}_{clean_tf}_{clean_ex}_features.json"
    
    if not filepath.exists():
        return None, f"File not found: {filepath}"
    
    with open(filepath, "r") as f:
        data = json.load(f)
    return data, None


def collect_pools(segments):
    """
    Collect all values for each field from all steps of all segments.
    NULL/None values are skipped.
    """
    pools = {field: [] for field in ALL_FIELDS}
    
    for seg in segments:
        for step in seg.get("steps", []):
            core = step.get("core_state", {})
            boost = step.get("boost", {})
            
            # CORE fields (skip None and NaN)
            for field in CORE_FIELDS:
                val = core.get(field)
                if val is not None and not (isinstance(val, float) and math.isnan(val)):
                    pools[field].append(val)
            
            # BOOST fields (skip None and NaN)
            for field in BOOST_FIELDS:
                val = boost.get(field)
                if val is not None and not (isinstance(val, float) and math.isnan(val)):
                    pools[field].append(val)
    
    return pools


def calculate_quantiles(pools):
    """
    Calculate q20/q40/q60/q80 for each field.
    - len == 0 → None
    - len > 0 → calculate (with WARNING if len < 20)
    """
    bins = {}
    warnings = []
    
    for field, values in pools.items():
        n_samples = len(values)
        
        if n_samples == 0:
            bins[field] = None
            warnings.append(f"No data for field={field}")
            continue
        
        # Rank fields require more samples for stable bins
        if field in RANK_FIELDS and n_samples < MIN_SAMPLE_FOR_BINS_RANK:
            bins[field] = None
            warnings.append(f"RANK_DISABLED: field={field} (n={n_samples} < {MIN_SAMPLE_FOR_BINS_RANK})")
            continue
        
        if n_samples < MIN_SAMPLE_WARNING:
            warnings.append(f"WARNING: low sample count for field={field}, n={n_samples}; quantiles may be unstable")
        
        # Convert to numpy array with explicit dtype (defensive programming)
        arr = np.asarray(values, dtype=float)
        
        # Calculate quantiles - strictly per ТЗ: method='linear'
        q = np.quantile(arr, q=[0.20, 0.40, 0.60, 0.80], method='linear')
        
        bins[field] = {
            "q20": float(q[0]),
            "q40": float(q[1]),
            "q60": float(q[2]),
            "q80": float(q[3]),
            "n_samples": len(values),
            "min": float(arr.min()),
            "max": float(arr.max()),
        }
    
    return bins, warnings


def save_to_supabase(bins_data, symbol, tf, exchange):
    """
    Save bins artifact to Supabase using upsert.
    artifact_key = bins_{symbol}_{tf}_{exchange}
    """
    try:
        url, key = load_secrets()
        supabase: Client = create_client(url, key)
    except Exception as e:
        return False, f"Supabase connection failed: {e}"
    
    clean_symbol = symbol.replace("/", "").replace(":", "")
    clean_tf = tf.replace("/", "")
    clean_ex = exchange.replace("/", "")
    
    artifact_key = f"bins_{clean_symbol}_{clean_tf}_{clean_ex}"
    
    record = {
        "artifact_key": artifact_key,
        "version": BUILD_VERSION,
        "patchlog_version": PATCHLOG_VERSION,
        "data_json": bins_data,
        "meta": {
            "symbol": symbol,
            "tf": tf,
            "exchange": exchange,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "n_fields": len([f for f in bins_data["fields"] if bins_data["fields"][f] is not None]),
        }
    }
    
    try:
        # Upsert for idempotency (composite PK: artifact_key + version)
        res = supabase.table("training_artifacts")\
            .upsert(record, on_conflict="artifact_key,version")\
            .execute()
        return True, f"Saved to Supabase: {artifact_key}"
    except Exception as e:
        return False, f"Supabase save failed: {e}"


def run_binning(symbol, tf, exchange="Binance"):
    """
    Executes Step 1.3: Build Bins.
    Returns: (success: bool, message: str)
    """
    print(f"[START] Building bins for {symbol} {tf} ({exchange})...")
    
    # 1. Load features
    segments, err = load_features(symbol, tf, exchange)
    if err:
        return False, err
    if segments is None or len(segments) == 0:
        return False, "No segments loaded (empty features file)"
    
    print(f"[INFO] Loaded {len(segments)} segments.")
    
    # 2. Collect pools
    pools = collect_pools(segments)
    total_samples = sum(len(v) for v in pools.values())
    print(f"[INFO] Collected {total_samples} total samples across {len(ALL_FIELDS)} fields.")
    
    # 3. Calculate quantiles
    bins, warnings = calculate_quantiles(pools)
    
    # Log warnings
    for w in warnings:
        print(f"[{w.split(':')[0]}] {w}")
    
    # 4. Build final artifact
    clean_symbol = symbol.replace("/", "").replace(":", "")
    clean_tf = tf.replace("/", "")
    clean_ex = exchange.replace("/", "")
    
    bins_artifact = {
        "version": BUILD_VERSION,
        "patchlog_version": PATCHLOG_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "tf": tf,
        "exchange": exchange,
        "fields": bins,
    }
    
    # 5. Save locally
    outfile = Path(__file__).parent / "data" / f"{clean_symbol}_{clean_tf}_{clean_ex}_bins.json"
    outfile.parent.mkdir(parents=True, exist_ok=True)
    
    with open(outfile, "w") as f:
        json.dump(bins_artifact, f, indent=2)
    print(f"[INFO] Saved locally: {outfile}")
    
    # 6. Save to Supabase
    success, msg = save_to_supabase(bins_artifact, symbol, tf, exchange)
    if success:
        print(f"[INFO] {msg}")
    else:
        print(f"[WARN] {msg}")
    
    # 7. Summary
    valid_fields = len([f for f in bins if bins[f] is not None])
    report = f"Создано бинов: {valid_fields}/{len(ALL_FIELDS)} полей."
    
    return True, report


if __name__ == "__main__":
    res, msg = run_binning("ETH", "1D", "Binance")
    print(f"[{'OK' if res else 'ERROR'}] {msg}")
