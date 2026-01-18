"""
Stage 5: Build STATS Bins (5_bins_stats.py)
Builds global quantile thresholds for STATS features.

Per ТЗ v2.1 PATCH-03/04/08:
- Input: _clean.json (Stage 1 output with RAW candle data)
- 8 STATS fields calculated via stats_calc.py
- Quantiles: q20, q40, q60, q80 via numpy.quantile(method='linear') [PATCH-04]
- NULL values are skipped in quantile calculation [PATCH-03]
- net_oi_change: first/last only [PATCH-08]
- Empty pool → raise ValueError (strict ТЗ compliance)
- Artifact saved locally + Supabase upsert
"""

import json
import tomllib
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
from supabase import create_client, Client

# Import shared STATS calculations
try:
    from .stats_calc import STATS_FIELDS, MAX_SEGMENT_LENGTH, calculate_stats
except ImportError:
    from stats_calc import STATS_FIELDS, MAX_SEGMENT_LENGTH, calculate_stats

# --- CONFIG ---
PATCHLOG_VERSION = "PATCHLOG_v2.1@2026-01-10"
BUILD_VERSION = datetime.now(timezone.utc).strftime("%Y-%m-%d")


def load_secrets():
    """Load Supabase credentials from .streamlit/secrets.toml."""
    secrets_path = Path(__file__).parent.parent / ".streamlit/secrets.toml"
    if not secrets_path.exists():
        raise FileNotFoundError(f"Secrets file not found at: {secrets_path}")
    
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
    
    return segments, None


def run_bins_stats(symbol: str, tf: str, exchange: str):
    """Main function to build STATS bins."""
    print(f"[START] Building STATS bins for {symbol} {tf} ({exchange})...")
    
    # 1. Load clean data
    segments, err = load_clean_data(symbol, tf, exchange)
    if err:
        return False, err
    
    # Type validation (defensive check)
    if not isinstance(segments, list):
        raise ValueError(f"Clean file must contain list of segments, got {type(segments).__name__}")
    
    print(f"[INFO] Loaded {len(segments)} segments.")
    
    # 2. Initialize pools for each STATS field
    pools = {field: [] for field in STATS_FIELDS}
    
    # 3. Process each segment
    processed_segments = 0
    processed_steps = 0
    
    for seg_idx, segment in enumerate(segments):
        # Get RAW candles from _clean.json structure
        raw_candles = segment.get("data", {}).get("CONTEXT", {}).get("DATA", [])
        
        # Type validation (defensive check)
        if not isinstance(raw_candles, list):
            raise ValueError(f"Segment {seg_idx}: CONTEXT.DATA must be a list, got {type(raw_candles).__name__}")
        
        # Strict check: Stage 1 contract violation
        if len(raw_candles) > MAX_SEGMENT_LENGTH:
            raise ValueError(
                f"Segment {seg_idx} too long ({len(raw_candles)} > {MAX_SEGMENT_LENGTH}) - Stage 1 bug"
            )
        
        if len(raw_candles) == 0:
            continue
        
        processed_segments += 1
        
        # Process each step (sliding window up to current position)
        for i in range(len(raw_candles)):
            # Buffer: last min(i+1, MAX_SEGMENT_LENGTH) candles
            start = max(0, i - (MAX_SEGMENT_LENGTH - 1))
            buffer = raw_candles[start:i+1]
            
            # Calculate all STATS
            stats = calculate_stats(buffer)
            
            # Add to pools (skip None and NaN values, iterate by STATS_FIELDS for contract)
            for field in STATS_FIELDS:
                value = stats.get(field)
                if value is None:
                    continue
                try:
                    if np.isnan(value):
                        continue
                except TypeError:
                    pass
                pools[field].append(value)
            
            processed_steps += 1
    
    print(f"[INFO] Processed {processed_segments} segments, {processed_steps} steps.")
    
    # 4. Calculate quantiles (PATCH-04)
    bins_stats = {}
    for field in STATS_FIELDS:
        values = pools[field]
        
        # Strict ТЗ: empty pool is error
        if len(values) == 0:
            raise ValueError(f"Empty pool for field '{field}' - no valid data")
        
        arr = np.asarray(values, dtype=float)  # Ensure float array
        quantiles = np.quantile(arr, [0.20, 0.40, 0.60, 0.80], method='linear')
        bins_stats[field] = {
            "q20": float(quantiles[0]),
            "q40": float(quantiles[1]),
            "q60": float(quantiles[2]),
            "q80": float(quantiles[3]),
        }
        
        print(f"[INFO] {field}: {len(values)} values, Q20={quantiles[0]:.4f}, Q80={quantiles[3]:.4f}")
    
    # 5. Build artifact
    clean_symbol = symbol.replace("/", "").replace(":", "")
    clean_tf = tf.replace("/", "")
    clean_ex = exchange.replace("/", "")
    
    artifact = {
        "version": BUILD_VERSION,
        "patchlog_version": PATCHLOG_VERSION,
        "symbol": symbol,
        "tf": tf,
        "exchange": exchange,
        "fields": bins_stats,
    }
    
    # 6. Save locally
    local_filename = f"{clean_symbol}_{clean_tf}_{clean_ex}_bins_stats.json"
    local_path = Path(__file__).parent / "data" / local_filename
    
    with open(local_path, "w") as f:
        json.dump(artifact, f, indent=2)
    print(f"[INFO] Saved locally: {local_path}")
    
    # 7. Save to Supabase
    url, key = load_secrets()
    supabase: Client = create_client(url, key)
    
    artifact_key = f"bins_stats_{clean_symbol}_{clean_tf}_{clean_ex}"
    
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
            "n_fields": len(bins_stats),
        }
    }
    
    try:
        supabase.table("training_artifacts")\
            .upsert(record, on_conflict="artifact_key,version")\
            .execute()
        print(f"[INFO] Saved to Supabase: {artifact_key}")
    except Exception as e:
        print(f"[WARN] Supabase save failed: {e}")
    
    print(f"[OK] STATS bins built successfully.")
    
    return True, f"Создано {len(bins_stats)} STATS бинов"


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 4:
        print("Usage: python stage5_bins_stats.py <symbol> <tf> <exchange>")
        print("Example: python stage5_bins_stats.py ETH 1D Binance")
        sys.exit(1)
    
    symbol = sys.argv[1]
    tf = sys.argv[2]
    exchange = sys.argv[3]
    
    success, error = run_bins_stats(symbol, tf, exchange)
    
    if not success:
        print(f"[ERROR] {error}")
        sys.exit(1)
