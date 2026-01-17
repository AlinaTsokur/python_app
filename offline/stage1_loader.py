import os
import sys
import json
import tomllib
from pathlib import Path
from supabase import create_client, Client
import math

# Core numerical fields that must NOT be None
# CORE_STATE обязательные поля (PATCH: NULL/NaN -> DROP setup)
CORE_FIELDS_CHECK = [
    "price_sign",
    "cvd_sign",
    "cvd_pct",
    "clv_pct",
    "oi_set",
    "oi_unload",
    "oi_counter",
    "oi_in_sens",
]

# Valid domains for CORE_STATE fields
VALID_SIGN = {-1, 0, 1}
VALID_BOOL = {0, 1, True, False}

# Valid y_size labels (case-insensitive) + normalization to ТЗ canon {S,M,L}
Y_SIZE_MAP = {
    "WEAK": "S",
    "MEDIUM": "M",
    "STRONG": "L",
    # Если прилетит уже в каноне:
    "S": "S",
    "M": "M",
    "L": "L",
}

def load_secrets():
    """Load Supabase credentials from .streamlit/secrets.toml (Single Source of Truth)."""
    secrets_path = Path(__file__).parent.parent / ".streamlit/secrets.toml"
    if not secrets_path.exists():
        raise FileNotFoundError(f"Secrets file not found at: {secrets_path}")
    
    with open(secrets_path, "rb") as f:
        secrets = tomllib.load(f)
        
    return secrets["SUPABASE_URL"], secrets["SUPABASE_KEY"]

def is_valid_segment(seg):
    """
    STRICT FILTERING LOGIC (per ТЗ v2.1):
    Priority order:
    1. CORE_MISSING: Any essential CORE_STATE field is missing or None.
    2. NAN_PRESENT: Any numerical field is NaN.
    3. LABEL checks, schema checks, etc.
    """
    # 0. Extract data from segment
    data = seg.get("data")
    if data is None:
        return False, "NO_DATA_JSON"
    
    # STRICT schema: data must contain CONTEXT.DATA (list of candles)
    if not isinstance(data, dict):
        return False, "BAD_SCHEMA: data_not_dict"

    context = data.get("CONTEXT")
    if not isinstance(context, dict):
        return False, "BAD_SCHEMA: missing_CONTEXT"

    candles = context.get("DATA")
    if not isinstance(candles, list) or len(candles) == 0:
        return False, "NO_CANDLES"

    # Segment length limit
    if len(candles) > 30:
        return False, "DROP_SEGMENT_TOO_LONG"
    
    # === PRIORITY 1: CORE_STATE checks (per ТЗ) ===
    for i, c in enumerate(candles):
        if not isinstance(c, dict):
            return False, f"BAD_SCHEMA: candle_not_dict at idx {i}"

        # 1) CORE_STATE: NULL/отсутствует -> DROP весь сетап
        for field in CORE_FIELDS_CHECK:
            if field not in c or c.get(field) is None:
                ts = c.get("ts")
                return False, f"DROP_CORE_MISSING: {field} at idx {i} ts={ts}"
        
        # 1.5) CORE_STATE domain validation
        # price_sign, cvd_sign must be in {-1, 0, 1}
        if c.get("price_sign") not in VALID_SIGN:
            ts = c.get("ts")
            return False, f"DROP_CORE_INVALID_DOMAIN: price_sign={c.get('price_sign')} at idx {i} ts={ts}"
        if c.get("cvd_sign") not in VALID_SIGN:
            ts = c.get("ts")
            return False, f"DROP_CORE_INVALID_DOMAIN: cvd_sign={c.get('cvd_sign')} at idx {i} ts={ts}"
        
        # OI flags must be boolean-like
        for flag in ["oi_set", "oi_unload", "oi_counter", "oi_in_sens"]:
            if c.get(flag) not in VALID_BOOL:
                ts = c.get("ts")
                return False, f"DROP_CORE_INVALID_DOMAIN: {flag}={c.get(flag)} at idx {i} ts={ts}"

    # === PRIORITY 2: NaN checks (per ТЗ) ===
    for i, c in enumerate(candles):
        # CONTRACT: Supabase JSON -> only JSON primitive types (dict/list/str/int/float/bool/None).
        # Therefore NaN can only appear as float('nan'); we check only float.
        for k, v in c.items():
            if isinstance(v, float) and math.isnan(v):
                ts = c.get("ts")
                return False, f"DROP_NAN_PRESENT: {k} at idx {i} ts={ts}"

    # === PRIORITY 3: Label checks ===
    # Check y_dir label (required for training)
    y_dir = seg.get("y_dir")
    if y_dir is None:
        return False, "DROP_LABEL_MISSING: y_dir"
    
    # Validate y_dir domain (case-insensitive) and normalize
    y_dir_upper = str(y_dir).upper()
    if y_dir_upper not in ("UP", "DOWN"):
        return False, f"DROP_LABEL_INVALID: y_dir={y_dir}"
    seg["y_dir"] = y_dir_upper  # Normalize to uppercase
    
    # Check y_size label (optional but validated and normalized if present)
    y_size = seg.get("y_size")
    if y_size is not None:
        y_size_upper = str(y_size).upper()
        if y_size_upper not in Y_SIZE_MAP:
            return False, f"DROP_LABEL_INVALID: y_size={y_size}"
        seg["y_size"] = Y_SIZE_MAP[y_size_upper]  # Normalize to ТЗ canon {S,M,L}
    
    # === PRIORITY 4: META checks (STRICT - per ТЗ v2.1 contract) ===
    meta = data.get("META")
    if not isinstance(meta, dict):
        return False, "BAD_META: missing_META"
    
    imp_idx = meta.get("impulse_split_index")
    if imp_idx is None:
        return False, "BAD_META: missing_impulse_split_index"
    
    if imp_idx != (len(candles) - 1):
        return False, f"BAD_META: impulse_split_index={imp_idx} expected={len(candles)-1}"

    return True, None

def run_pipeline(symbol, tf, exchange="Binance", limit=10000):
    """
    Executes Step 1.1: Load & Filter Data.
    Returns: (success: bool, message: str, count: int)
    """
    print(f"[START] Loading data for {symbol} {tf}...")
    
    # 1. Setup
    try:
        url, key = load_secrets()
        supabase: Client = create_client(url, key)
    except Exception as e:
        return False, f"Connection Failed: {e}", 0

    # 2. Extract (Fetch)
    try:
        print("[SUPABASE] Querying segments...")
        res = supabase.table("segments")\
            .select("*")\
            .eq("symbol", symbol)\
            .eq("tf", tf)\
            .eq("exchange", exchange)\
            .limit(limit)\
            .execute()
        
        raw_segments = res.data or []
    except Exception as e:
        return False, f"Query Failed: {e}", 0

    print(f"[INFO] Fetched {len(raw_segments)} raw segments.")
    
    # 3. Transform & Filter (Strict Mode)
    clean_segments = []
    dropped_log = []  # Detailed audit log
    dropped_stats = {
        "NO_DATA_JSON": 0,
        "BAD_SCHEMA": 0,
        "BAD_META": 0,
        "NO_CANDLES": 0,
        "DROP_SEGMENT_TOO_LONG": 0,
        "DROP_CORE_MISSING": 0,
        "DROP_CORE_INVALID_DOMAIN": 0,
        "DROP_NAN_PRESENT": 0,
        "DROP_LABEL_MISSING": 0,
        "DROP_LABEL_INVALID": 0,
    }
    
    for seg in raw_segments:
        is_valid, reason = is_valid_segment(seg)
        if is_valid:
            seg["setup_status"] = "USE"
            clean_segments.append(seg)
        else:
            seg["setup_status"] = reason.split(":")[0]
            seg["drop_reason"] = reason
            
            category = reason.split(":")[0]
            if category in dropped_stats:
                dropped_stats[category] += 1
            else:
                dropped_stats[category] = dropped_stats.get(category, 0) + 1
            
            # Audit log entry
            dropped_log.append({
                "segment_id": seg.get("id"),
                "reason": reason,
                "symbol": seg.get("symbol"),
                "tf": seg.get("tf"),
            })

    # 4. Load (Save Locally)
    clean_symbol = symbol.replace("/", "").replace(":", "")
    clean_tf = tf.replace("/", "")
    clean_ex = exchange.replace("/", "")
    
    outfile = Path(__file__).parent / "data" / f"{clean_symbol}_{clean_tf}_{clean_ex}_clean.json"
    dropped_file = Path(__file__).parent / "data" / f"{clean_symbol}_{clean_tf}_{clean_ex}_dropped.json"
    
    outfile.parent.mkdir(parents=True, exist_ok=True)
    
    with open(outfile, "w") as f:
        json.dump(clean_segments, f, indent=2, default=str)
    
    # Save drop log for audit
    with open(dropped_file, "w") as f:
        json.dump({"stats": dropped_stats, "details": dropped_log}, f, indent=2, default=str)
    
    # Warning for insufficient data
    if len(clean_segments) < 10:
        print(f"[WARN] Too few samples ({len(clean_segments)}) for reliable training!")
        
    report = f"Загружено: {len(raw_segments)} | Чистых: {len(clean_segments)} | Отброшено: {len(raw_segments)-len(clean_segments)}"
    return True, report, len(clean_segments)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 4:
        print("Usage: python stage1_loader.py <symbol> <tf> <exchange>")
        print("Example: python stage1_loader.py ETH 1D Binance")
        sys.exit(1)
    
    symbol = sys.argv[1]
    tf = sys.argv[2]
    exchange = sys.argv[3]
    
    res, msg, cnt = run_pipeline(symbol, tf, exchange)
    print(f"[{'OK' if res else 'ERROR'}] {msg}")
