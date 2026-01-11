"""
Stage 4: Mining Rules (4_mine_rules_data.py)
Finds contiguous patterns in CORE_STATE that predict impulse direction.

Per ТЗ v2.1 + PATCH-08:
- Token format: DIV={div_type}|F={oi_flags}|CVD={Qx}|CLV={Qx}
- Contiguous patterns only (no gaps)
- Support/wins by unique setups
- Beta-prior smoothing for probabilities
- TTI histogram with 1/M weighting
- Coverage-based greedy selection

5-Pass Architecture:
1. Mine patterns + support/wins (last_seen_id optimization)
2. Compute edge + filter candidates
3. Greedy selection with coverage
4. TTI only for selected rules
5. Build index for online matching
"""

import json
import math
import tomllib
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict
from supabase import create_client, Client

# --- CONFIG ---
PATCHLOG_VERSION = "PATCHLOG_v2.1@2026-01-10"
BUILD_VERSION = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# Mining parameters (per ТЗ)
MAX_PATTERN_LENGTH = 15
PRIOR_STRENGTH = 10


# --- HELPERS ---

def load_secrets():
    """Load Supabase credentials from .streamlit/secrets.toml."""
    secrets_path = Path(__file__).parent.parent / ".streamlit/secrets.toml"
    if not secrets_path.exists():
        raise FileNotFoundError(f"Secrets file not found at: {secrets_path}")
    
    with open(secrets_path, "rb") as f:
        secrets = tomllib.load(f)
    
    return secrets["SUPABASE_URL"], secrets["SUPABASE_KEY"]


def load_features(symbol, tf, exchange):
    """Load features from Stage 2 output."""
    clean_symbol = symbol.replace("/", "").replace(":", "")
    clean_tf = tf.replace("/", "")
    clean_ex = exchange.replace("/", "")
    filepath = Path(f"offline/data/{clean_symbol}_{clean_tf}_{clean_ex}_features.json")
    
    if not filepath.exists():
        return None, f"File not found: {filepath}"
    
    with open(filepath, "r") as f:
        data = json.load(f)
    return data, None


def load_bins(symbol, tf, exchange):
    """Load bins from Stage 3 output."""
    clean_symbol = symbol.replace("/", "").replace(":", "")
    clean_tf = tf.replace("/", "")
    clean_ex = exchange.replace("/", "")
    filepath = Path(f"offline/data/{clean_symbol}_{clean_tf}_{clean_ex}_bins.json")
    
    if not filepath.exists():
        return None, f"File not found: {filepath}"
    
    with open(filepath, "r") as f:
        data = json.load(f)
    return data, None


def load_clean_data(symbol, tf, exchange):
    """Load clean data from Stage 1 output (for segment metadata: ts_start, ts_end)."""
    clean_symbol = symbol.replace("/", "").replace(":", "")
    clean_tf = tf.replace("/", "")
    clean_ex = exchange.replace("/", "")
    filepath = Path(f"offline/data/{clean_symbol}_{clean_tf}_{clean_ex}_clean.json")
    
    if not filepath.exists():
        return None, f"File not found: {filepath}"
    
    with open(filepath, "r") as f:
        data = json.load(f)
    return data, None


def assign_bin(value, thresholds):
    """Assign Q1-Q5 based on quantile thresholds."""
    # Robust handling: convert to float, handle None/NaN/strings/numpy
    if value is None or thresholds is None:
        return None
    
    try:
        v = float(value)
    except (ValueError, TypeError):
        return None
    
    if math.isnan(v):
        return None
    
    # Safe threshold extraction with float conversion
    try:
        q20 = float(thresholds.get("q20"))
        q40 = float(thresholds.get("q40"))
        q60 = float(thresholds.get("q60"))
        q80 = float(thresholds.get("q80"))
    except (ValueError, TypeError):
        return None
    
    if v <= q20:
        return "Q1"
    elif v <= q40:
        return "Q2"
    elif v <= q60:
        return "Q3"
    elif v <= q80:
        return "Q4"
    else:
        return "Q5"


def tokenize_state(step, bins_fields):
    """
    PATCH-08: Canonical token format.
    DIV={div_type}|F={oi_flags}|CVD={Qx}|CLV={Qx}
    Returns None if any required field is missing (segment will be dropped).
    """
    core = step.get("core_state")
    if core is None:
        return None
    
    # Use .get() to prevent KeyError on missing fields
    div_type = core.get("div_type")
    oi_flags_raw = core.get("oi_flags")
    
    if div_type is None or oi_flags_raw is None:
        return None
    
    # Normalize oi_flags to int for stable token format
    try:
        oi_flags = int(oi_flags_raw)
    except (ValueError, TypeError):
        return None
    
    cvd_bin = assign_bin(core.get("cvd_pct"), bins_fields.get("cvd_pct"))
    clv_bin = assign_bin(core.get("clv_pct"), bins_fields.get("clv_pct"))
    
    # Don't allow None in token - violates PATCH-08
    if cvd_bin is None or clv_bin is None:
        return None
    
    return f"DIV={div_type}|F={oi_flags}|CVD={cvd_bin}|CLV={clv_bin}"


def find_all_matches(pattern, seq):
    """Return end_positions of all pattern occurrences."""
    matches = []
    for start in range(len(seq) - len(pattern) + 1):
        if tuple(seq[start:start + len(pattern)]) == pattern:
            matches.append(start + len(pattern) - 1)  # end position
    return matches


def compute_eta_probs(tti_hist):
    """Compute ETA bucket probabilities with +1/+3 smoothing."""
    # NEAR: tti ∈ {0, 1}
    count_NEAR = sum(tti_hist.get(t, 0) for t in [0, 1])
    # MID: tti ∈ {2, 3, 4}
    count_MID = sum(tti_hist.get(t, 0) for t in [2, 3, 4])
    # EARLY: tti >= 5 (no upper bound!)
    count_EARLY = sum(v for t, v in tti_hist.items() if t >= 5)
    
    total = count_NEAR + count_MID + count_EARLY
    
    return {
        "NEAR": (count_NEAR + 1) / (total + 3),
        "MID": (count_MID + 1) / (total + 3),
        "EARLY": (count_EARLY + 1) / (total + 3),
    }


def build_tti_histogram(pattern, sequences, setup_ids):
    """Build TTI histogram with 1/M weighting."""
    tti_hist = defaultdict(float)
    
    for seq, seg_id in zip(sequences, setup_ids):
        K = len(seq)
        matches = find_all_matches(pattern, seq)
        M = len(matches)
        
        if M == 0:
            continue
        
        weight = 1.0 / M  # prevent single setup domination
        for end_pos in matches:
            tti = K - 1 - end_pos  # candles until impulse
            tti_hist[tti] += weight
    
    return tti_hist


# --- PASS 1: Mine patterns + support/wins ---

# Safety limit for patterns in memory (prevents OOM on large datasets)
MAX_PATTERNS_IN_MEMORY = 2_000_000

def mine_patterns_apriori(sequences, setup_ids, y_dirs, min_support, max_len):
    """
    Apriori-like contiguous n-gram mining.
    Only extends patterns whose prefix already has min_support.
    This dramatically reduces memory usage on large datasets.
    """
    # Level 1: Mine 1-grams
    patterns = {}  # pattern -> {support, wins_up, last_seen_id}
    
    for seq, seg_id, y_dir in zip(sequences, setup_ids, y_dirs):
        for token in seq:
            pattern = (token,)
            if pattern not in patterns:
                # Check limit on EVERY add
                if len(patterns) >= MAX_PATTERNS_IN_MEMORY:
                    print(f"[WARN] Pattern limit reached ({MAX_PATTERNS_IN_MEMORY}) at level 1")
                    return {p: v for p, v in patterns.items() if v["support"] >= min_support}
                patterns[pattern] = {"support": 0, "wins_up": 0, "last_seen_id": None}
            
            p = patterns[pattern]
            if p["last_seen_id"] != seg_id:
                p["support"] += 1
                if y_dir == "UP":
                    p["wins_up"] += 1
                p["last_seen_id"] = seg_id
    
    # Build frequent set for level 1 (used for pruning)
    frequent_prev = {p for p, v in patterns.items() if v["support"] >= min_support}
    print(f"[INFO] Level 1: {len(frequent_prev)} frequent 1-grams")
    
    # Levels 2..max_len: Apriori extension
    for length in range(2, max_len + 1):
        new_patterns_count = 0
        
        for seq, seg_id, y_dir in zip(sequences, setup_ids, y_dirs):
            for start in range(len(seq) - length + 1):
                pattern = tuple(seq[start:start + length])
                
                # Apriori pruning: only extend if prefix is frequent
                prefix = pattern[:-1]
                if prefix not in frequent_prev:
                    continue
                
                if pattern not in patterns:
                    # Check limit on EVERY add
                    if len(patterns) >= MAX_PATTERNS_IN_MEMORY:
                        print(f"[WARN] Pattern limit reached ({MAX_PATTERNS_IN_MEMORY}) at level {length}")
                        return {p: v for p, v in patterns.items() if v["support"] >= min_support}
                    patterns[pattern] = {"support": 0, "wins_up": 0, "last_seen_id": None}
                    new_patterns_count += 1
                
                p = patterns[pattern]
                if p["last_seen_id"] != seg_id:
                    p["support"] += 1
                    if y_dir == "UP":
                        p["wins_up"] += 1
                    p["last_seen_id"] = seg_id
        
        # Build frequent set for this level (for next iteration)
        frequent_at_level = {p for p, v in patterns.items() if len(p) == length and v["support"] >= min_support}
        print(f"[INFO] Level {length}: {new_patterns_count} new, {len(frequent_at_level)} frequent")
        
        if len(frequent_at_level) == 0:
            break  # No point extending further
        
        frequent_prev = frequent_at_level
    
    # Filter all by min_support
    return {p: v for p, v in patterns.items() if v["support"] >= min_support}


# --- MAIN LOGIC ---


def run_mining(symbol, tf, exchange="Binance"):
    """
    Execute Step 1.4: Mine Rules.
    Returns: (success: bool, message: str)
    """
    print(f"[START] Mining rules for {symbol} {tf} ({exchange})...")
    
    # 1. Load data
    segments, err = load_features(symbol, tf, exchange)
    if err:
        return False, err
    if not segments or len(segments) == 0:
        return False, "No segments loaded"
    
    bins_data, err = load_bins(symbol, tf, exchange)
    if err:
        return False, err
    
    bins_fields = bins_data.get("fields", {})
    
    # Fail fast: CORE bins must exist
    if bins_fields.get("cvd_pct") is None or bins_fields.get("clv_pct") is None:
        return False, "CORE bins missing (cvd_pct or clv_pct) - cannot proceed"
    
    print(f"[INFO] Loaded {len(segments)} segments.")
    
    # 3. Tokenize all steps
    sequences = []
    setup_base_ids = []  # Original IDs for JOIN with segments table
    y_dirs = []
    # Counters for logging
    skipped_y_dir = 0
    skipped_tokenization = 0
    skipped_no_id = 0
    # Track seen IDs for uniqueness check
    seen_ids = set()
    
    for idx, seg in enumerate(segments):
        # Require segment id for JOIN with segments table
        base_id = seg.get("id")
        if not base_id:
            skipped_no_id += 1
            continue
        
        # Strict uniqueness check - fail on duplicates
        if base_id in seen_ids:
            return False, f"DUPLICATE_ID: segment id '{base_id}' appears multiple times. IDs must be unique."
        
        # Validate y_dir - don't use default
        y_dir = seg.get("y_dir")
        if y_dir not in ("UP", "DOWN"):
            skipped_y_dir += 1
            continue
        
        # Tokenize all steps - if ANY fails, DROP entire segment (contiguous requirement)
        seq = []
        segment_valid = True
        for step in seg.get("steps", []):
            token = tokenize_state(step, bins_fields)
            if token is None:
                segment_valid = False
                break
            seq.append(token)
        
        if not segment_valid:
            skipped_tokenization += 1
            continue
        
        if seq:  # non-empty
            sequences.append(seq)
            seen_ids.add(base_id)
            setup_base_ids.append(base_id)
            y_dirs.append(y_dir)
    
    # Log skipped segments
    if skipped_no_id > 0:
        print(f"[WARN] Skipped {skipped_no_id} segments without id")
    if skipped_y_dir > 0:
        print(f"[WARN] Skipped {skipped_y_dir} segments with invalid y_dir")
    if skipped_tokenization > 0:
        print(f"[WARN] Skipped {skipped_tokenization} segments with tokenization errors")
    
    print(f"[INFO] Tokenized {len(sequences)} sequences.")
    
    # Fix #2: Recalculate N from actual used sequences
    N = len(sequences)
    if N == 0:
        return False, "No valid sequences after tokenization"
    
    # Recalculate adaptive parameters with actual N
    min_support_abs = max(3, math.ceil(0.02 * N))
    min_edge_threshold = max(0.03, 1 / math.sqrt(N))
    K_rules = min(50, max(10, N // 10))
    
    print(f"[INFO] Parameters (N={N}): min_support={min_support_abs}, min_edge={min_edge_threshold:.4f}, K_rules={K_rules}")
    
    # 4. Calculate base rate
    up_count = sum(1 for y in y_dirs if y == "UP")
    base_P_UP = up_count / N
    print(f"[INFO] Base P(UP) = {base_P_UP:.4f}")
    
    # 5. Pass 1: Mine patterns
    print("[PASS 1] Mining patterns...")
    patterns = mine_patterns_apriori(sequences, setup_base_ids, y_dirs, min_support_abs, MAX_PATTERN_LENGTH)
    print(f"[INFO] Found {len(patterns)} patterns with support >= {min_support_abs}")
    
    # 6. Pass 2: Compute edge + filter candidates
    print("[PASS 2] Computing edge and filtering candidates...")
    alpha_prior = base_P_UP * PRIOR_STRENGTH + 1
    beta_prior = (1 - base_P_UP) * PRIOR_STRENGTH + 1
    
    candidates = []
    rejected_by_edge = []  # Track rejected patterns for debug
    
    for pattern, stats in patterns.items():
        support = stats["support"]
        wins_up = stats["wins_up"]
        
        p_up_smooth = (wins_up + alpha_prior) / (support + alpha_prior + beta_prior)
        p_down_smooth = 1 - p_up_smooth
        
        edge_up = p_up_smooth - base_P_UP
        edge_down = p_down_smooth - (1 - base_P_UP)
        
        pattern_info = {
            "pattern": pattern,
            "support": support,
            "wins_up": wins_up,
            "wins_down": support - wins_up,
            "p_up_smooth": p_up_smooth,
            "p_down_smooth": p_down_smooth,
            "edge_up": edge_up,
            "edge_down": edge_down,
        }
        
        if max(edge_up, edge_down) >= min_edge_threshold:
            candidates.append(pattern_info)
        else:
            pattern_info["reason"] = "edge_below_threshold"
            pattern_info["reason_ru"] = "Преимущество ниже порога (паттерн недостаточно предсказателен)"
            pattern_info["threshold"] = min_edge_threshold
            rejected_by_edge.append(pattern_info)
    
    print(f"[INFO] {len(candidates)} candidates pass edge, {len(rejected_by_edge)} rejected by edge.")
    
    if not candidates:
        print("[WARN] No candidates found!")
        return True, "No rules found (edge threshold not met)"
    
    # 7. Sort and LIMIT candidates BEFORE building coverage_map (memory optimization)
    print("[PASS 2.5] Sorting and limiting candidates...")
    candidates.sort(key=lambda c: (
        -max(c["edge_up"], c["edge_down"]),
        -c["support"],
        -len(c["pattern"])
    ))
    
    MAX_CANDIDATES_FOR_COVERAGE = 2000
    if len(candidates) > MAX_CANDIDATES_FOR_COVERAGE:
        print(f"[INFO] Limiting candidates from {len(candidates)} to {MAX_CANDIDATES_FOR_COVERAGE}")
        candidates = candidates[:MAX_CANDIDATES_FOR_COVERAGE]
    
    # 8. Build coverage map ONLY for candidate_patterns (not rejected - saves memory)
    print("[PASS 2.5] Building coverage map...")
    candidate_patterns = {c["pattern"] for c in candidates}  # O(1) lookup
    coverage_map = {p: set() for p in candidate_patterns}
    
    for seq, base_id in zip(sequences, setup_base_ids):  # Use base_id for JOIN with segments
        for start in range(len(seq)):
            for length in range(1, min(MAX_PATTERN_LENGTH, len(seq) - start) + 1):
                pattern = tuple(seq[start:start + length])
                if pattern in candidate_patterns:
                    coverage_map[pattern].add(base_id)
    
    # 9. Pass 3: Greedy selection with coverage (now O(candidates) using prebuilt map)
    print("[PASS 3] Greedy selection with coverage...")
    
    covered_setups = set()
    selected_rules = []
    rejected_by_coverage = []  # Track patterns rejected due to no new coverage
    
    for c in candidates:
        if len(selected_rules) >= K_rules:
            # Remaining candidates are rejected due to K_rules limit
            c_copy = dict(c)
            c_copy["reason"] = "k_rules_limit"
            c_copy["reason_ru"] = "Достигнут лимит правил (уже выбрано максимальное количество)"
            rejected_by_coverage.append(c_copy)
            continue
        
        # Use prebuilt coverage_map instead of get_setups_with_pattern()
        setups_with_p = coverage_map.get(c["pattern"], set())
        new_coverage = setups_with_p - covered_setups
        
        if not new_coverage:
            c_copy = dict(c)
            c_copy["reason"] = "no_new_coverage"
            c_copy["reason_ru"] = "Нет нового покрытия (сетапы уже покрыты другими правилами)"
            c_copy["already_covered_by"] = list(covered_setups & setups_with_p)[:5]  # Sample
            rejected_by_coverage.append(c_copy)
            continue
        
        # DON'T mutate c - store coverage separately for debug output
        selected_rules.append(c)
        covered_setups |= setups_with_p
    
    # Fallback: if empty, take top-1
    if not selected_rules and candidates:
        first = candidates[0]
        if max(first["edge_up"], first["edge_down"]) >= min_edge_threshold:
            selected_rules.append(first)
    
    print(f"[INFO] Selected {len(selected_rules)} rules, {len(rejected_by_coverage)} rejected by coverage.")
    
    # 8. Pass 4: TTI for selected rules
    print("[PASS 4] Computing TTI histograms...")
    for rule in selected_rules:
        tti_hist = build_tti_histogram(rule["pattern"], sequences, setup_base_ids)
        rule["tti_probs"] = compute_eta_probs(tti_hist)
        rule["pattern"] = list(rule["pattern"])  # tuple -> list for JSON
        rule["last_state"] = rule["pattern"][-1]
    
    # 10. Pass 5: Build index
    # NOTE: index stores rule INDICES (not rule objects) for compact JSON.
    # IMPORTANT: JSON keys are always strings! When loading in online detector:
    #   - index_by_len_last["3"] not index_by_len_last[3]
    #   - or convert: {int(k): v for k, v in data["index_by_len_last"].items()}
    print("[PASS 5] Building index...")
    index_by_len_last = {}
    for i, rule in enumerate(selected_rules):
        L = len(rule["pattern"])
        last_state = rule["last_state"]
        
        # Store L as string for JSON compatibility
        L_str = str(L)
        if L_str not in index_by_len_last:
            index_by_len_last[L_str] = {}
        if last_state not in index_by_len_last[L_str]:
            index_by_len_last[L_str][last_state] = []
        
        index_by_len_last[L_str][last_state].append(i)
    
    # 11. Build artifact
    clean_symbol = symbol.replace("/", "").replace(":", "")
    clean_tf = tf.replace("/", "")
    clean_ex = exchange.replace("/", "")
    
    rules_artifact = {
        "version": BUILD_VERSION,
        "patchlog_version": PATCHLOG_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "tf": tf,
        "exchange": exchange,
        "meta": {
            "N_setups": N,
            "min_support_abs": min_support_abs,
            "min_edge_threshold": round(min_edge_threshold, 4),
            "K_rules": K_rules,
            "base_P_UP": round(base_P_UP, 4),
            "n_rules": len(selected_rules),
        },
        "rules": selected_rules,
        "index_by_len_last": index_by_len_last,
    }
    
    # 11. Save locally
    outfile = Path(f"offline/data/{clean_symbol}_{clean_tf}_{clean_ex}_rules.json")
    outfile.parent.mkdir(parents=True, exist_ok=True)
    
    with open(outfile, "w") as f:
        json.dump(rules_artifact, f, indent=2)
    print(f"[INFO] Saved locally: {outfile}")
    
    # 12. Save to Supabase
    success, msg = save_to_supabase(rules_artifact, symbol, tf, exchange)
    if success:
        print(f"[INFO] {msg}")
    else:
        print(f"[WARN] {msg}")
    
    # 13. Save debug files (local only, not to Supabase)
    print("[DEBUG] Saving debug files...")
    
    # Limits for scalability
    MAX_REJECTED_TO_SAVE = 2000  # Limit rejected patterns to save
    # Note: setups are now just IDs (no limit needed - IDs are lightweight)
    
    # Build debug rules (with setups as ID array from coverage_map)
    rules_debug = []
    for rule in selected_rules:
        rule_copy = dict(rule)
        # Get setups from coverage_map, not from rule object
        pattern = rule.get("pattern")
        if isinstance(pattern, list):
            pattern = tuple(pattern)
        setups_ids = coverage_map.get(pattern, set())
        
        # Store only IDs - dates will be fetched via SQL JOIN with segments table
        rule_copy["setups"] = sorted(setups_ids)  # Sorted for stable order
        rules_debug.append(rule_copy)
    
    # Build rejected patterns list with setups
    
    all_rejected = []
    
    # Helper to get setups for accepted pattern (from coverage_map)
    def get_setups_for_accepted(pattern):
        """Get setup IDs for accepted pattern (uses coverage_map)."""
        setups_ids = coverage_map.get(pattern, set())
        return sorted(setups_ids)  # Sorted for stable order
    
    # Helper to get setups for rejected pattern (lazy scan - no coverage_map)
    def get_setups_for_rejected(pattern):
        """Get setup IDs via lazy scan (rejected patterns not in coverage_map)."""
        found_ids = set()  # Use set to avoid duplicates
        pattern_tuple = tuple(pattern) if isinstance(pattern, list) else pattern
        
        for seq, base_id in zip(sequences, setup_base_ids):  # Use base_id for JOIN with segments
            # Check if pattern is in this sequence
            for start in range(len(seq)):
                end = start + len(pattern_tuple)
                if end <= len(seq) and tuple(seq[start:end]) == pattern_tuple:
                    found_ids.add(base_id)
                    break  # One match per sequence is enough
        return sorted(found_ids)  # Sorted for stable order
    
    # Sort rejected_by_edge by support (most frequent first) and limit
    rejected_by_edge_sorted = sorted(rejected_by_edge, key=lambda x: -x.get("support", 0))
    rejected_by_edge_limited = rejected_by_edge_sorted[:MAX_REJECTED_TO_SAVE]
    
    # Add rejected by edge (with lazy setups lookup)
    for p in rejected_by_edge_limited:
        p_copy = dict(p)
        pattern = p_copy.get("pattern")
        if isinstance(pattern, tuple):
            p_copy["pattern"] = list(pattern)
        p_copy["setups"] = get_setups_for_rejected(pattern)
        all_rejected.append(p_copy)
    
    # Add rejected by coverage (uses coverage_map since these were candidates)
    for p in rejected_by_coverage:
        if len(all_rejected) >= MAX_REJECTED_TO_SAVE:
            break
        p_copy = dict(p)
        pattern = p_copy.get("pattern")
        if isinstance(pattern, tuple):
            p_copy["pattern"] = list(pattern)
        p_copy["setups"] = get_setups_for_accepted(pattern)
        all_rejected.append(p_copy)
    
    print(f"[INFO] Prepared {len(all_rejected)} rejected patterns for export (limit: {MAX_REJECTED_TO_SAVE})")
    
    # 14. Save debug data to Supabase tables (instead of local JSON)
    print("[DEBUG] Saving debug data to Supabase...")
    
    try:
        url, key = load_secrets()
        supabase: Client = create_client(url, key)
        
        # Clear previous data for this symbol/tf/exchange (avoid duplicates)
        supabase.table("mining_accepted_patterns")\
            .delete()\
            .eq("symbol", symbol)\
            .eq("tf", tf)\
            .eq("exchange", exchange)\
            .execute()
        
        supabase.table("mining_rejected_patterns")\
            .delete()\
            .eq("symbol", symbol)\
            .eq("tf", tf)\
            .eq("exchange", exchange)\
            .execute()
        
        print(f"[INFO] Cleared previous debug data for {symbol} {tf} {exchange}")
        
        # Prepare accepted patterns for insert
        accepted_records = []
        for rule in rules_debug:
            pattern = rule.get("pattern")
            if isinstance(pattern, tuple):
                pattern = list(pattern)
            
            accepted_records.append({
                "run_version": BUILD_VERSION,
                "symbol": symbol,
                "tf": tf,
                "exchange": exchange,
                "pattern": pattern,
                "support": rule.get("support"),
                "wins_up": rule.get("wins_up"),
                "wins_down": rule.get("wins_down"),
                "edge_up": rule.get("edge_up"),
                "edge_down": rule.get("edge_down"),
                "tti_probs": rule.get("tti_probs"),
                "setups": rule.get("setups", []),
            })
        
        # Insert accepted patterns in chunks (same as rejected for consistency)
        CHUNK_SIZE = 200
        if accepted_records:
            for i in range(0, len(accepted_records), CHUNK_SIZE):
                chunk = accepted_records[i:i + CHUNK_SIZE]
                supabase.table("mining_accepted_patterns").insert(chunk).execute()
            print(f"[INFO] Saved {len(accepted_records)} accepted patterns to Supabase")
        
        # Prepare rejected patterns for insert
        rejected_records = []
        for p in all_rejected:
            pattern = p.get("pattern")
            if isinstance(pattern, tuple):
                pattern = list(pattern)
            
            rejected_records.append({
                "run_version": BUILD_VERSION,
                "symbol": symbol,
                "tf": tf,
                "exchange": exchange,
                "pattern": pattern,
                "support": p.get("support"),
                "wins_up": p.get("wins_up"),
                "wins_down": p.get("wins_down"),
                "edge_up": p.get("edge_up"),
                "edge_down": p.get("edge_down"),
                "reason": p.get("reason"),
                "reason_ru": p.get("reason_ru"),
                "threshold": p.get("threshold"),
                "setups": p.get("setups", []),
            })
        
        # Insert rejected patterns in chunks (avoid request size limits)
        CHUNK_SIZE = 200
        if rejected_records:
            for i in range(0, len(rejected_records), CHUNK_SIZE):
                chunk = rejected_records[i:i + CHUNK_SIZE]
                supabase.table("mining_rejected_patterns").insert(chunk).execute()
            print(f"[INFO] Saved {len(rejected_records)} rejected patterns to Supabase (chunked)")
            
    except Exception as e:
        print(f"[WARN] Failed to save debug data to Supabase: {e}")
    
    return True, f"Mined {len(selected_rules)} rules."


def save_to_supabase(rules_data, symbol, tf, exchange):
    """Save rules artifact to Supabase."""
    try:
        url, key = load_secrets()
        supabase: Client = create_client(url, key)
    except Exception as e:
        return False, f"Supabase connection failed: {e}"
    
    clean_symbol = symbol.replace("/", "").replace(":", "")
    clean_tf = tf.replace("/", "")
    clean_ex = exchange.replace("/", "")
    
    artifact_key = f"rules_{clean_symbol}_{clean_tf}_{clean_ex}"
    
    record = {
        "artifact_key": artifact_key,
        "version": BUILD_VERSION,
        "patchlog_version": PATCHLOG_VERSION,
        "data_json": rules_data,
        "meta": {
            "symbol": symbol,
            "tf": tf,
            "exchange": exchange,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "n_rules": rules_data["meta"]["n_rules"],
        }
    }
    
    try:
        res = supabase.table("training_artifacts")\
            .upsert(record, on_conflict="artifact_key,version")\
            .execute()
        return True, f"Saved to Supabase: {artifact_key}"
    except Exception as e:
        return False, f"Supabase save failed: {e}"


if __name__ == "__main__":
    res, msg = run_mining("ETH", "1D", "Binance")
    print(f"[{'OK' if res else 'ERROR'}] {msg}")
