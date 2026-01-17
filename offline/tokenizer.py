"""
Tokenizer module for CORE_STATE serialization.

PATCH-09/10: Profiles STRICT/SMALLN + TAIL_DOM (TD)

Functions:
- get_tail_dom(candle) → "U"|"L"|"N"
- map_f_zone(oi_flags) → zone string for SMALLN
- map_q_zone(q_bin) → "LOW"|"MID"|"HIGH" for SMALLN
- tokenize_core_state(core_state, profile) → token string
"""
import math


def get_tail_dom(candle):
    """
    Calculate tail dominance from candle fields.
    
    Returns:
        "U" if upper_tail_pct > lower_tail_pct
        "L" if lower_tail_pct > upper_tail_pct
        "N" if equal or any is None/NaN
    
    Raises:
        ValueError if values are not numeric (to catch parsing bugs early)
    """
    upper = candle.get("upper_tail_pct")
    lower = candle.get("lower_tail_pct")
    
    # None is allowed (returns N)
    if upper is None or lower is None:
        return "N"
    
    # Check type - non-numeric is a bug, not missing data
    # Note: bool is a subclass of int, so check for bool first
    if isinstance(upper, bool) or isinstance(lower, bool):
        raise ValueError(f"upper_tail_pct/lower_tail_pct must be numeric, not bool: upper={upper}, lower={lower}")
    if not isinstance(upper, (int, float)) or not isinstance(lower, (int, float)):
        raise ValueError(f"upper_tail_pct/lower_tail_pct must be numeric or None, got: {type(upper).__name__}, {type(lower).__name__}")
    
    # Check for NaN - should be caught at Stage1, if here it's a bug
    if math.isnan(upper) or math.isnan(lower):
        raise ValueError(f"upper_tail_pct/lower_tail_pct contains NaN - should have been filtered at Stage1")
    
    if upper > lower:
        return "U"
    elif lower > upper:
        return "L"
    return "N"


def map_f_zone(oi_flags):
    """
    Map oi_flags (0-15) to zones for SMALLN profile.
    
    Zones: "0", "1-3", "4-7", "8-15"
    """
    if isinstance(oi_flags, bool) or not isinstance(oi_flags, int) or oi_flags < 0 or oi_flags > 15:
        raise ValueError(f"Invalid oi_flags: {oi_flags}. Expected int 0-15")
    
    if oi_flags == 0:
        return "0"
    elif 1 <= oi_flags <= 3:
        return "1-3"
    elif 4 <= oi_flags <= 7:
        return "4-7"
    else:
        return "8-15"


# Valid values
VALID_BINS = {"Q1", "Q2", "Q3", "Q4", "Q5"}
VALID_TD = {"U", "L", "N"}
VALID_PROFILES = {"STRICT", "SMALLN"}
VALID_DIV_TYPES = {"match_up", "match_down", "div_price_up_delta_down", "div_price_down_delta_up", "neutral_or_zero"}


def map_q_zone(q_bin):
    """
    Map Q1-Q5 bins to zones for SMALLN profile.
    
    Q1,Q2 → LOW
    Q3 → MID
    Q4,Q5 → HIGH
    """
    if q_bin not in VALID_BINS:
        raise ValueError(f"Invalid bin: {q_bin}. Expected one of {sorted(VALID_BINS)}")
    
    if q_bin in ("Q1", "Q2"):
        return "LOW"
    elif q_bin == "Q3":
        return "MID"
    else:
        return "HIGH"


def tokenize_core_state(core_state, profile):
    """
    Build canonical token string from core_state.
    
    Args:
        core_state: dict with div_type, oi_flags, cvd_bin, clv_bin, td
        profile: "STRICT" or "SMALLN"
    
    Returns:
        STRICT: DIV={div_type}|F={oi_flags}|CVD={Qx}|CLV={Qx}|TD={U/L/N}
        SMALLN: DIV={div_type}|FZ={zone}|CVDZ={zone}|CLVZ={zone}|TD={U/L/N}
    """
    # Validate profile
    if profile not in VALID_PROFILES:
        raise ValueError(f"Unknown profile: {profile}. Expected one of {sorted(VALID_PROFILES)}")
    
    # Get values with meaningful errors
    td = core_state.get("td")
    if td is None:
        raise ValueError("Missing 'td' in core_state")
    
    cvd_bin = core_state.get("cvd_bin")
    if cvd_bin is None:
        raise ValueError("Missing 'cvd_bin' in core_state")
    
    clv_bin = core_state.get("clv_bin")
    if clv_bin is None:
        raise ValueError("Missing 'clv_bin' in core_state")
    
    # Validate td
    if td not in VALID_TD:
        raise ValueError(f"Invalid td: {td}. Expected one of {sorted(VALID_TD)}")
    
    # Validate bins
    if cvd_bin not in VALID_BINS:
        raise ValueError(f"Invalid cvd_bin: {cvd_bin}. Expected one of {sorted(VALID_BINS)}")
    if clv_bin not in VALID_BINS:
        raise ValueError(f"Invalid clv_bin: {clv_bin}. Expected one of {sorted(VALID_BINS)}")
    
    # Validate div_type
    div_type = core_state.get("div_type")
    if div_type is None:
        raise ValueError("Missing 'div_type' in core_state")
    if div_type not in VALID_DIV_TYPES:
        raise ValueError(f"Invalid div_type: {div_type}. Expected one of {sorted(VALID_DIV_TYPES)}")
    
    # Validate oi_flags
    oi_flags = core_state.get("oi_flags")
    if oi_flags is None:
        raise ValueError("Missing 'oi_flags' in core_state")
    if isinstance(oi_flags, bool) or not isinstance(oi_flags, int) or oi_flags < 0 or oi_flags > 15:
        raise ValueError(f"Invalid oi_flags: {oi_flags}. Expected int 0-15")
    
    if profile == "STRICT":
        return (
            f"DIV={div_type}|"
            f"F={oi_flags}|"
            f"CVD={cvd_bin}|CLV={clv_bin}|TD={td}"
        )
    else:  # SMALLN
        return (
            f"DIV={div_type}|"
            f"FZ={map_f_zone(oi_flags)}|"
            f"CVDZ={map_q_zone(cvd_bin)}|"
            f"CLVZ={map_q_zone(clv_bin)}|TD={td}"
        )
