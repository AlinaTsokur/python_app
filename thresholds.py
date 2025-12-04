# Thresholds (Пороги) configuration
# Based on "Лист пороги" from scripts.docx

# Coefficients for different assets
# [SENS, k_set, k_ctr, k_unl]
ASSET_COEFFS = {
    'ETH': {'coeff': 1.0},
    'BTC': {'coeff': 1.0},
    'SOL': {'coeff': 1.2},
    'XRP': {'coeff': 1.1},
    'ADA': {'coeff': 1.1},
}

# Base Thresholds by Timeframe
# [SENS, k_set, k_ctr, k_unl]
# SENS: Base sensitivity
# k_set: Coefficient for "Набор" (Accumulation)
# k_ctr: Coefficient for "Встречный" (Counter)
# k_unl: Coefficient for "Разгрузка" (Unloading)
THRESHOLDS = {
    '5m':  {'SENS': 0.15, 'k_set': 2.0000, 'k_ctr': 2.3333, 'k_unl': 2.0000},
    '15m': {'SENS': 0.20, 'k_set': 2.0000, 'k_ctr': 2.5000, 'k_unl': 2.2500},
    '30m': {'SENS': 0.22, 'k_set': 1.8182, 'k_ctr': 2.5000, 'k_unl': 2.0455},
    '1h':  {'SENS': 0.30, 'k_set': 1.6667, 'k_ctr': 2.0000, 'k_unl': 1.8333},
    '4h':  {'SENS': 0.45, 'k_set': 1.7778, 'k_ctr': 2.2222, 'k_unl': 2.0000},
    '1d':  {'SENS': 0.90, 'k_set': 1.3333, 'k_ctr': 1.5556, 'k_unl': 1.4444},
    '1w':  {'SENS': 1.55, 'k_set': 1.3226, 'k_ctr': 1.5484, 'k_unl': 1.4516},
}

# Volatility Reference (rv_tf reference values)
# From rows 12-14 in scripts.txt
VOLATILITY_REF = {
    '1W': 2.5,
    '1D': 2.0,
    '4H': 0.7
}

# Sensitivity Base (sensitivity_base)
# From rows 17-19 in scripts.txt
SENSITIVITY_BASE = {
    '1W': 1.2,
    '1D': 0.9,
    '4H': 0.45
}

# Width Coefficients (for Levels calculation)
# From rows 21-24 in scripts.txt: [base, max, min]
# Note: In screenshot, order is Base (Col B), Max (Col C), Min (Col D)
WIDTH_COEFFICIENTS = {
    '1W': {'base': 0.3,  'max': 0.7,  'min': 0.3},
    '1D': {'base': 0.25, 'max': 0.55, 'min': 0.2},
    '4H': {'base': 0.33, 'max': 0.3,  'min': 0.1}
}

# Liquidation constants
LIQ_SQUEEZE_THRESHOLD = 0.3  # 30%
LIQ_WARNING_THRESHOLD = 0.2  # 20% (Fixed from 0.1)