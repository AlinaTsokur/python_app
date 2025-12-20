
import re

def parse_value_raw(val):
    if not val: return None
    try:
        val = val.replace(',', '')
        mult = 1.0
        if val.endswith('M'):
            mult = 1_000_000.0
            val = val[:-1]
        elif val.endswith('K'):
            mult = 1_000.0
            val = val[:-1]
        elif val.endswith('B'):
            mult = 1_000_000_000.0
            val = val[:-1]
        elif val.endswith('%'):
            val = val[:-1]
        clean_str = re.sub(r'[^\d.-]', '', val)
        return round(float(clean_str) * mult, 2)
    except:
        return None

def extract(regex, text):
    match = re.search(regex, text, re.IGNORECASE | re.DOTALL)
    if match: return parse_value_raw(match.group(1))
    return None

text = "15.12.2025 17:00:00	Binance · ETH USDT Perp · 4H O 3,009.49 H 3,016.55 L 2,890.51 C 2,938.79 V 5.13B Change -70.70(-2.35%) Amplitude 126.04(4.19%) Active Buy/Sell Volume Buy 626.11K Sell -808.78K Delta -182.67K Ratio -0.13 Active Buy/Sell Trades Buy 1.44M Sell -1.51M Delta -76.20K Ratio -0.03 Open Interest O 2.05M H 2.09M L 2.05M C 2.09M Liquidation Long 6.99M Short -752.06K"

b_trades = extract(r'Active Buy/Sell Trades.*?Buy ([+\-]?[\d,.]+[MKB]?)', text)
s_trades = extract(r'Active Buy/Sell Trades.*?Sell ([+\-]?[\d,.]+[MKB]?)', text)
s_trades_abs = abs(s_trades) if s_trades else 0
trades_delta = extract(r'Active Buy/Sell Trades.*?Delta ([+\-]?[\d,.]+[MKB]?)', text)

total_trades = (b_trades or 0) + (s_trades_abs or 0)
calc_pct = (trades_delta / total_trades * 100) if total_trades else 0

derived_delta = (b_trades or 0) - (s_trades_abs or 0)
derived_pct = (derived_delta / total_trades * 100) if total_trades else 0

print(f"Buy: {b_trades}")
print(f"Sell: {s_trades} (Abs: {s_trades_abs})")
print(f"Total: {total_trades}")
print(f"Delta (Parsed): {trades_delta}")
print(f"Delta (Derived): {derived_delta}")
print(f"Pct (Parsed Delta): {calc_pct}")
print(f"Pct (Derived Delta): {derived_pct}")
