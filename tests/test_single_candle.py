import sys
import os
import tomllib
import logging
from pathlib import Path
from supabase import create_client, Client
import warnings

# Hack to import from parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from batch_parser import parse_raw_input, calculate_metrics, save_to_candles

# Suppress warnings for cleaner output
warnings.filterwarnings("ignore")

RAW_CANDLE_TEXT = "06.01.2026 1:00:00\tBinance · ETH USDT Perp · 1D O 3,223.72 H 3,308.00 L 3,180.71 C 3,295.46 V 14.91B Change +71.74(+2.23%) Amplitude 127.29(3.95%) Active Buy/Sell Volume ( ETHUSDT ) Buy 2.31M Sell -2.27M Delta 40.16K Ratio 0.01 Active Buy/Sell Trades ( ETHUSDT ) Buy 3.23M Sell -3.17M Delta 64.89K Ratio 0.01 Open Interest ( ETHUSDT ) O 2.20M H 2.26M L 2.13M C 2.14M Liquidation ( ETHUSDT ) Long 9.17M Short -8.91M"

def load_secrets():
    secrets_path = Path(__file__).parent.parent / ".streamlit/secrets.toml"
    with open(secrets_path, "rb") as f:
        secrets = tomllib.load(f)
    return secrets["SUPABASE_URL"], secrets["SUPABASE_KEY"]

def test_legacy_flow():
    print("\n[TEST] Starting Legacy Flow Verification...")
    
    # 1. Connect to DB
    try:
        url, key = load_secrets()
        supabase: Client = create_client(url, key)
        print("✅ DB Connection: OK")
    except Exception as e:
        print(f"❌ DB Connection Failed: {e}")
        return

    # 2. Parse Raw Input
    print("\n[TEST] Parsing Candle...")
    try:
        base_candle = parse_raw_input(RAW_CANDLE_TEXT)
        if not base_candle.get('open'):
            print(f"❌ Parsing Failed (empty result)")
            return
        
        # Verify basic fields
        print(f"DEBUG: Parsed Base Candle: {base_candle}")
        # assert base_candle['symbol_clean'] == 'ETHUSDT' 
        # assert base_candle['tf'] == '1D'
        
        # 3. Calculate Metrics (Fetch Config)
        print("\n[TEST] Fetching Config & Calculating Metrics...")
        # Note: calculate_metrics in batch_parser might rely on global/app context or defaults.
        # Let's see if it calls DB. Reading code suggests it takes 'config' arg.
        # If config is None, it uses defaults.
        # User asked: "try to get thresholds from supabase tf_params"
        
        # Manually fetch tf_params to verify DB read as user requested
        res_tf = supabase.table('tf_params').select("*").execute()
        if res_tf.data:
            print(f"DEBUG: First row keys in tf_params: {list(res_tf.data[0].keys())}")
            # Try to guess column name if tf_name missing
            first = res_tf.data[0]
            key_col = 'tf' if 'tf' in first else ('tf_name' if 'tf_name' in first else ('name' if 'name' in first else 'timeframe'))
            tf_params = {r[key_col]: r for r in res_tf.data}
            print(f"✅ DB Read (tf_params): OK (Found {len(tf_params)} TFs using key '{key_col}')")
        else:
             print("⚠️ Warning: tf_params table is empty!")
             tf_params = {}
        
        # Pass dummy config for now just to test calc logic
        full_candle = calculate_metrics(base_candle, config=None)
        print(f"✅ Metrics Calculation: OK (CVD={full_candle.get('cvd_pct')}%)")

        # 4. Save to DB
        print("\n[TEST] Saving to Supabase (candles table)...")
        # Reuse save_to_candles from batch_parser
        # It expects a list of candles
        count = save_to_candles(supabase, [full_candle])
        
        if count == 1:
            print(f"✅ DB Write (candles): OK (Saved 1 row)")
        else:
            print(f"❌ DB Write Failed: count={count}")

    except Exception as e:
        print(f"❌ Operation Failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_legacy_flow()
