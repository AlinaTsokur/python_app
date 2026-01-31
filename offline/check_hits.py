import os
try:
    import tomllib
except ImportError:
    import toml as tomllib
from pathlib import Path
from supabase import create_client

def load_secrets():
    secrets_path = Path(__file__).parent.parent / ".streamlit/secrets.toml"
    if secrets_path.exists():
        with open(secrets_path, "rb") as f:
            secrets = tomllib.load(f)
            # Handle both possible structures (root or [supabase] section)
            if "supabase" in secrets:
                return secrets["supabase"]["SUPABASE_URL"], secrets["supabase"]["SUPABASE_KEY"]
            return secrets.get("SUPABASE_URL"), secrets.get("SUPABASE_KEY")
    return os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")

def check_hits():
    url, key = load_secrets()
    client = create_client(url, key)
    
    # Query for hits
    response = client.table("pattern_hits")\
        .select("pattern_key, candle_ts, candle_index")\
        .eq("symbol", "ETH")\
        .eq("tf", "1D")\
        .eq("profile", "STRICT")\
        .limit(5)\
        .execute()
        
    print("--- First 5 hits ---")
    for hit in response.data:
        print(f"Index: {hit['candle_index']} | TS: {hit['candle_ts']} | Pattern: {hit['pattern_key'][:50]}...")

if __name__ == "__main__":
    check_hits()
