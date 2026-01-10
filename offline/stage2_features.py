import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# --- CONFIG ---
CONFIG_PATH = Path("online/config.json")

def load_config():
    if not CONFIG_PATH.exists():
        print(f"[WARN] Config not found at {CONFIG_PATH}, using defaults.")
        return {"buffer_size": 30}
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)

CONFIG = load_config()
BUFFER_SIZE = CONFIG.get("buffer_size", 30)

def load_data(symbol, tf, exchange="Binance"):
    clean_symbol = symbol.replace("/", "").replace(":", "")
    clean_tf = tf.replace("/", "")
    clean_ex = exchange.replace("/", "")
    filepath = Path(f"offline/data/{clean_symbol}_{clean_tf}_{clean_ex}_clean.json")

    if not filepath.exists():
        return None, f"File not found: {filepath}"
        
    with open(filepath, "r") as f:
        data = json.load(f)
    return data, None

def calculate_time_features(candles):
    """
    Calculates TTI (Time-to-Impulse) and other temporal features.
    TTI is distance to the Impulse event.
    """
    # Placeholder for TTI logic (requires Impulse Detection logic which is Online logic)
    # In Offline training, we label segments.
    # We assume the segment already CONTAINS the setup leading to an impulse (or not/null).
    # Step 1.2 is "Simulate States". We simulate the 'Online Buffer' over the segment.
    pass

def simulate_buffer(segment_candles):
    """
    Simulates the Rolling Buffer state (Online State) iteratevly over the segment.
    Returns a list of 'STATE' vectors.
    """
    # This is the CORE of Offline Training.
    # We must replay the segment candle-by-candle.
    # At each step t, we have a buffer [t-30 : t].
    # We compute STATS(buffer_t).
    
    states = []
    
    # Needs minimum history?
    # Spec says segments are pre-cut setups.
    # Usually a segment has ~50-100 candles.
    # We assume the segment *validity* starts when buffer is full? OR we pad?
    # Let's assume strict buffer requirement.
    
    n = len(segment_candles)
    if n < BUFFER_SIZE:
        return [] # Skip too short
        
    for i in range(BUFFER_SIZE, n):
        # Window: [i-BUFFER..i] (inclusive of i? No, usually buffer is past history)
        # Online: At time T (close of candle T), we have history including T.
        # So window = candles[i-BUFFER+1 : i+1]
        
        # Slicing in python: [start : end] (end is exclusive)
        # So candles[i-BUFFER+1 : i+1] gives 30 candles ending at i.
        
        window = segment_candles[i-BUFFER+1 : i+1]
        
        # Compute Features
        # For MVP Step 1.2, we just extract basic STATS (sum_vol, etc) locally
        # to prove 'simulation' works.
        # Actual feature extraction logic is heavy (see batch_parser.stats).
        
        # We can reuse batch_parser logic if importable, or implement essential here.
        # Let's implement lightweight version for speed.
        
        # Feature Vector Candidate:
        # 1. Volatility (Range)
        # 2. Volume Flux
        # 3. CVD Stream
        
        # Just creating a dummy state to verify flow.
        state = {
            "ts": window[-1].get("ts"),
            "close": window[-1].get("close"),
            "vol_30_sum": sum(c.get("volume", 0) or 0 for c in window)
        }
        states.append(state)
        
    return states

def run_simulation(symbol, tf, exchange="Binance"):
    """
    Executes Step 1.2: Feature Engineering (Simulation).
    """
    print(f"[START] Feature Engineering for {symbol} {tf} ({exchange})...")
    
    # 1. Load
    segments, err = load_data(symbol, tf, exchange)
    if not segments:
        return False, err, 0
        
    print(f"[INFO] Loaded {len(segments)} segments.")
    
    # 2. Simulate
    enriched_data = []
    total_states = 0
    
    for seg in segments:
        # Extract candles from deep structure (reusing logic from 1.1 or assuming 1.1 flattened it?)
        # 1.1 preserved structure.
        data = seg.get("data", {})
        candles = []
        if isinstance(data, dict):
            if "CONTEXT" in data and "DATA" in data["CONTEXT"]:
                candles = data["CONTEXT"]["DATA"]
            elif "candles" in data:
                 candles = data["candles"]
        elif isinstance(data, list):
            candles = data
            
        if not candles: continue
            
        # Run Simulation
        states = simulate_buffer(candles)
        
        # Store result
        if states:
            # We can save specific Vectors or enriched segment
            # For now, let's just count 'valid training samples' generated
            total_states += len(states)
            
            # Append states to segment for inspection
            seg["training_states"] = states
            enriched_data.append(seg)
            
    # 3. Save
    clean_symbol = symbol.replace("/", "").replace(":", "")
    clean_tf = tf.replace("/", "")
    clean_ex = exchange.replace("/", "")
    outfile = Path(f"offline/data/{clean_symbol}_{clean_tf}_{clean_ex}_features.json")
    
    with open(outfile, "w") as f:
        json.dump(enriched_data, f, indent=2, default=str)
        
    report = f" processed {len(segments)} segments -> Generated {total_states} training states."
    return True, report, total_states

if __name__ == "__main__":
    res, msg, cnt = run_simulation("ETH", "1D")
    print(f"[{'OK' if res else 'ERROR'}] {msg}")
