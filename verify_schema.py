from app import add_candle_to_db, load_candles
from datetime import datetime

raw_data = "Binance · ETH USDT Perp · 4H O 3,027.65 H 3,048.44 L 3,027.05 C 3,033.44 V 753.13M Change +5.79(+0.19%) Amplitude 21.39(0.71%) Active Buy/Sell Volume Buy 128.07K Sell -119.89K Delta 8.18K Ratio 0.03 Active Buy/Sell Trades Buy 219.29K Sell -215.30K Delta 3.99K Ratio 0.01 Open Interest O 1.90M H 1.91M L 1.90M C 1.91M Liquidation Long 148.52K Short -288.69K"

print("Adding test candle...")
success, msg = add_candle_to_db(datetime.now().date(), datetime.now().time(), raw_data, "Schema Test")
print(f"Success: {success}, Msg: {msg}")

print("Loading candles...")
df = load_candles()
print("Columns:", df.columns.tolist())
print("Row 0 Vol_Delta:", df.iloc[0]['Vol_Delta'])
