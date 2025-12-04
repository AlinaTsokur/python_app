import streamlit as st
import re
import pandas as pd
import os
import uuid
from datetime import datetime, time
import thresholds  # Файл thresholds.py должен быть в той же папке

# --- Настройка страницы ---
st.set_page_config(
    page_title="VANTA Black",
    page_icon="🖤",
    layout="centered",
    initial_sidebar_state="collapsed"
)

# --- Константы ---
LEVELS_FILE = "levels.csv"
CANDLES_FILE = "candles.csv"

# --- 🎨 CSS: ПРЕМИУМ ДИЗАЙН (MESH GRADIENT + GLASS) ---
st.markdown("""
    <style>
        /* 1. СЛОЖНЫЙ ЖИВОЙ ФОН (Mesh Gradient) */
        .stApp {
            background-color: #0e1117;
            background-image: 
                radial-gradient(at 0% 0%, rgba(45, 55, 72, 0.6) 0px, transparent 50%),
                radial-gradient(at 100% 0%, rgba(20, 30, 60, 0.6) 0px, transparent 50%),
                radial-gradient(at 100% 100%, rgba(45, 55, 72, 0.6) 0px, transparent 50%),
                radial-gradient(at 0% 100%, rgba(20, 30, 60, 0.6) 0px, transparent 50%);
            background-attachment: fixed;
            color: #E0E0E0;
        }

        /* 2. НАСТОЯЩЕЕ ЖИДКОЕ СТЕКЛО (Glassmorphism) */
        [data-testid="stVerticalBlockBorderWrapper"] > div {
            /* Почти прозрачный фон, чтобы было видно размытие сзади */
            background: rgba(255, 255, 255, 0.03) !important; 
            
            /* Сильное размытие фона под карточкой */
            backdrop-filter: blur(20px) !important;
            -webkit-backdrop-filter: blur(20px) !important;
            
            /* Тонкая светящаяся рамка */
            border: 1px solid rgba(255, 255, 255, 0.08) !important;
            border-top: 1px solid rgba(255, 255, 255, 0.15) !important; /* Сверху чуть ярче (блик) */
            
            border-radius: 20px !important;
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.4) !important;
            margin-bottom: 24px;
            padding: 24px !important;
        }

        /* 3. ПОЛЯ ВВОДА (Интеграция в дизайн) */
        .stTextInput input, .stNumberInput input, .stDateInput input, .stTextArea textarea {
            background-color: rgba(0, 0, 0, 0.3) !important;
            color: white !important;
            border: 1px solid rgba(255, 255, 255, 0.1) !important;
            border-radius: 10px !important;
            transition: border 0.3s ease;
        }
        .stTextInput input:focus, .stNumberInput input:focus, .stTextArea textarea:focus {
            border: 1px solid rgba(255, 255, 255, 0.4) !important;
            box-shadow: 0 0 10px rgba(255, 255, 255, 0.1);
        }
        
        /* Скрываем степперы */
        [data-testid=stNumberInputStepDown], [data-testid=stNumberInputStepUp] { display: none; }

        /* 4. ТАБЛИЦЫ (DataFrame) */
        [data-testid="stDataFrame"] {
            background: rgba(0, 0, 0, 0.2);
            border-radius: 12px;
            padding: 10px;
            border: 1px solid rgba(255, 255, 255, 0.05);
        }

        /* 5. ВКЛАДКИ (Tabs) - CLEAN TEXT ONLY */
        .stTabs [data-baseweb="tab-list"] {
            background-color: transparent !important;
            gap: 20px;
        }
        .stTabs [data-baseweb="tab"] {
            height: auto;
            border-radius: 0;
            color: #888;
            font-weight: 500;
            padding-bottom: 5px;
            border: none;
            background-color: transparent !important;
        }
        .stTabs [aria-selected="true"] {
            background-color: transparent !important;
            color: white !important;
            font-weight: 700;
            border-bottom: 2px solid #ff4b4b; /* Optional: keep the red underline or remove it if they want purely text */
        }

        /* 6. КРАСНЫЙ БЕЙДЖ ТАЙМФРЕЙМА */
        .tf-badge {
            background: linear-gradient(135deg, #ff4b4b, #d10000);
            color: white;
            padding: 3px 10px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: 700;
            margin-left: 8px;
            border: 1px solid rgba(255,255,255,0.2);
            box-shadow: 0 2px 8px rgba(255, 75, 75, 0.3);
        }
        
        /* Заголовки */
        h1, h2, h3 {
            color: #ffffff;
            font-weight: 700;
            text-shadow: 0 2px 10px rgba(0,0,0,0.5);
        }
    </style>
""", unsafe_allow_html=True)

# --- Вспомогательные функции ---

def load_levels():
    if os.path.exists(LEVELS_FILE):
        return pd.read_csv(LEVELS_FILE)
    return pd.DataFrame(columns=["Symbol", "Price", "Type", "Note"])

def save_levels(df):
    df.to_csv(LEVELS_FILE, index=False)

def load_candles():
    if os.path.exists(CANDLES_FILE):
        return pd.read_csv(CANDLES_FILE)
    
    columns = [
        "Date", "Time", "Symbol", "Asset", "Timeframe", "Note", "Exchange",
        "Open", "High", "Low", "Close", "Volume", 
        "Change_Abs", "Change_Pct", "Amplitude_Abs", "Range_Pct",
        "Price_Sign", "Body_Pct", "Body_Label", "Color",
        "Upper_Tail_Pct", "Lower_Tail_Pct", "CLV_Pct", "Close_Pos", "Dominant_Reject",
        "Vol_Buy", "Vol_Sell", "Vol_Delta", "Trade_Buy", "Trade_Sell", "Trade_Delta",
        "ABV_Ratio_Pct", "Trades_Ratio_Pct",
        "Avg_Trade_Buy", "Avg_Trade_Sell", "Tilt_Pct", 
        "CVD_Pct", "CVD_Sign", "CVD_Small", "Implied_Price",
        "OI_Open", "OI_High", "OI_Low", "OI_Close", "OI_Units",
        "DOI_Pct", "POROG_DOI_Pct", "r_Strength", "Signal_Strength", "epsilon", "OE",
        "OI_Pos", "OI_Path", "OI_Flow", 
        "OI_In_Sens", "OI_Set", "OI_Counter", "OI_Unload",
        "T_Set", "T_Ctr", "T_Unl",
        "Liq_Long", "Liq_Short", "LiqShare_Pct", "Limb", "Liq_Squeeze",
        "DPX", "Price_vs_Delta", "Ratio_Stable", 
        "Geo_Score", "Flow_Score", "Liq_Penalty", "RQ_Calculated",
        "X-RAY Report", "Raw_Data"
    ]
    return pd.DataFrame(columns=columns)

def save_candles(df):
    df.to_csv(CANDLES_FILE, index=False)

def fmt(val):
    if val is None or val == '-' or val == '': return '-'
    try:
        f_val = float(val)
        return "{:.2f}".format(f_val)
    except:
        return str(val)

def parse_value(val_str):
    if not val_str or val_str == '-': return None
    clean_str = val_str.replace(',', '')
    suffix = clean_str[-1].upper()
    multiplier = 1
    if suffix in ['K', 'M', 'B']:
        if suffix == 'K': multiplier = 1000
        elif suffix == 'M': multiplier = 1_000_000
        elif suffix == 'B': multiplier = 1_000_000_000
        clean_str = clean_str[:-1]
    try:
        return float(clean_str) * multiplier
    except ValueError:
        return None

def parse_float(val_str):
    if not val_str or val_str == '-': return None
    try:
        return float(val_str.replace(',', ''))
    except:
        return None

def extract(regex, text):
    match = re.search(regex, text)
    if match: return parse_value(match.group(1))
    return None

# --- ЯДРО РАСЧЕТОВ ---
def calculate_derived_metrics(data, raw_text=""):
    metrics = {}
    try:
        ohlc = data.get('ohlc', {})
        O = ohlc.get('Open')
        H = ohlc.get('High')
        L = ohlc.get('Low')
        C = ohlc.get('Close')
        V = data.get('volume')
        
        if None in [O, H, L, C, V] or V == 0: return metrics

        symbol = data.get('symbol', '')
        metrics['Asset'] = symbol.split(' ')[0].replace('USDT', '').replace('PERP', '')

        metrics['Change_Abs'] = C - O
        metrics['Change_Pct'] = (metrics['Change_Abs'] / O * 100) if O else 0
        metrics['Amplitude_Abs'] = H - L
        metrics['Range_Pct'] = (metrics['Amplitude_Abs'] / C * 100) if C else 0
        metrics['Price_Sign'] = 1 if C > O else (-1 if C < O else 0)
        metrics['Color'] = "зелёная" if C >= O else "красная"
        metrics['Body_Pct'] = (abs(C - O) / metrics['Amplitude_Abs'] * 100) if metrics['Amplitude_Abs'] else 0
        
        bp = metrics['Body_Pct']
        if bp < 5: metrics['Body_Label'] = "доджи"
        elif bp < 15: metrics['Body_Label'] = "сверхмалое тело"
        elif bp < 25: metrics['Body_Label'] = "малое тело"
        elif bp < 50: metrics['Body_Label'] = "среднее- тело"
        elif bp < 60: metrics['Body_Label'] = "среднее+ тело"
        elif bp < 80: metrics['Body_Label'] = "крупное тело"
        elif bp < 95: metrics['Body_Label'] = "очень крупное тело"
        else: metrics['Body_Label'] = "почти полное (марубозу)"

        metrics['Upper_Tail_Pct'] = ((H - max(O, C)) / metrics['Amplitude_Abs'] * 100) if metrics['Amplitude_Abs'] else 0
        metrics['Lower_Tail_Pct'] = ((min(O, C) - L) / metrics['Amplitude_Abs'] * 100) if metrics['Amplitude_Abs'] else 0
        
        metrics['CLV_Pct'] = ((C - L) / metrics['Amplitude_Abs'] * 100) if metrics['Amplitude_Abs'] else 50
        
        clv = metrics['CLV_Pct']
        if clv <= 5: metrics['Close_Pos'] = "у лоя"
        elif clv < 20: metrics['Close_Pos'] = "в нижней части диапазона"
        elif clv < 40: metrics['Close_Pos'] = "ниже середины диапазона"
        elif clv <= 60: metrics['Close_Pos'] = "в середине диапазона"
        elif clv <= 80: metrics['Close_Pos'] = "выше середины диапазона"
        elif clv < 95: metrics['Close_Pos'] = "в верхней части диапазона"
        else: metrics['Close_Pos'] = "у хая"

        ut, lt, body = metrics['Upper_Tail_Pct'], metrics['Lower_Tail_Pct'], metrics['Body_Pct']
        dom_rej = "-"
        if lt >= 3 * body and ut <= 10 and clv >= 85: dom_rej = "bull_Ideal"
        elif ut >= 3 * body and lt <= 10 and clv <= 15: dom_rej = "bear_Ideal"
        elif lt >= 2 * body and ut <= 25 and clv >= 75: dom_rej = "bull_Valid"
        elif ut >= 2 * body and lt <= 25 and clv <= 25: dom_rej = "bear_Valid"
        elif lt >= 1.5 * body and clv >= 65 and ut <= 0.5 * lt: dom_rej = "bull_Loose"
        elif ut >= 1.5 * body and clv <= 35 and lt <= 0.5 * ut: dom_rej = "bear_Loose"
        metrics['Dominant_Reject'] = dom_rej

        vol_m = data.get('vol_metrics', {})
        trade_m = data.get('trade_metrics', {})
        
        metrics['Vol_Buy'] = vol_m.get('Buy')
        metrics['Vol_Sell'] = abs(vol_m.get('Sell')) if vol_m.get('Sell') is not None else None
        if vol_m.get('Delta') is not None: metrics['Vol_Delta'] = vol_m.get('Delta')
        elif metrics['Vol_Buy'] is not None and metrics['Vol_Sell'] is not None: metrics['Vol_Delta'] = metrics['Vol_Buy'] - metrics['Vol_Sell']
        else: metrics['Vol_Delta'] = None

        metrics['Trade_Buy'] = trade_m.get('Buy')
        metrics['Trade_Sell'] = abs(trade_m.get('Sell')) if trade_m.get('Sell') is not None else None
        if trade_m.get('Delta') is not None: metrics['Trade_Delta'] = trade_m.get('Delta')
        elif metrics['Trade_Buy'] is not None and metrics['Trade_Sell'] is not None: metrics['Trade_Delta'] = metrics['Trade_Buy'] - metrics['Trade_Sell']
        else: metrics['Trade_Delta'] = None
        
        if metrics['Vol_Buy'] is not None and metrics['Vol_Sell'] is not None:
            denom = metrics['Vol_Buy'] + metrics['Vol_Sell']
            metrics['ABV_Ratio_Pct'] = ((metrics['Vol_Buy'] - metrics['Vol_Sell']) / denom * 100) if denom else 0
        else: metrics['ABV_Ratio_Pct'] = 0

        if metrics['Trade_Buy'] is not None and metrics['Trade_Sell'] is not None:
            denom = metrics['Trade_Buy'] + metrics['Trade_Sell']
            metrics['Trades_Ratio_Pct'] = ((metrics['Trade_Buy'] - metrics['Trade_Sell']) / denom * 100) if denom else 0
        else: metrics['Trades_Ratio_Pct'] = 0
            
        metrics['Avg_Trade_Buy'] = (metrics['Vol_Buy'] / metrics['Trade_Buy']) if (metrics['Vol_Buy'] is not None and metrics['Trade_Buy']) else 0
        metrics['Avg_Trade_Sell'] = (metrics['Vol_Sell'] / metrics['Trade_Sell']) if (metrics['Vol_Sell'] is not None and metrics['Trade_Sell']) else 0
        
        if metrics['Avg_Trade_Buy'] > 0: metrics['Tilt_Pct'] = (metrics['Avg_Trade_Sell'] / metrics['Avg_Trade_Buy'] - 1) * 100
        else: metrics['Tilt_Pct'] = 0
            
        total_active_vol = (metrics['Vol_Buy'] + metrics['Vol_Sell']) if (metrics['Vol_Buy'] is not None and metrics['Vol_Sell'] is not None) else V
        metrics['CVD_Pct'] = (metrics['Vol_Delta'] / total_active_vol * 100) if (metrics['Vol_Delta'] is not None and total_active_vol) else 0
        metrics['CVD_Small'] = abs(metrics['CVD_Pct']) < 1.0
        metrics['CVD_Sign'] = 1 if metrics['CVD_Pct'] > 0 else (-1 if metrics['CVD_Pct'] < 0 else 0)
        
        if metrics['Vol_Buy'] is not None and metrics['Vol_Sell'] is not None and (metrics['Vol_Buy'] + metrics['Vol_Sell']) > 0:
            metrics['Implied_Price'] = V / (metrics['Vol_Buy'] + metrics['Vol_Sell'])
        else: metrics['Implied_Price'] = 0

        oi = data.get('oi', {})
        metrics['OI_Open'] = oi.get('Open')
        metrics['OI_High'] = oi.get('High')
        metrics['OI_Low'] = oi.get('Low')
        metrics['OI_Close'] = oi.get('Close')
        
        if re.search(r"Open Interest [^L]* (USDT|USD|\$)", raw_text): metrics['OI_Units'] = "USDT"
        elif re.search(r"Open Interest [^L]* (coin|COIN|BTC|ETH|SOL|XRP|ADA)", raw_text): metrics['OI_Units'] = "coin"
        else: metrics['OI_Units'] = "contracts"

        metrics['DOI_Pct'] = ((oi.get('Close', 0) - oi.get('Open', 0)) / oi.get('Open', 1) * 100) if oi.get('Open') else 0
        
        tf = data.get('timeframe', '4h').lower()
        th = thresholds.THRESHOLDS.get(tf, thresholds.THRESHOLDS['4h'])
        asset_c = thresholds.ASSET_COEFFS.get(data.get('symbol', '').split(' ')[0].upper(), {'coeff': 1.0})['coeff']
        metrics['POROG_DOI_Pct'] = th['SENS'] * asset_c
        
        porog = metrics['POROG_DOI_Pct']
        metrics['r_Strength'] = (abs(metrics['DOI_Pct']) / porog) if porog else 0
        
        if metrics['r_Strength'] >= 2: metrics['Signal_Strength'] = "сильный"
        elif metrics['r_Strength'] >= 1: metrics['Signal_Strength'] = "средний"
        else: metrics['Signal_Strength'] = "слабый"

        metrics['epsilon'] = 0.33 * porog
        metrics['OE'] = abs(metrics['DOI_Pct']) / abs(metrics['Change_Pct']) if abs(metrics['Change_Pct']) > 0 else 0
        
        if oi.get('High') is not None and oi.get('Low') is not None:
            oi_rng = oi['High'] - oi['Low']
            raw_pos = (oi['Close'] - oi['Low']) / oi_rng if oi_rng else 0.5
            metrics['OI_Pos'] = max(0.0, min(1.0, raw_pos))
        else: metrics['OI_Pos'] = 0.5

        if metrics['DOI_Pct'] > 0: metrics['OI_Path'] = "up"
        elif metrics['DOI_Pct'] < 0: metrics['OI_Path'] = "down"
        else: metrics['OI_Path'] = "neutral"
            
        metrics['OI_Flow'] = "новые позиции" if metrics['DOI_Pct'] > 0 else "закрытия"

        t_set = porog * th['k_set']
        t_ctr = porog * th['k_ctr']
        t_unl = -(porog * th['k_unl'])
        metrics['T_Set'] = t_set
        metrics['T_Ctr'] = t_ctr
        metrics['T_Unl'] = t_unl

        metrics['OI_In_Sens'] = abs(metrics['DOI_Pct']) <= metrics['POROG_DOI_Pct']
        metrics['OI_Set'] = metrics['DOI_Pct'] > t_set
        metrics['OI_Unload'] = metrics['DOI_Pct'] <= t_unl
        
        liq = data.get('liquidation', {})
        ll = liq.get('Long', 0) or 0
        ls = liq.get('Short', 0) or 0
        metrics['Liq_Long'] = ll
        metrics['Liq_Short'] = ls
        metrics['LiqShare_Pct'] = (ll + ls) / V * 100 if V else 0
        
        if (ll + ls) > 0: metrics['Limb'] = ((ls - ll) / (ll + ls) * 100)
        else: metrics['Limb'] = 0
        
        metrics['Liq_Squeeze'] = metrics['LiqShare_Pct'] > (thresholds.LIQ_SQUEEZE_THRESHOLD * 100)

        metrics['DPX'] = metrics['Price_Sign'] * metrics['CVD_Sign']
        metrics['Price_vs_Delta'] = "div" if metrics['DPX'] == -1 else ("match" if metrics['DPX'] == 1 else "neutral")
        metrics['OI_Counter'] = (metrics['DPX'] == -1) and (metrics['DOI_Pct'] > t_ctr)
        
        trades_sign = 1 if metrics['Trades_Ratio_Pct'] > 0 else (-1 if metrics['Trades_Ratio_Pct'] < 0 else 0)
        metrics['Ratio_Stable'] = (metrics['CVD_Sign'] == trades_sign)
        
        geo_score = 0
        if metrics['CLV_Pct'] >= 80 or metrics['CLV_Pct'] <= 20: geo_score += 2
        if metrics['Upper_Tail_Pct'] >= 30: geo_score += 1
        if metrics['Body_Pct'] >= 30: geo_score += 1
        metrics['Geo_Score'] = geo_score
        
        adapt_sens = metrics.get('POROG_DOI_Pct', 0.5) 
        if adapt_sens == 0: adapt_sens = 0.5
        
        flow_score = 0
        abs_doi = abs(metrics['DOI_Pct'])
        if abs_doi >= adapt_sens: flow_score += 2
        elif abs_doi >= 0.5 * adapt_sens: flow_score += 1
            
        if metrics['DPX'] == -1: flow_score += 1
        if metrics['Ratio_Stable']: flow_score += 1
        metrics['Flow_Score'] = flow_score
        
        liq_penalty = 0
        if metrics['LiqShare_Pct'] >= 0.3: liq_penalty = -2
        elif metrics['LiqShare_Pct'] >= 0.2: liq_penalty = -1
        metrics['Liq_Penalty'] = liq_penalty
        
        metrics['RQ_Calculated'] = max(0, geo_score + flow_score + liq_penalty)

    except Exception as e:
        import traceback
        st.error(f"Calc Error: {e}")
        
    return metrics

def parse_candle_data(text):
    data = {}
    header_match = re.search(r'(.+?) · (.+?) · (\w+)', text)
    data['exchange'] = header_match.group(1) if header_match else 'Unknown'
    data['symbol'] = header_match.group(2) if header_match else 'Unknown'
    data['timeframe'] = header_match.group(3) if header_match else '-'

    ohlc_match = re.search(r'O ([\d,.]+) H ([\d,.]+) L ([\d,.]+) C ([\d,.]+)', text)
    data['ohlc'] = {
        'Open': parse_float(ohlc_match.group(1)) if ohlc_match else None,
        'High': parse_float(ohlc_match.group(2)) if ohlc_match else None,
        'Low': parse_float(ohlc_match.group(3)) if ohlc_match else None,
        'Close': parse_float(ohlc_match.group(4)) if ohlc_match else None
    }

    data['volume'] = extract(r'V ([\d,.]+[MKB]?)', text)
    
    vol_buy = extract(r'Active Buy/Sell Volume Buy ([+\-]?[\d,.]+[MKB]?)', text)
    vol_sell = extract(r'Active Buy/Sell Volume.*?Sell ([+\-]?[\d,.]+[MKB]?)', text)
    data['vol_metrics'] = {
        'Buy': vol_buy,
        'Sell': abs(vol_sell) if vol_sell is not None else None,
        'Delta': extract(r'Active Buy/Sell Volume.*?Delta ([+\-]?[\d,.]+[MKB]?)', text)
    }

    trade_buy = extract(r'Active Buy/Sell Trades Buy ([+\-]?[\d,.]+[MKB]?)', text)
    trade_sell = extract(r'Active Buy/Sell Trades.*?Sell ([+\-]?[\d,.]+[MKB]?)', text)
    data['trade_metrics'] = {
        'Buy': trade_buy,
        'Sell': abs(trade_sell) if trade_sell is not None else None,
        'Delta': extract(r'Active Buy/Sell Trades.*?Delta ([+\-]?[\d,.]+[MKB]?)', text)
    }

    oi_match = re.search(r'Open Interest O ([\d,.]+[MKB]?) H ([\d,.]+[MKB]?) L ([\d,.]+[MKB]?) C ([\d,.]+[MKB]?)', text)
    data['oi'] = {
        'Open': parse_value(oi_match.group(1)) if oi_match else None,
        'High': parse_value(oi_match.group(2)) if oi_match else None,
        'Low': parse_value(oi_match.group(3)) if oi_match else None,
        'Close': parse_value(oi_match.group(4)) if oi_match else None
    }

    liq_long = extract(r'Liquidation Long ([\d,.]+[MKB]?)', text)
    liq_short = extract(r'Liquidation.*?Short ([+\-]?[\d,.]+[MKB]?)', text)
    data['liquidation'] = {
        'Long': liq_long,
        'Short': abs(liq_short) if liq_short is not None else None
    }

    return data

def add_candle_to_db(date, time, raw_data, report_text, note=""):
    try:
        df = load_candles()
        data = parse_candle_data(raw_data)
        metrics = calculate_derived_metrics(data, raw_data)
        
        def get_fmt(d, k): return fmt(d.get(k))
        
        row_dict = {
            "Date": date,
            "Time": time,
            "Exchange": data.get('exchange'),
            "Symbol": data.get('symbol'),
            "Timeframe": data.get('timeframe'),
            "Note": note,
            "Open": data['ohlc']['Open'],
            "High": data['ohlc']['High'],
            "Low": data['ohlc']['Low'],
            "Close": data['ohlc']['Close'],
            "Volume": data.get('volume'),
            "Raw_Data": raw_data,
            "X-RAY Report": report_text
        }

        for key, val in metrics.items():
            if key not in row_dict:
                row_dict[key] = fmt(val) if isinstance(val, (int, float)) else val

        new_row = pd.DataFrame([row_dict])
        
        bool_cols = ["CVD_Small", "OI_In_Sens", "OI_Set", "OI_Counter", "OI_Unload", "Liq_Squeeze", "Ratio_Stable"]
        for col in bool_cols:
            if col in new_row.columns:
                new_row[col] = new_row[col].astype(bool)

        for col in df.columns:
            if col not in new_row.columns: new_row[col] = None
        new_row = new_row[df.columns]

        df = pd.concat([df, new_row], ignore_index=True)
        save_candles(df)
        return True, f"Candle for {data.get('symbol', 'Unknown')} saved!"
    except Exception as e:
        import traceback
        return False, f"Error saving candle: {e}\n{traceback.format_exc()}"

# --- ГЛАВНЫЙ ИНТЕРФЕЙС (UI) ---

st.title("🖤 VANTA")
tab1, tab2, tab3 = st.tabs(["Отчеты", "Уровни", "БД Свечи"])

# --- ВКЛАДКА 1: ОТЧЕТЫ ---
with tab1:
    st.markdown("### 📦 Загрузка Свечей")

    
    def split_candle_input(text):
        exchanges = ["Binance", "Bybit", "OKX", "Bitget", "Coinbase", "Kraken", "KuCoin", "HTX", "Gate.io", "MEXC", "BingX"]
        pattern = r'(?=(?:' + '|'.join(exchanges) + r')\s+·)'
        parts = re.split(pattern, text, flags=re.IGNORECASE)
        return [p.strip() for p in parts if p.strip()]

    # Removed st.form to remove the border
    input_text = st.text_area("Input", label_visibility="collapsed", height=200, placeholder="Вставьте одну или несколько свечей")
    process_submitted = st.button("Распарсить Пакет", type="primary")

    if process_submitted and input_text:
        raw_candles = split_candle_input(input_text)
        parsed_batch = []
        for raw in raw_candles:
            data = parse_candle_data(raw)
            if data:
                parsed_batch.append({'id': str(uuid.uuid4()), 'raw': raw, 'data': data, 'valid': True})
            else:
                 parsed_batch.append({'id': str(uuid.uuid4()), 'raw': raw, 'data': None, 'valid': False})
        st.session_state['parsed_batch'] = parsed_batch

    if 'parsed_batch' in st.session_state and st.session_state['parsed_batch']:
        st.divider()
        st.subheader(f"Найдено свечей: {len(st.session_state['parsed_batch'])}")
        
        valid_candles = []
        for i, item in enumerate(st.session_state['parsed_batch']):
            # ИСПОЛЬЗУЕМ "СТЕКЛЯННЫЙ" КОНТЕЙНЕР (border=True активирует CSS)
            with st.container(border=True):
                if item['valid']:
                    data = item['data']
                    col_info, col_date, col_h, col_m = st.columns([4, 2, 1, 1])
                    
                    with col_info:
                        st.markdown(f"**#{i+1} {data['exchange']} {data['symbol']}** <span class='tf-badge'>{data['timeframe']}</span>", unsafe_allow_html=True)
                        st.caption(f"Close: {fmt(data['ohlc']['Close'])}")
                    
                    with col_date:
                        d_key = f"date_{i}"
                        if d_key not in st.session_state: st.session_state[d_key] = datetime.now().date()
                        new_date = st.date_input("Дата", key=d_key, label_visibility="collapsed")
                        
                    with col_h:
                        h_key = f"hour_{i}"
                        if h_key not in st.session_state: st.session_state[h_key] = datetime.now().hour
                        new_h = st.number_input("Час", min_value=0, max_value=23, step=1, key=h_key, label_visibility="collapsed", help="Часы")
                    
                    with col_m:
                        m_key = f"min_{i}"
                        if m_key not in st.session_state: st.session_state[m_key] = datetime.now().minute
                        new_m = st.number_input("Мин", min_value=0, max_value=59, step=1, key=m_key, label_visibility="collapsed", help="Минуты")
                    
                    item['user_date'] = new_date
                    item['user_time'] = time(new_h, new_m)
                    
                    metrics = calculate_derived_metrics(data, item['raw'])
                    def fmt_bool(val): return "true" if val else "false"
                    ts_str = f"{item['user_date'].strftime('%d.%m.%Y')} {item['user_time'].strftime('%H:%M')}"
                    
                    report = f"""ts: {ts_str}
exchange: {data['exchange']}
symbol: {data['symbol']}
tf: {data['timeframe']}
open: {fmt(data['ohlc']['Open'])}
high: {fmt(data['ohlc']['High'])}
low: {fmt(data['ohlc']['Low'])}
close: {fmt(data['ohlc']['Close'])}
volume: {fmt(data['volume'])}
buy_volume: {fmt(metrics.get('Vol_Buy'))}
sell_volume: {fmt(metrics.get('Vol_Sell'))}
buy_trades: {fmt(metrics.get('Trade_Buy'))}
sell_trades: {fmt(metrics.get('Trade_Sell'))}
oi_open: {fmt(metrics.get('OI_Open'))}
oi_high: {fmt(metrics.get('OI_High'))}
oi_low: {fmt(metrics.get('OI_Low'))}
oi_close: {fmt(metrics.get('OI_Close'))}
liq_long: {fmt(metrics.get('Liq_Long'))}
liq_short: {fmt(metrics.get('Liq_Short'))}
range: {fmt(metrics.get('Amplitude_Abs'))}
body_pct: {fmt(metrics.get('Body_Pct'))}%
clv_pct: {fmt(metrics.get('CLV_Pct'))}%
upper_tail_pct: {fmt(metrics.get('Upper_Tail_Pct'))}%
lower_tail_pct: {fmt(metrics.get('Lower_Tail_Pct'))}%
price_sign: {metrics.get('Price_Sign')}
dominant_reject: {metrics.get('Dominant_Reject')}
cvd_pct: {fmt(metrics.get('CVD_Pct'))}%
cvd_sign: {metrics.get('CVD_Sign')}
cvd_small: {fmt_bool(metrics.get('CVD_Small'))}
dpx: {fmt(metrics.get('DPX'))}
price_vs_delta: {metrics.get('Price_vs_Delta')}
dtrades_pct: {fmt(metrics.get('Trades_Ratio_Pct'))}%
ratio_stable: {fmt_bool(metrics.get('Ratio_Stable'))}
tilt_pct: {fmt(metrics.get('Tilt_Pct'))}%
doi_pct: {fmt(metrics.get('DOI_Pct'))}%
oi_in_sens: {fmt_bool(metrics.get('OI_In_Sens'))}
oi_set: {fmt_bool(metrics.get('OI_Set'))}
oi_counter: {fmt_bool(metrics.get('OI_Counter'))}
oi_unload: {fmt_bool(metrics.get('OI_Unload'))}
oipos: {fmt(metrics.get('OI_Pos'))}
oi_path: {metrics.get('OI_Path')}
oe: {fmt(metrics.get('OE'))}
liqshare_pct: {fmt(metrics.get('LiqShare_Pct'))}%
limb_pct: {fmt(metrics.get('Limb'))}%
liq_squeeze: {fmt_bool(metrics.get('Liq_Squeeze'))}
range_pct: {fmt(metrics.get('Range_Pct'))}%
implied_price: {fmt(metrics.get('Implied_Price'))}
avg_trade_buy: {fmt(metrics.get('Avg_Trade_Buy'))}
avg_trade_sell: {fmt(metrics.get('Avg_Trade_Sell'))}"""
                    
                    item['report'] = report
                    valid_candles.append(item)
                else:
                    st.error(f"#{i+1}: Ошибка парсинга.")

        st.subheader("📊 Генерация Отчетов")
        if st.button("🔬 X-RAY Отчет"):
            full_report = ""
            for item in valid_candles:
                full_report += f"--- REPORT FOR {item['data']['symbol']} ---\n{item['report']}\n\n"
            st.markdown("### 📝 X-RAY Batch Report")
            st.code(full_report, language='yaml')

        st.divider()
        st.subheader("💾 Сохранение")
        if st.button("Сохранить ВСЕ свечи в базу", type="primary"):
            saved_count = 0; errors = []
            progress_bar = st.progress(0)
            for i, item in enumerate(valid_candles):
                try:
                    success, msg = add_candle_to_db(
                        item['user_date'], 
                        item['user_time'], 
                        item['raw'], 
                        item['report'],
                        note="Batch Import"
                    )
                    if success: saved_count += 1
                    else: errors.append(f"{item['data']['symbol']}: {msg}")
                except Exception as e:
                    errors.append(f"{item['data']['symbol']}: {e}")
                progress_bar.progress((i + 1) / len(valid_candles))
            if saved_count > 0: st.success(f"Успешно сохранено свечей: {saved_count}")
            if errors:
                st.error(f"Ошибки ({len(errors)}):")
                for err in errors: st.write(err)

# --- ВКЛАДКА 2: УРОВНИ ---
with tab2:
    st.header("Управление Уровнями")
    with st.form("add_level_form"):
        col_sym, col_price, col_type = st.columns(3)
        with col_sym: new_symbol = st.text_input("Символ (например, ETH)", value="")
        with col_price: new_price = st.number_input("Цена", min_value=0.0, step=0.01, format="%.2f")
        with col_type: new_type = st.selectbox("Тип", ["Resistance", "Support", "Liquidity", "Other"])
        new_note = st.text_input("Заметка (Опционально)")
        if st.form_submit_button("Добавить Уровень"):
            df = load_levels()
            new_row = pd.DataFrame({"Symbol": [new_symbol], "Price": [new_price], "Type": [new_type], "Note": [new_note]})
            df = pd.concat([df, new_row], ignore_index=True)
            save_levels(df)
            st.success("Уровень добавлен!")
            st.rerun()
    df = load_levels()
    if not df.empty:
        display_df = df.rename(columns={"Symbol": "Символ", "Price": "Цена", "Type": "Тип", "Note": "Заметка"})
        st.dataframe(display_df, hide_index=True, use_container_width=True)
    else:
        st.info("Список уровней пуст.")

# --- ВКЛАДКА 3: БД СВЕЧИ ---
with tab3:
    st.header("База Свечей")
    candles_df = load_candles()
    
    if not candles_df.empty:
        candles_df = candles_df.sort_index(ascending=False)
        
        candles_df.insert(0, "Удалить", False)
        
        edited_df = st.data_editor(
            candles_df,
            column_config={
                "Удалить": st.column_config.CheckboxColumn(
                    "Удалить?",
                    help="Выберите свечи для удаления",
                    default=False,
                ),
                "X-RAY Report": st.column_config.TextColumn("Отчет X-RAY", width="large"),
                "Raw_Data": st.column_config.TextColumn("Сырые данные", width="medium"),
            },
            hide_index=True,
            num_rows="fixed",
            use_container_width=True
        )
        
        col_del, col_down = st.columns([1, 1])
        
        with col_del:
            if st.button("🗑 Удалить выбранные и Обновить"):
                rows_to_keep = edited_df[edited_df["Удалить"] == False]
                rows_to_keep = rows_to_keep.drop(columns=["Удалить"])
                save_candles(rows_to_keep)
                st.success("База обновлена!")
                st.rerun()
                
        with col_down:
            export_df = candles_df.drop(columns=["Удалить"])
            csv = export_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Скачать базу (CSV)",
                data=csv,
                file_name='candles_export.csv',
                mime='text/csv',
            )
    else:
        st.info("База пуста.")