import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo # Fallback для старых версий python

# --- КОНСТАНТЫ ---
# Локальная временная зона пользователя (Мадрид)
LOCAL_TZ = ZoneInfo("Europe/Madrid")

# Карта стандартизации таймфреймов
TF_MAP = {
    '1m': 'M1', 'm1': 'M1',
    '5m': 'M5', 'm5': 'M5',
    '15m': 'M15', 'm15': 'M15',
    '30m': 'M30', 'm30': 'M30',
    '1h': 'H1', 'h1': 'H1',
    '4h': 'H4', 'h4': 'H4',
    '1d': '1D', 'd1': '1D',
    '1w': '1W', 'w1': '1W'
}

# --- ХЕЛПЕРЫ (ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ) ---

def to_utc(dt: datetime) -> datetime:
    """Приводит дату/время к UTC."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=LOCAL_TZ)
    return dt.astimezone(timezone.utc)

def normalize_tf(tf: Any) -> str:
    """Нормализует название таймфрейма."""
    if tf is None:
        return ""
    s = str(tf).strip()
    if not s:
        return ""
    return TF_MAP.get(s.lower(), s.upper())

def _pick_tf_col(df: pd.DataFrame) -> Optional[str]:
    """Выбирает имя колонки с таймфреймом."""
    if "tf" in df.columns:
        return "tf"
    if "timeframe" in df.columns:
        return "timeframe"
    return None

def _validate_candles_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """
    Строгая валидация ("Hard Gate").
    К этому моменту обязательные колонки уже должны быть проверены (в run_full_analysis).
    Здесь проверяем целостность данных внутри строк.
    """
    warnings = []
    if df.empty:
        return df, warnings

    ok = pd.Series(True, index=df.index)

    # Правило 0: Пустой TF
    if "tf_norm" in df.columns:
        is_empty_tf = df["tf_norm"] == ""
        if is_empty_tf.any():
            warnings.append(f"Validation: Исключено {is_empty_tf.sum()} свечей с пустым/неизвестным TF")
            ok &= ~is_empty_tf
    
    # Правило 1: low <= close <= high (Целостность цен)
    needed = {"low", "high", "close"}
    if needed.issubset(df.columns):
        valid_ohlc = (df["low"] <= df["close"]) & (df["close"] <= df["high"]) & (df["high"] >= df["low"])
        ok &= valid_ohlc
    else:
        # Этого быть не должно, если OHLC проверяется как 'needed' выше, но на всякий случай
        warnings.append(f"Validation: Пропущены поля {needed}, OHLC не проверен!")

    # Правило 2: body_pct (диапазон 0-100)
    if "body_pct" in df.columns:
        valid_body = df["body_pct"].between(0, 100, inclusive="both")
        ok &= valid_body

    # Правило 3: price_vs_delta (допустимые значения)
    if "price_vs_delta" in df.columns:
        valid_pvd = df["price_vs_delta"].isin(["match", "div"])
        ok &= valid_pvd 

    # Правило 4: CVD Sign Consistency
    if "cvd_pct" in df.columns and "cvd_sign" in df.columns:
        cvd = pd.to_numeric(df["cvd_pct"], errors="coerce")
        # sign: 1 если >0, -1 если <0, 0 если 0
        sign = cvd.apply(lambda x: 0 if pd.isna(x) else (1 if x > 0 else (-1 if x < 0 else 0)))
        
        cvd_sign_val = pd.to_numeric(df["cvd_sign"], errors="coerce")
        
        # Проверка: совпадают знаки, ИЛИ cvd=0, ИЛИ cvd_sign неизвестен (NaN)
        # Мы не выкидываем строку, если CVD просто не посчитан (NaN), 
        # но если он есть и противоречит проценту — выкидываем.
        is_match = (sign == cvd_sign_val)
        is_zero = (cvd == 0)
        is_nan = cvd_sign_val.isna()
        
        valid_cvd = (is_match | is_zero | is_nan)
        ok &= valid_cvd

    # Правило 5: OI Flags (Logically Consistent - minimal check)
    flags = ["oi_set", "oi_counter", "oi_unload"]
    if all(c in df.columns for c in flags):
        a = df["oi_set"].astype("bool", errors="ignore")
        b = df["oi_counter"].astype("bool", errors="ignore")
        c = df["oi_unload"].astype("bool", errors="ignore")
        # Все три не могут быть True
        impossible = (a & b & c)
        ok &= ~impossible

    # Итог
    bad_count = (~ok).sum()
    if bad_count > 0:
        warnings.append(f"Validation: Исключено {bad_count} невалидных свечей по правилам целостности.")

    return df.loc[ok].copy(), warnings

def _build_tf_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """Собирает статистику по таймфреймам."""
    out = {}
    if df.empty or "tf_norm" not in df.columns:
        return out

    for tf, g in df.groupby("tf_norm"):
        if not tf: continue
        out[str(tf)] = {
            "count": int(len(g)),
            "first_ts": g["ts"].min().isoformat(),
            "last_ts": g["ts"].max().isoformat(),
        }
    return out

def _choose_final_tf(tf_stats: Dict[str, Any]) -> Optional[str]:
    """
    Выбирает финальный TF (Spec 1.7).
    Сравнение идет через datetime для надежности.
    """
    if not tf_stats:
        return None

    has_h4 = "H4" in tf_stats
    has_d1 = "1D" in tf_stats

    if has_h4 and has_d1:
        # Парсим ISO строки в datetime для надежного сравнения
        ts_d1_str = tf_stats["1D"]["last_ts"]
        ts_h4_str = tf_stats["H4"]["last_ts"]
        
        # fromisoformat доступен в Py 3.7+
        ts_d1 = datetime.fromisoformat(ts_d1_str)
        ts_h4 = datetime.fromisoformat(ts_h4_str)
        
        if ts_d1 > ts_h4:
            return "1D" # Дневка перекрывает
        return "H4"

    if has_h4:
        return "H4"

    return max(tf_stats.keys(), key=lambda k: tf_stats[k]["count"])


# --- ГЛАВНАЯ ФУНКЦИЯ ---

def run_full_analysis(db, symbol: str, start_ts: datetime, end_ts: datetime) -> Dict[str, Any]:
    """
    Главная точка входа. 
    Stage 1: Загрузка, Строгая фильтрация, Валидация (Hard Gate), Метаданные.
    """
    report = {
        "meta": {
            "symbol": symbol,
            "requested_start_ts": None, "requested_end_ts": None,
            "effective_start_ts": None, "effective_end_ts": None
        },
        "data": {
            "candles_fetched": 0, "candles_loaded": 0,
            "final_tf": None, "tf_stats": {}
        },
        "anchor": None, "flow": None, "metrics": None,
        "veto": None, "tam": None, "probability": None, "phase": None,
        "errors": [], "warnings": []
    }

    try:
        # Гарантируем UTC
        s_ts_utc = to_utc(start_ts)
        e_ts_utc = to_utc(end_ts)
        report["meta"]["requested_start_ts"] = s_ts_utc.isoformat()
        report["meta"]["requested_end_ts"] = e_ts_utc.isoformat()

        # 1. Валидация диапазонов
        if s_ts_utc >= e_ts_utc:
            report["errors"].append("Дата начала должна быть раньше даты конца")
            return report

        # 2. Загрузка
        fetch_limit = 100000
        df = db.load_candles(
            symbols=[symbol],
            start_date=s_ts_utc.date(),
            end_date=e_ts_utc.date(),
            limit=fetch_limit
        )
        report["data"]["candles_fetched"] = len(df)
        
        if len(df) >= fetch_limit:
            report["warnings"].append(f"Достигнут лимит загрузки {fetch_limit}.")

        if df.empty:
            report["errors"].append("В базе данных нет записей за этот период")
            return report

        # 3. Фильтрация времени и приведение типов
        df['ts'] = pd.to_datetime(df['ts'], utc=True, errors='coerce')
        if df['ts'].isnull().any():
            report["warnings"].append("Исключены строки с некорректной датой")
            df = df.dropna(subset=['ts'])
            
        mask = (df['ts'] >= s_ts_utc) & (df['ts'] <= e_ts_utc)
        df_filtered = df.loc[mask].copy()
        
        if df_filtered.empty:
            report["errors"].append("После фильтрации по времени не осталось свечей")
            return report

        # 4. Сортировка и Нормализация
        df_filtered = df_filtered.sort_values("ts", ascending=True).reset_index(drop=True)
        
        tf_col = _pick_tf_col(df_filtered)
        if tf_col:
            df_filtered["tf_norm"] = df_filtered[tf_col].apply(normalize_tf)
        else:
            report["errors"].append("CRITICAL: Нет колонки 'tf'/'timeframe'.")
            return report

        # 5. HARD GATE CHECK (Обязательные колонки)
        # Если этих колонок нет — мы не можем анализировать рынок по контракту
        REQUIRED_COLS = ["body_pct", "price_vs_delta"]
        missing_cols = [c for c in REQUIRED_COLS if c not in df_filtered.columns]
        if missing_cols:
            report["errors"].append(f"HARD GATE: Отсутствуют обязательные поля {missing_cols}. Анализ остановлен.")
            return report

        # 6. Валидация значений (удаление битых строк)
        df_clean, val_warnings = _validate_candles_df(df_filtered)
        report["warnings"].extend(val_warnings)
        report["data"]["candles_loaded"] = int(len(df_clean))
        
        if df_clean.empty:
            report["errors"].append("После валидации (Hard Gate) не осталось корректных свечей")
            return report

        # 7. Метаданные и Статистика
        min_ts = df_clean["ts"].min()
        max_ts = df_clean["ts"].max()
        report["meta"]["effective_start_ts"] = min_ts.isoformat()
        report["meta"]["effective_end_ts"] = max_ts.isoformat()

        if "tf_norm" in df_clean.columns:
            report["data"]["tf_stats"] = _build_tf_stats(df_clean)
            report["data"]["final_tf"] = _choose_final_tf(report["data"]["tf_stats"])
            
    except Exception as e:
        report["errors"].append(f"Критическая ошибка системы: {str(e)}")
        
    return report
