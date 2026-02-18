# УНИФИЦИРОВАННЫЙ ПРОМПТ: АЛГОРИТМ ЯКОРНОЙ ТОЧКИ + ORDER FLOW V3.4

## 📋 СТРУКТУРА СИСТЕМЫ

```
┌─────────────────────────────────────────────────────────────┐
│  PYTHON CORE ENGINE (Supabase + Расчёты)                    │
│  ├─ Модуль 1: Поиск якорной точки (ANCHOR FINDER)          │
│  ├─ Модуль 2: Структурный анализ (STRUCTURE OVERRIDE)       │
│  ├─ Модуль 3: Метрики потока (FLOW METRICS)                 │
│  ├─ Модуль 4: VETO-фильтры (WALL/ER/SR/GR1)                │
│  └─ Модуль 5: TAM-V3.3 аномалии (7 фильтров)               │
│                                                              │
│  OUTPUT: JSON-отчёт с метриками                             │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  AI INTERPRETER (Claude/GPT)                                 │
│  ├─ Интерпретация якоря и потока                            │
│  ├─ Фазовый анализ (ИМПУЛЬС/КУЛЬМИНАЦИЯ/БАЛАНС)            │
│  ├─ Синтез с уровнями (1-4)                                 │
│  ├─ Риски и манипуляции                                     │
│  └─ Финальное резюме + План торговли                        │
│                                                              │
│  OUTPUT: Человекочитаемый отчёт на русском                  │
└─────────────────────────────────────────────────────────────┘
```

---

## ⚙️ ЧАСТЬ 1: PYTHON CORE ENGINE (РАСЧЁТНАЯ ЛОГИКА)

### 1.0. ИСТОЧНИК ДАННЫХ: SUPABASE

**Таблица:** `candles`  
**Структура:**

```sql
CREATE TABLE candles (
  id UUID PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  tf VARCHAR(10) NOT NULL,           -- '1D', 'H4', '4h', 'H1', '1h', 'M30', 'M15', 'M5', 'M1'
  symbol_clean VARCHAR(20) NOT NULL, -- 'ETHUSDT', 'BTCUSDT'
  exchange VARCHAR(20),              -- 'binance', 'bybit'
  
  -- Цена (8 полей)
  open NUMERIC,
  high NUMERIC,
  low NUMERIC,
  close NUMERIC,
  range NUMERIC,
  body NUMERIC,
  clv_pct NUMERIC,
  price_sign INT, -- +1/0/-1
  
  -- Объём (5 полей)
  volume NUMERIC,
  delta NUMERIC,
  delta_pct NUMERIC,
  imbalance_pct NUMERIC,
  trades_count INT,
  
  -- Поток (8 полей)
  cvd_pct NUMERIC,
  cvd_small NUMERIC,
  dtrades_pct NUMERIC,
  trades_buy INT,
  trades_sell INT,
  price_vs_delta VARCHAR(10), -- 'match' / 'div'
  delta_buy NUMERIC,
  delta_sell NUMERIC,
  
  -- OI (11 полей)
  oi_set BOOLEAN,
  oi_counter BOOLEAN,
  oi_unload BOOLEAN,
  oi_in_sens BOOLEAN,
  oi_path VARCHAR(10), -- 'up' / 'down' / 'flat'
  doi_pct NUMERIC,
  oi_long NUMERIC,
  oi_short NUMERIC,
  oi_total NUMERIC,
  oi_change NUMERIC,
  oi_level VARCHAR(20),
  
  -- Ликвидации (5 полей)
  liq_long NUMERIC,
  liq_short NUMERIC,
  limb_pct NUMERIC,
  liqshare_pct NUMERIC,
  liq_squeeze BOOLEAN,
  
  -- Геометрия (10 полей)
  body_pct NUMERIC,
  upper_tail_pct NUMERIC,
  lower_tail_pct NUMERIC,
  dominant_reject VARCHAR(10),
  range_pct NUMERIC,
  tilt_pct NUMERIC,
  ratio_stable NUMERIC,
  
  -- Производные
  er NUMERIC, -- Efficiency Ratio (range / |cvd|)
  sr NUMERIC, -- Sustainability Ratio (doi / range)
  oe NUMERIC, -- Order Efficiency (body / |cvd|)
  
  -- Метаданные и заметки
  note TEXT
);

-- Композитный ключ для upsert
CREATE UNIQUE INDEX idx_candles_upsert ON candles(exchange, symbol_clean, tf, ts);
CREATE INDEX idx_candles_symbol_tf_ts ON candles(symbol_clean, tf, ts);
```

**Запрос данных:**
```python
def fetch_candles(symbol: str, exchange: str, start_ts: str, end_ts: str, 
                  limit: int = 500) -> List[Dict]:
    """
    Получение свечей из Supabase за период.
    
    КРИТИЧНО:
    - exchange обязателен — предотвращает смешивание данных разных бирж
    - свечи могут быть разных TF (1D, H4, H1 и т.д.)
    - limit применяется к СМЕШАННОМУ списку — ставить с запасом!
    
    Рекомендация: после определения final_tf можно сделать второй запрос
    уже с фильтром по tf для гарантии достаточного количества свечей.
    
    Args:
        symbol: Тикер (например, 'ETHUSDT')
        exchange: Биржа ('binance', 'bybit') — ОБЯЗАТЕЛЬНО
        start_ts: Начало периода (ISO8601 UTC)
        end_ts: Конец периода (ISO8601 UTC)
        limit: Максимум свечей (рекомендуется 500+ для смешанных TF)
    
    Returns:
        List[Dict]: Свечи в хронологическом порядке, поле 'tf' содержит таймфрейм.
    """
    response = supabase.table('candles') \
        .select('*') \
        .eq('exchange', exchange) \
        .eq('symbol_clean', symbol) \
        .gte('ts', start_ts) \
        .lte('ts', end_ts) \
        .order('ts', desc=False) \
        .limit(limit) \
        .execute()
    
    return response.data
```

---

### 1.1. МОДУЛЬ ПОИСКА ЯКОРЯ (ANCHOR FINDER)

**ВХОД:** `List[Dict]` — свечи из Supabase (50-300 шт)  
**ВЫХОД:** `Dict` — якорь T0 или None

#### 1.1.1. ЭТАП SCAN: Поиск кандидатов

```python
def scan_candidates(candles: List[Dict]) -> List[Dict]:
    """
    Сканирование свечей на события-кандидаты.
    
    ℹ️ В unified workflow сюда приходят свечи УЖЕ отфильтрованные по final_tf.
    TF читается из candle['tf'] и нормализуется.
    
    Возвращает список кандидатов с полями:
        - index: номер свечи в массиве
        - type: 'A' | 'B' | 'C'
        - side: 'LONG' | 'SHORT'
        - price: цена якоря
        - timestamp: время свечи
        - tf: таймфрейм свечи
    """
    candidates = []
    
    for i in range(len(candles)):
        candle = candles[i]
        # ВАЖНО: нормализуем TF для корректных сравнений с порогами!
        c_tf = _normalize_tf(candle.get('tf') or candle.get('timeframe'))
        
        # === TYPE A: Слом OI ===
        # side определяется НЕ по знаку doi, а по развороту цены после свечи i
        if abs(candle.get('doi_pct') or 0) > 5.0:
            side = None
            
            # Ищем после i откат/разворот >5% в любую сторону от close[i]
            base = candle.get('close')
            if base is None or base == 0:
                continue  # Пропускаем свечи без цены закрытия
            
            future_candles = candles[i+1:]
            
            if future_candles:
                max_after = max((c.get('high') or 0) for c in future_candles)
                min_after = min((c.get('low') or float('inf')) for c in future_candles)
                
                up_move = (max_after - base) / base * 100
                down_move = (base - min_after) / base * 100
                
                if up_move > 5.0 and up_move >= down_move:
                    side = 'LONG'
                elif down_move > 5.0 and down_move > up_move:
                    side = 'SHORT'
            
            # Если side не определился — кандидат TYPE A отбрасываем
            if side is not None:
                candidates.append({
                    'index': i,
                    'type': 'A',
                    'side': side,
                    'price': candle['close'],
                    'timestamp': candle['ts'],
                    'high': candle['high'],
                    'low': candle['low'],
                    'tf': c_tf
                })
        
        # === TYPE B: Слом цены + откат ===
        # БЕЗ ограничения 10 свечей — ищем откат до конца массива
        if i > 0 and i < len(candles) - 1:
            prev_highs = [c['high'] for c in candles[:i]]
            prev_lows = [c['low'] for c in candles[:i]]
            
            is_new_high = candle['high'] > max(prev_highs) if prev_highs else False
            is_new_low = candle['low'] < min(prev_lows) if prev_lows else False
            
            # Пробой вверх → ищем откат вниз >5%
            if is_new_high:
                for j in range(i+1, len(candles)):  # БЕЗ min(i+10, ...)
                    retracement = (candle['high'] - candles[j]['close']) / candle['high'] * 100
                    if retracement > 5.0:
                        candidates.append({
                            'index': i,
                            'type': 'B',
                            'side': 'SHORT',
                            'price': candle['high'],
                            'timestamp': candle['ts'],
                            'high': candle['high'],
                            'low': candle['low'],
                            'tf': c_tf
                        })
                        break
            
            # Пробой вниз → ищем откат вверх >5%
            if is_new_low:
                for j in range(i+1, len(candles)):  # БЕЗ min(i+10, ...)
                    retracement = (candles[j]['close'] - candle['low']) / candle['low'] * 100
                    if retracement > 5.0:
                        candidates.append({
                            'index': i,
                            'type': 'B',
                            'side': 'LONG',
                            'price': candle['low'],
                            'timestamp': candle['ts'],
                            'high': candle['high'],
                            'low': candle['low'],
                            'tf': c_tf
                        })
                        break
        
        # === TYPE C: Кульминация CVD + подтверждённый разворот цены >5% ===
        # Порог CVD зависит от TF (c_tf уже нормализован через _normalize_tf)
        cvd_threshold = 5.0 if c_tf == '1D' else 15.0 if c_tf == 'H4' else None
        
        if cvd_threshold is not None and abs(candle.get('cvd_pct') or 0) > cvd_threshold:
            if i < len(candles) - 1:
                base = candle['close']
                future_candles = candles[i+1:]
                
                max_after = max(c['high'] for c in future_candles)
                min_after = min(c['low'] for c in future_candles)
                
                up_move = (max_after - base) / base * 100
                down_move = (base - min_after) / base * 100
                
                # Кульминация BUY (cvd > 0), ждём разворот вниз >5% → SHORT
                if candle['cvd_pct'] > 0 and down_move > 5.0:
                    candidates.append({
                        'index': i,
                        'type': 'C',
                        'side': 'SHORT',
                        'price': candle['close'],
                        'timestamp': candle['ts'],
                        'high': candle['high'],
                        'low': candle['low'],
                        'tf': c_tf
                    })
                
                # Кульминация SELL (cvd < 0), ждём разворот вверх >5% → LONG
                if candle['cvd_pct'] < 0 and up_move > 5.0:
                    candidates.append({
                        'index': i,
                        'type': 'C',
                        'side': 'LONG',
                        'price': candle['close'],
                        'timestamp': candle['ts'],
                        'high': candle['high'],
                        'low': candle['low'],
                        'tf': c_tf
                    })
    
    return candidates
```

#### 1.1.2. ЭТАП SELECT: Выбор валидного якоря

```python
def select_anchor(candidates: List[Dict], candles: List[Dict]) -> Optional[Dict]:
    """
    Выбор актуального якоря из кандидатов.
    
    Проверяет:
        1. Overlap (пробита ли цена якоря)
        2. OI Reset (Net OI после якоря <= 0)
    
    Возвращает первый валидный якорь (с конца списка).
    """
    # Идём с конца (самые свежие кандидаты)
    for cand in reversed(candidates):
        t0_index = cand['index']
        side = cand['side']
        
        # Проверка 1: Overlap
        is_overlapped = False
        for i in range(t0_index + 1, len(candles)):
            close = candles[i]['close']
            if side == 'SHORT' and close > cand['high']:
                is_overlapped = True
                break
            if side == 'LONG' and close < cand['low']:
                is_overlapped = True
                break
        
        if is_overlapped:
            continue  # Якорь пробит, ищем следующий
        
        # Проверка 2: OI Reset
        # ВАЖНО: Net OI считается от T0 включительно до конца (не от t0+1)
        net_oi = sum((candles[i].get('oi_change') or 0) for i in range(t0_index, len(candles)))
        
        if net_oi <= 0:
            continue  # Позиции обнулены
        
        # Якорь валиден!
        return cand
    
    return None  # Не нашли валидный якорь
```

---

### 1.2. МОДУЛЬ СТРУКТУРНОГО СЛОМА (STRUCTURE OVERRIDE)

**ВХОД:** Якорь T0 + история свечей  
**ВЫХОД:** T_struct (индекс слома) или None

#### 1.2.1. Определение структурного слома

```python
def find_structural_break(candles: List[Dict], anchor: Dict, tf: str) -> Optional[int]:
    """
    Поиск структурного слома НАЗАД от якоря.
    
    Args:
        candles: массив свечей
        anchor: якорь T0
        tf: таймфрейм ('1D', 'H4', etc.)
    
    Returns:
        index первой свечи слома или None
    """
    t0_index = anchor['index']
    side = anchor['side']
    
    # Параметр окна структуры
    # H4 = 30 (зафиксировано), 1D = 20, 1W = 8
    L_struct = 20 if tf == '1D' else 8 if tf == '1W' else 30
    
    # Идём назад от T0-1
    for k in range(t0_index - 1, L_struct, -1):
        # Вычисляем уровни структуры
        window = candles[k - L_struct:k]
        S_high = max(c['high'] for c in window)
        S_low = min(c['low'] for c in window)
        
        candle_k = candles[k]
        candle_k1 = candles[k + 1]
        
        # Bullish Break (ищем если side=LONG)
        if side == 'LONG':
            # Тело свечи k выше S_high
            if min(candle_k['open'], candle_k['close']) > S_high:
                # Тело свечи k+1 тоже выше S_high
                if min(candle_k1['open'], candle_k1['close']) > S_high:
                    return k  # Нашли слом!
        
        # Bearish Break (ищем если side=SHORT)
        if side == 'SHORT':
            # Тело свечи k ниже S_low
            if max(candle_k['open'], candle_k['close']) < S_low:
                # Тело свечи k+1 тоже ниже S_low
                if max(candle_k1['open'], candle_k1['close']) < S_low:
                    return k  # Нашли слом!
    
    return None  # Структурный слом не найден
```

---

### 1.3. МОДУЛЬ МЕТРИК ПОТОКА (FLOW METRICS)

**ВХОД:** T_flow (начало потока), свечи до Tn  
**ВЫХОД:** Dict с метриками

```python
def calculate_flow_metrics(candles: List[Dict], t_flow: int, t_current: int) -> Dict:
    """
    Расчёт метрик потока на диапазоне [T_flow .. T_current].
    
    ВАЖНО: NULL-safe! Все поля проверяются на None.
    
    Returns:
        {
            'net_oi': float,       # Чистый OI (абсолютное значение)
            'net_oi_pct': float,   # Чистый OI в % от начального OI
            'cum_cvd': float,      # Кумулятивный CVD
            'avg_entry': float,    # Средняя цена входа
            'current_price': float,
            'pl_percent': float    # P&L позиций
        }
    """
    flow_segment = candles[t_flow:t_current + 1]
    
    if not flow_segment:
        return {
            'net_oi': 0, 'net_oi_pct': None, 'cum_cvd': 0,
            'avg_entry': None, 'current_price': None, 'pl_percent': None
        }
    
    # 1. Net OI (абсолютное значение) — защита от NULL
    net_oi = sum((c.get('oi_change') or 0) for c in flow_segment)
    
    # 2. Net OI в процентах от начального OI
    oi_base = flow_segment[0].get('oi_total')
    net_oi_pct = (net_oi / oi_base * 100) if oi_base and oi_base != 0 else None
    
    # 3. Cum CVD — защита от NULL
    cum_cvd = sum((c.get('cvd_pct') or 0) for c in flow_segment)
    
    # 4. Avg Entry (только свечи с набором OI)
    weighted_sum = 0.0
    total_weight = 0.0
    
    for c in flow_segment:
        oi_change = c.get('oi_change') or 0
        if oi_change > 0:
            close_price = c.get('close') or 0
            weighted_sum += close_price * oi_change
            total_weight += oi_change
    
    avg_entry = weighted_sum / total_weight if total_weight > 0 else candles[t_flow].get('close')
    
    # 5. Current Price
    current_price = candles[t_current].get('close')
    
    # 6. P&L — защита от деления на 0
    pl_percent = None
    if avg_entry and avg_entry != 0 and current_price:
        pl_percent = ((current_price - avg_entry) / avg_entry) * 100
    
    return {
        'net_oi': net_oi,
        'net_oi_pct': round(net_oi_pct, 2) if net_oi_pct is not None else None,
        'cum_cvd': round(cum_cvd, 2),
        'avg_entry': avg_entry,
        'current_price': current_price,
        'pl_percent': round(pl_percent, 2) if pl_percent is not None else None
    }
```

---

### 1.4. МОДУЛЬ ИНВАЛИДАЦИИ ПОТОКА (FLOW INVALIDATION)

**ВХОД:** T_flow, якорь, свечи  
**ВЫХОД:** Status ('ACTIVE', 'STRUCT_BROKEN', 'OI_INVALIDATED')

```python
def check_flow_invalidation(candles: List[Dict], anchor: Dict, t_flow: int, tf: str) -> Dict:
    """
    Проверка инвалидации потока.
    
    Returns:
        {
            'status': str,        # 'ACTIVE' | 'STRUCT_BROKEN' | 'OI_INVALIDATED'
            'reason': str,        # Причина инвалидации
            'candle_index': int   # На какой свече сломался
        }
    """
    side = anchor['side']
    # H4 = 30 (зафиксировано), 1D = 20, 1W = 8
    L_struct = 20 if tf == '1D' else 8 if tf == '1W' else 30
    
    # Проверка 1: Обратный структурный слом
    for k in range(t_flow + 1, len(candles) - 1):
        window = candles[max(0, k - L_struct):k]
        S_high = max(c['high'] for c in window)
        S_low = min(c['low'] for c in window)
        
        candle_k = candles[k]
        candle_k1 = candles[k + 1]
        
        # Если якорь был SHORT → ищем Bullish Break
        if side == 'SHORT':
            if min(candle_k['open'], candle_k['close']) > S_high and \
               min(candle_k1['open'], candle_k1['close']) > S_high:
                return {
                    'status': 'STRUCT_BROKEN',
                    'reason': 'Обратный Bullish Break',
                    'candle_index': k
                }
        
        # Если якорь был LONG → ищем Bearish Break
        if side == 'LONG':
            if max(candle_k['open'], candle_k['close']) < S_low and \
               max(candle_k1['open'], candle_k1['close']) < S_low:
                return {
                    'status': 'STRUCT_BROKEN',
                    'reason': 'Обратный Bearish Break',
                    'candle_index': k
                }
    
    # Проверка 2: Net OI обнулился — NULL-safe!
    net_oi = sum((candles[i].get('oi_change') or 0) for i in range(t_flow, len(candles)))
    
    if net_oi <= 0:
        return {
            'status': 'OI_INVALIDATED',
            'reason': 'Net OI обнулился',
            'candle_index': len(candles) - 1
        }
    
    # Поток активен
    return {
        'status': 'ACTIVE',
        'reason': None,
        'candle_index': None
    }
```

---

### 1.5. МОДУЛЬ VETO-ФИЛЬТРОВ (CORE ENGINE)

**Весь код из старого промпта раздела 8.1 — без изменений!**

```python
class V3_Deterministic_Core:
    def __init__(self, tf="H4"):
        self.tf = tf
        self.thresholds = {
            "1D":  {"sens": 0.90, "setup": 1.20, "counter": 1.40, "unload": -1.30, "div_min": 2.0},
            "H4":  {"sens": 0.45, "setup": 0.80, "counter": 1.00, "unload": -0.90, "div_min": 2.0},
            "H1":  {"sens": 0.30, "setup": 0.50, "counter": 0.60, "unload": -0.55, "div_min": 1.5},
        }.get(self.tf, {"sens": 0.30, "setup": 0.50, "counter": 0.60, "unload": -0.55, "div_min": 1.5})

    def run_analysis(self, candles):
        """
        Запуск VETO-фильтров на последней свече.
        
        Возвращает:
            {
                'veto': {'down': bool, 'up': bool, 'reasons': List[str]},
                'er': float,
                'sr': float,
                'oe': float
            }
        """
        # Полный код из раздела 8.1 старого промпта
        # [копируется дословно без изменений]
```

---

### 1.6. МОДУЛЬ TAM-V3.3 (7 ФИЛЬТРОВ АНОМАЛИЙ)

**Весь код из старого промпта раздела 12 — без изменений!**

```python
def detect_tam_anomalies(last_candle: Dict, baseline: Dict, tf: str) -> Dict:
    """
    Детекция аномалий по TAM-V3.3.
    
    Returns:
        {
            'friction': bool,
            'skew': bool,
            'fuel_inversion': bool,
            'quality': bool,
            'crowding': bool,
            'vacuum': bool,
            'hollow': bool,
            'details': Dict  # Подробности по каждому фильтру
        }
    """
    # Полный код из раздела 12 старого промпта
    # [копируется дословно без изменений]
```

---

### 1.7. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ

```python
def _normalize_tf(tf: str) -> str:
    """
    Нормализация TF к единому формату.
    
    '4h' -> 'H4', '4H' -> 'H4', '1d' -> '1D', etc.
    Важно: V3_Deterministic_Core.thresholds использует 'H4', 'H1', '1D'.
    """
    if not tf:
        return tf
    tf_lower = tf.strip().lower()  # Приводим к нижнему регистру
    mapping = {
        '4h': 'H4', '1h': 'H1', '30m': 'M30', '15m': 'M15', 
        '5m': 'M5', '1m': 'M1', '1d': '1D', '1w': '1W',
        'h4': 'H4', 'h1': 'H1', 'm30': 'M30', 'm15': 'M15',
    }
    # Если не найдено — возвращаем в верхнем регистре (унификация)
    return mapping.get(tf_lower) or tf.strip().upper()


def _filter_by_tf(candles: List[Dict], target_tf: str) -> List[Dict]:
    """
    Фильтрация свечей по одному TF.
    
    КРИТИЧНО: все расчёты (якорь, структура, метрики, VETO, TAM)
    должны делаться на свечах ОДНОГО TF, иначе математика невалидна.
    
    Args:
        candles: смешанный список свечей
        target_tf: целевой TF (уже нормализованный)
    
    Returns:
        Список свечей только целевого TF, отсортированный по ts
    """
    target_tf = _normalize_tf(target_tf)
    filtered = []
    for c in candles:
        c_tf = _normalize_tf(c.get('tf') or c.get('timeframe'))
        if c_tf == target_tf:
            filtered.append(c)
    # Гарантируем порядок по времени
    filtered.sort(key=lambda x: x.get('ts', ''))
    return filtered


def _pick_final_tf(candles: List[Dict]) -> Dict:
    """
    Определяет финальный TF для итогового результата.
    
    Логика:
    - Если есть D1 и H4: D1 перекрывает H4 если закрылась >= последней H4
    - Если только H4 — берём H4
    - Иначе — самый старший TF из имеющихся
    
    Returns:
        {'final_tf': 'H4'|'1D'|..., 'final_ts': timestamp}
        final_tf всегда нормализован.
    """
    by_tf = {}
    for c in candles:
        tf = _normalize_tf(c.get('tf') or c.get('timeframe'))
        if not tf:
            continue
        by_tf.setdefault(tf, []).append(c)
    
    def _last_ts(tf_key: str):
        arr = by_tf.get(tf_key, [])
        if not arr:
            return None
        return max(x['ts'] for x in arr)
    
    ts_d1 = _last_ts('1D')
    ts_h4 = _last_ts('H4')
    
    if ts_d1 and ts_h4:
        # D1 перекрывает, если закрылась не раньше последней H4
        if ts_d1 >= ts_h4:
            return {'final_tf': '1D', 'final_ts': ts_d1}
        return {'final_tf': 'H4', 'final_ts': ts_h4}
    
    if ts_h4:
        return {'final_tf': 'H4', 'final_ts': ts_h4}
    
    # Fallback: если H4 нет — берём самый старший TF из имеющихся по иерархии
    # Этот priority используется ТОЛЬКО если выше не нашли D1/H4
    priority = ['1W', '1D', 'H4', 'H1', 'M30', 'M15', 'M5', 'M1']
    for p in priority:
        ts = _last_ts(p)
        if ts:
            return {'final_tf': p, 'final_ts': ts}
    
    return {'final_tf': None, 'final_ts': None}
```

---

### 1.8. МОДУЛЬ ФАЗЫ И ВЕРОЯТНОСТИ

**ВАЖНО:** Все расчёты фазы и вероятности делаются в Python. AI только отображает готовые значения.

```python
def compute_phase(anchor: Dict, metrics: Dict, flow_status: str) -> Dict:
    """
    Определение фазы рынка (IMPULS/KULMINACIA/BALANCE/INVALIDATED).
    
    ⚠️ ПОРОГИ (ВРЕМЕННЫЕ, до вынесения в конфиг V3.x):
    - price_deviation > 5.0% + net_oi_pct < 5.0% → KULMINACIA
    - price_deviation > 3.0% + net_oi_pct > 10.0% → IMPULS
    - price_deviation < 2.0% + |net_oi_pct| < 3.0% → BALANCE
    
    Считается ЧАСТЬЮ Python Core, НЕ AI-логикой.
    """
    if flow_status != 'ACTIVE':
        return {
            'phase': 'INVALIDATED',
            'description': 'Поток инвалидирован, якорь мёртв',
            'confidence': 'HIGH'
        }
    
    anchor_price = anchor.get('price', 0)
    current_price = metrics.get('current_price', 0)
    net_oi_pct = metrics.get('net_oi_pct') or 0
    
    if anchor_price == 0:
        return {'phase': 'UNKNOWN', 'description': 'Нет данных о цене якоря', 'confidence': 'LOW'}
    
    price_deviation = abs((current_price - anchor_price) / anchor_price * 100)
    
    if price_deviation > 5.0 and net_oi_pct < 5.0:
        return {
            'phase': 'KULMINACIA',
            'description': f'Цена ушла на {price_deviation:.1f}%, OI затухает ({net_oi_pct:.1f}%)',
            'confidence': 'HIGH'
        }
    elif price_deviation > 3.0 and net_oi_pct > 10.0:
        return {
            'phase': 'IMPULS',
            'description': f'Активный набор: цена +{price_deviation:.1f}%, OI +{net_oi_pct:.1f}%',
            'confidence': 'HIGH'
        }
    elif price_deviation < 2.0 and abs(net_oi_pct) < 3.0:
        return {
            'phase': 'BALANCE',
            'description': 'Боковик, нет направленного движения',
            'confidence': 'MEDIUM'
        }
    else:
        return {
            'phase': 'TRANSITION',
            'description': f'Переходное состояние: цена {price_deviation:.1f}%, OI {net_oi_pct:.1f}%',
            'confidence': 'LOW'
        }


def compute_probability(veto: Dict, tam: Dict, anchor: Dict, 
                       metrics: Dict, flow_status: str) -> Dict:
    """
    Расчёт вероятности направления.
    
    Базовая: 50%
    + Уровень 1 (VETO не блокирует): +15%
    + Уровень 2 (OI согласован): +8%
    + Уровень 3 (TAM без аномалий): +4%
    + Уровень 4 (фоновые): +2%
    - Каждая TAM-аномалия: -5%
    """
    if flow_status != 'ACTIVE':
        return {
            'direction': None,
            'probability_pct': 0,
            'confidence': 'NONE',
            'breakdown': {'reason': 'Flow invalidated'}
        }
    
    side = anchor.get('side', 'UNKNOWN')
    prob = 50  # Базовая
    breakdown = {'base': 50}
    
    # Уровень 1: VETO
    veto_down = veto.get('down', False)
    veto_up = veto.get('up', False)
    
    if side == 'LONG' and not veto_up:
        prob += 15
        breakdown['level1_veto'] = '+15'
    elif side == 'SHORT' and not veto_down:
        prob += 15
        breakdown['level1_veto'] = '+15'
    else:
        breakdown['level1_veto'] = '0 (blocked)'
    
    # Уровень 2: OI согласован
    net_oi_pct = metrics.get('net_oi_pct') or 0
    if (side == 'LONG' and net_oi_pct > 0) or (side == 'SHORT' and net_oi_pct < 0):
        prob += 8
        breakdown['level2_oi'] = '+8'
    else:
        breakdown['level2_oi'] = '0'
    
    # Уровень 3: TAM аномалии
    tam_count = sum(1 for k, v in tam.items() if k != 'details' and v is True)
    if tam_count == 0:
        prob += 4
        breakdown['level3_tam'] = '+4'
    else:
        penalty = tam_count * 5
        prob -= penalty
        breakdown['level3_tam'] = f'-{penalty}'
    
    # Уровень 4: фоновые (упрощённо)
    prob += 2
    breakdown['level4_background'] = '+2'
    
    # Ограничения
    prob = max(0, min(100, prob))
    
    confidence = 'HIGH' if prob >= 70 else 'MEDIUM' if prob >= 50 else 'LOW'
    
    return {
        # direction = прогноз движения рынка (UP/DOWN/NONE), НЕ торговая сторона!
        # anchor.side = торговая сторона (LONG/SHORT)
        'direction': 'UP' if side == 'LONG' else 'DOWN' if side == 'SHORT' else None,
        'anchor_side': side,  # Сохраняем для справки
        'probability_pct': prob,
        'confidence': confidence,
        'breakdown': breakdown
    }
```

---

### 1.9. ГЛАВНАЯ ФУНКЦИЯ: ПОЛНЫЙ АНАЛИЗ

```python
def run_full_analysis(symbol: str, exchange: str, start_ts: str, end_ts: str, 
                      limit: int = 500) -> Dict:
    """
    Полный цикл анализа якорной точки + Order Flow.
    
    КРИТИЧНО: 
    - exchange обязателен (binance/bybit)
    - все расчёты делаются на свечах ОДНОГО final_tf
    - смешивание TF только для определения final_tf
    
    Args:
        symbol: Тикер ('ETHUSDT', 'BTCUSDT')
        exchange: Биржа ('binance', 'bybit') — ОБЯЗАТЕЛЬНО
        start_ts: Начало периода (ISO8601 UTC)
        end_ts: Конец периода (ISO8601 UTC)
        limit: Максимум свечей (рекомендуется 500+ для смешанных TF)
    
    Returns:
        JSON-отчёт для передачи AI-интерпретатору
    """
    # 1. Получение ВСЕХ данных (смешанные TF)
    candles_all = fetch_candles(symbol, exchange, start_ts, end_ts, limit)
    
    if not candles_all:
        return {'error': 'Нет данных для анализа'}
    
    # 2. Определяем финальный TF (D1 может перекрывать H4)
    final_info = _pick_final_tf(candles_all)
    final_tf = _normalize_tf(final_info['final_tf'] or 'H4')
    
    # 3. КРИТИЧНО: Фильтруем свечи по final_tf
    #    Все дальнейшие расчёты — только на одном TF!
    candles = _filter_by_tf(candles_all, final_tf)
    
    if not candles or len(candles) < 10:
        return {
            'error': f'Недостаточно данных для final_tf={final_tf}',
            'total_candles_all': len(candles_all),
            'total_candles_final_tf': len(candles)
        }
    
    # 4. Поиск якоря (на отфильтрованных свечах!)
    candidates = scan_candidates(candles)
    anchor = select_anchor(candidates, candles)
    
    if not anchor:
        return {
            'error': 'Валидный якорь не найден',
            'candidates_count': len(candidates)
        }
    
    # 5. Структурный слом
    t_struct = find_structural_break(candles, anchor, final_tf)
    t_flow = t_struct if t_struct is not None else anchor['index']
    
    # 6. Метрики потока
    t_current = len(candles) - 1
    metrics = calculate_flow_metrics(candles, t_flow, t_current)
    
    # 7. Инвалидация потока
    invalidation = check_flow_invalidation(candles, anchor, t_flow, final_tf)
    
    # 8. VETO-фильтры
    core = V3_Deterministic_Core(final_tf)
    veto_result = core.run_analysis(candles)
    
    # 9. TAM аномалии
    baseline = compute_baseline(candles)
    tam = detect_tam_anomalies(candles[-1], baseline, final_tf)
    
    # 10. Фаза рынка (Python считает!)
    phase = compute_phase(anchor, metrics, invalidation['status'])
    
    # 11. Вероятность (Python считает!)
    probability = compute_probability(
        veto=veto_result.get('veto', {}),
        tam=tam,
        anchor=anchor,
        metrics=metrics,
        flow_status=invalidation['status']
    )
    
    # 12. Формирование JSON-отчёта
    report = {
        'metadata': {
            'symbol': symbol,
            'start_ts': start_ts,
            'end_ts': end_ts,
            'final_tf': final_tf,
            'final_ts': final_info['final_ts'],
            'timestamp': candles[-1]['ts'],
            'total_candles_all': len(candles_all),
            'total_candles_final_tf': len(candles)
        },
        'anchor': {
            'index': anchor['index'],
            'type': anchor['type'],
            'side': anchor['side'],
            'price': anchor['price'],
            'timestamp': anchor['timestamp'],
            'tf': final_tf
        },
        'flow': {
            'start_index': t_flow,
            'start_timestamp': candles[t_flow]['ts'],
            'status': invalidation['status'],
            'invalidation_reason': invalidation['reason'],
            'length': t_current - t_flow + 1  # Кол-во свечей в потоке (AI не считает!)
        },
        'phase': phase,
        'probability': probability,
        'metrics': metrics,
        'veto': veto_result.get('veto', {}),
        'efficiency': {
            'er': veto_result.get('er'),
            'sr': veto_result.get('sr'),
            'oe': veto_result.get('oe')
        },
        'tam_anomalies': {
            **tam,
            'count': sum(1 for k, v in tam.items() if k != 'details' and v is True)
        },
        'last_candle': {
            **candles[-1],
            # Добавляем sign поля чтобы AI не считал
            'cvd_sign': 1 if (candles[-1].get('cvd_pct') or 0) > 0 else -1 if (candles[-1].get('cvd_pct') or 0) < 0 else 0,
            'doi_sign': 1 if (candles[-1].get('doi_pct') or 0) > 0 else -1 if (candles[-1].get('doi_pct') or 0) < 0 else 0
        }
    }
    
    return report
```

---

## 🧠 ЧАСТЬ 2: AI INTERPRETER (ИНТЕРПРЕТАЦИЯ)

**Задача AI:** Получить JSON от Python → Сделать человекочитаемый отчёт

### 2.1. ВХОДНОЙ JSON

```json
{
  "metadata": {
    "symbol": "ETHUSDT",
    "start_ts": "2026-01-01T00:00:00Z",
    "end_ts": "2026-02-03T23:59:59Z",
    "final_tf": "H4",
    "final_ts": "2026-02-03T08:00:00Z",
    "timestamp": "2026-02-03T10:00:00Z",
    "total_candles_all": 450,
    "total_candles_final_tf": 120
  },
  "anchor": {
    "index": 52,
    "type": "B",
    "side": "SHORT",
    "price": 45000,
    "timestamp": "2026-02-01T08:00:00Z",
    "tf": "H4"
  },
  "flow": {
    "start_index": 35,
    "start_timestamp": "2026-01-31T20:00:00Z",
    "status": "ACTIVE",
    "invalidation_reason": null,
    "length": 85
  },
  "phase": {
    "phase": "IMPULS",
    "description": "Активный набор: цена +2.2%, OI +12.5%",
    "confidence": "HIGH"
  },
  "probability": {
    "direction": "DOWN",
    "anchor_side": "SHORT",
    "probability_pct": 79,
    "confidence": "HIGH",
    "breakdown": {
      "base": 50,
      "level1_veto": "+15",
      "level2_oi": "+8",
      "level3_tam": "-4",
      "level4_background": "+2"
    }
  },
  "metrics": {
    "net_oi": 125000,
    "net_oi_pct": 12.5,
    "cum_cvd": 45.3,
    "avg_entry": 44500,
    "current_price": 46000,
    "pl_percent": 3.37
  },
  "veto": {
    "down": false,
    "up": false,
    "reasons": []
  },
  "efficiency": {
    "er": 1.8,
    "sr": 0.65,
    "oe": 2.3
  },
  "tam_anomalies": {
    "friction": false,
    "skew": false,
    "fuel_inversion": true,
    "quality": false,
    "crowding": false,
    "vacuum": false,
    "hollow": false,
    "count": 1,
    "details": {
      "fuel_inversion": "price_sign=-1, limb_pct=+15% (ликвидаций шортов больше при падении)"
    }
  },
  "last_candle": {
    "ts": "2026-02-03T08:00:00Z",
    "tf": "H4",
    "open": 45800,
    "high": 46200,
    "low": 45600,
    "close": 46000,
    "cvd_pct": 8.5,
    "cvd_sign": 1,
    "doi_pct": 1.2,
    "doi_sign": 1,
    "oi_change": 5000,
    "oi_total": 1000000,
    "price_sign": 1,
    "body_pct": 65,
    "clv_pct": 75,
    "liq_long": 150000,
    "liq_short": 50000
  }
}
```

---

### 2.2. ОБЯЗАТЕЛЬНАЯ СТРУКТУРА ОТЧЁТА

#### 2.2.1. БЛОК: ЯКОРНАЯ ТОЧКА И ПОТОК

```markdown
## 🎯 ЯКОРНАЯ ТОЧКА И ПОТОК

**ЯКОРЬ (T0):**
- Свеча: #52 (2026-02-01 08:00 UTC)
- Тип события: TYPE B (Пробой с откатом)
- Направление: SHORT (ожидаем падение)
- Цена якоря: 45000
- Статус: ✅ ВАЛИДЕН

**НАЧАЛО ПОТОКА (T_flow):**
- Свеча: #35 (2026-01-31 20:00 UTC)
- Причина: Найден структурный слом (Bearish Break) РАНЬШЕ якоря
- Поток учитывает: {flow.length} свечей от слома до текущей

**СТАТУС ПОТОКА:**
- ✅ АКТИВНЫЙ
- Net OI: +12.5% (позиции набираются)
- Cum CVD: +45.3% (доминируют покупатели)
- Avg Entry: 44500 (средняя цена входа)
- Текущая цена: 46000
- P&L позиций: +3.4% (участники в плюсе)

**ПРОВЕРКА ИНВАЛИДАЦИИ:**
- Обратный структурный слом: ❌ НЕТ
- Net OI обнулился: ❌ НЕТ
- Вывод: Поток жив, тренд продолжается
```

#### 2.2.2. БЛОК: ФАЗА РЫНКА

**⚠️ ВАЖНО: AI НЕ ВЫЧИСЛЯЕТ ФАЗУ. Фаза приходит готовая из JSON (report.phase).**

**Правило:** Если `phase.phase` отсутствует или null → писать `[UNKNOWN]`.

**Формат вывода (только отображение!):**

```markdown
## 📊 ФАЗА РЫНКА

**ФАЗА:** {phase.phase} {emoji}

**ОПИСАНИЕ:** {phase.description}

**УВЕРЕННОСТЬ:** {phase.confidence}
```

**Emoji по фазам:**
- IMPULS → 🚀
- KULMINACIA → ⚡
- BALANCE → ⚖️
- TRANSITION → 🔄
- INVALIDATED → ❌
- UNKNOWN → ❓

#### 2.2.3. БЛОК: VETO И ЭФФЕКТИВНОСТЬ

**⚠️ ПРАВИЛО:** Показываем ТОЛЬКО поля, которые РЕАЛЬНО есть в JSON:  
- `veto.up`, `veto.down`, `veto.reasons`  
- `efficiency.er`, `efficiency.sr`, `efficiency.oe`

**Из старого промпта раздел 8.1:**

```markdown
## 🚫 VETO-ФИЛЬТРЫ

**ИТОГ VETO:**
- Блокировка UP: {veto.up ? "🔴 ДА" : "🟢 НЕТ"}
- Блокировка DOWN: {veto.down ? "🔴 ДА" : "🟢 НЕТ"}
- Причины: {veto.reasons или "нет"}

**ЭФФЕКТИВНОСТЬ (из JSON):**
- ER = {efficiency.er} (порог 0.1)
- SR = {efficiency.sr} (порог 0.5)
- OE = {efficiency.oe}

**ВЫВОД VETO:** {veto.up || veto.down ? "🔴 ЗАБЛОКИРОВАНО" : "🟢 НЕ ЗАБЛОКИРОВАНО"}
Направление: {probability.direction}
```

#### 2.2.4. БЛОК: TAM-V3.3 АНОМАЛИИ

**⚠️ ПРАВИЛО:** AI ТОЛЬКО отображает данные из `tam_anomalies.*`.  
Если флаг `true` → пишем "⚠️ ОБНАРУЖЕН", если `false` → "❌ НЕТ".

**Из старого промпта раздел 12:**

```markdown
## ⚠️ РИСКИ И МАНИПУЛЯЦИИ (TAM-V3.3)

**FRICTION:** {tam_anomalies.friction ? "⚠️ ОБНАРУЖЕН" : "❌ НЕТ"}
**SKEW:** {tam_anomalies.skew ? "⚠️ ОБНАРУЖЕН" : "❌ НЕТ"}
**FUEL INVERSION:** {tam_anomalies.fuel_inversion ? "⚠️ ОБНАРУЖЕН" : "❌ НЕТ"}
**QUALITY:** {tam_anomalies.quality ? "⚠️ ОБНАРУЖЕН" : "❌ НЕТ"}
**CROWDING:** {tam_anomalies.crowding ? "⚠️ ОБНАРУЖЕН" : "❌ НЕТ"}
**VACUUM:** {tam_anomalies.vacuum ? "⚠️ ОБНАРУЖЕН" : "❌ НЕТ"}
**HOLLOW:** {tam_anomalies.hollow ? "⚠️ ОБНАРУЖЕН" : "❌ НЕТ"}

**ДЕТАЛИ:** {tam_anomalies.details}

**ВЫВОД TAM:** {tam_anomalies.count} аномалий обнаружено.
```

#### 2.2.5. БЛОК: СИНТЕЗ УРОВНЕЙ (1-4)

**⚠️ ВАЖНО:** Синтез уровней описывает **локальные сигналы** последней свечи.  
**ИТОГОВОЕ НАПРАВЛЕНИЕ** берётся ТОЛЬКО из `probability.direction` (вычислено Python).

**Из старого промпта разделы 5-6:**

```markdown
## 🎯 СИНТЕЗ СИГНАЛОВ (4 УРОВНЯ)

**УРОВЕНЬ 1 (КРИТИЧНЫЕ):**
- Price_Sign: {last_candle.price_sign}
- Price_vs_Delta: {match/divergence}
- CVD%: {last_candle.cvd_pct}%
- ВЫВОД УРОВНЯ: {локальный сигнал}

**УРОВЕНЬ 2 (ВАЖНЫЕ):**
- DOI%: {last_candle.doi_pct}%
- Limb%: {last_candle.limb_pct}%
- Liq_imbalance: {liq_short > liq_long ?}
- ВЫВОД УРОВНЯ: {локальный сигнал}

**УРОВЕНЬ 3 (ВСПОМОГАТЕЛЬНЫЕ):**
- Body%: {last_candle.body_pct}%
- CLV%: {last_candle.clv_pct}%
- ВЫВОД УРОВНЯ: {локальный сигнал}

**УРОВЕНЬ 4 (ФОНОВЫЕ):**
- Range%: {above/below average}
- Tilt%: {last_candle.tilt_pct}%
- ВЫВОД УРОВНЯ: {локальный сигнал}

**КОНФЛИКТЫ:** {есть/нет конфликтов между уровнями}

**РАСЧЁТ ВЕРОЯТНОСТИ (из JSON):**
{probability.breakdown}

**ИТОГОВОЕ НАПРАВЛЕНИЕ:** {probability.direction} {probability.probability_pct}%
```

#### 2.2.6. БЛОК: ПОЧЕМУ Я МОГУ ОШИБАТЬСЯ

**Из старого промпта раздел 8.13 — ОБЯЗАТЕЛЬНО!**

```markdown
## 🔍 ПОЧЕМУ Я МОГУ ОШИБАТЬСЯ

1. **FUEL INVERSION:** Рост на ликвидациях шортов — топливо ограничено
2. **Якорь SHORT, а цена растёт:** Возможно якорь уже не актуален, ждём инвалидации
3. **P&L +3.4%:** Позиции в плюсе, риск фиксации прибыли
4. **Отсутствие крупного DOI:** +1.2% — слабый набор, нет уверенности крупных игроков
```

#### 2.2.7. БЛОК: ПЛАН ТОРГОВЛИ

**Из старого промпта раздел 13 — дословно!**

```markdown
## 📈 ПЛАН ТОРГОВЛИ

**ГЛОБАЛЬНО (1D):**
- Зона входа: 45000-45500 (якорь + структурный слом)
- Инвалидация: Закрытие дня ниже 44200 (low структурного слома)
- Цель: 47500 (следующий уровень сопротивления)

**ЛОКАЛЬНО (H4):**

**Сценарий 1 (Агрессивный):**
- Вход: 46000 (текущая цена) при условии DOI > 0.8% на следующей свече
- Стоп: 45400 (под откат к якорю)
- Риск: Fuel Inversion — возможен откат

**Сценарий 2 (Консервативный):**
- Вход: 45500 (откат к якорю) при условии Net OI продолжает расти
- Стоп: 44900 (под low якоря)
- Подтверждение: CVD% > 5% на откате вниз

**ИНВАЛИДАЦИЯ:**
- Закрытие H4 свечи ниже 44500 (пробой якоря)

**⚠️ КРИТИЧЕСКИЕ РИСКИ:**
1. **Fuel Inversion:** Топливо истощается, возможен резкий откат
2. **Якорь SHORT:** Противоречие с текущей ценой, ждём обновления якоря
3. **Crowd Exit:** При P&L > 5% риск массовой фиксации прибыли
```

---

### 2.3. ФОРМАТ ИТОГОВОГО РЕЗЮМЕ

```markdown
## 📋 ИТОГОВОЕ РЕЗЮМЕ

**BTCUSDT H4 | 03.02.2026 10:00 UTC**

**ЯКОРЬ:** #52 (TYPE B, SHORT, 45000) — ВАЛИДЕН  
**ПОТОК:** С #35 (структурный слом) — АКТИВНЫЙ (+12.5% Net OI)  
**ФАЗА:** ИМПУЛЬС 🚀 (тренд молодой, идёт набор)  

**НАПРАВЛЕНИЕ:** {probability.direction} {probability.probability_pct}%  
**VETO:** 🟢 НЕ ЗАБЛОКИРОВАНО  
**АНОМАЛИИ:** ⚠️ FUEL INVERSION (рост на ликвидациях шортов)

**ВХОД:** 46000 (агрессивно) | 45500 (консервативно)  
**СТОП:** 45400 | 44900  
**ЦЕЛЬ:** 47500  

**ГЛАВНЫЙ РИСК:** Истощение топлива (ликвидаций шортов). Следить за DOI и Net OI на следующих свечах.
```

---

## 📚 СОХРАНЁННЫЕ РАЗДЕЛЫ ИЗ СТАРОГО ПРОМПТА

**Следующие разделы ОСТАЮТСЯ БЕЗ ИЗМЕНЕНИЙ и применяются AI при интерпретации:**

### ✅ СОХРАНЕНО ДОСЛОВНО:

- **Раздел 0:** Режим и приоритеты (ALWAYS-ON)
- **Раздел 2:** Жёсткие запреты (ZERO TOLERANCE)
- **Раздел 4:** Конкретные пороги по параметрам
- **Раздел 8:** Единый свод правил (V3.4)
  - 8.2: Золотые законы истины
  - 8.3: Фундаментальные правила механики
  - 8.4: Матрица аномалий (TAM-V3.3)
  - 8.5: ONLINE-команды детерминации
  - 8.6: Итоговая матрица принятия решений
  - 8.7: Контекст участка: синтез D1 + H4
  - 8.8-8.12: Все антиошибочные правила
  - 8.13: Жёсткие запреты и требования
- **Раздел 9:** Справочник порогов ORDER FLOW V3
- **Раздел 10:** UTA-V2.1 (Универсальный Алгоритм Истины)
- **Раздел 11:** Форма вывода отчёта (АДИ-V3.5)
- **Раздел 12:** Риски и манипуляции (5 фильтров TAM)
- **Раздел 13:** План торговли (V3.7 OMNISCIENT)

---

## 🔧 УБРАНО ИЗ СТАРОГО ПРОМПТА:

### ❌ УДАЛЕНО (теперь в Python):

- **Раздел 3.1:** Входные данные (47 параметров) — теперь Supabase
- **Раздел 3.2:** Валидация целостности — теперь в Python
- **Раздел 3.3:** Группировка по таймфрейму — теперь SQL запрос
- **СТАДИЯ 2:** Фоновое состояние (BASELINE) — Python считает
- **СТАДИЯ 3:** Бинаризация — Python категоризирует
- **Раздел 6:** Вероятность (PYTHON!) — Python считает
- **Раздел 8.1:** CORE-ENGINE — весь код в Python

---

## 🎯 ФИНАЛЬНЫЙ WORKFLOW

```
1. Python CORE ENGINE запускается:
   ├─ Получает свечи из Supabase
   ├─ Ищет якорь (SCAN → SELECT)
   ├─ Ищет структурный слом (STRUCTURAL OVERRIDE)
   ├─ Считает метрики потока (Net OI, CVD, Avg Entry)
   ├─ Проверяет инвалидацию (STRUCT BREAK, OI RESET)
   ├─ Запускает VETO-фильтры (WALL/ER/SR/GR1)
   ├─ Запускает TAM-V3.3 (7 фильтров аномалий)
   └─ Формирует JSON-отчёт

2. JSON передаётся AI:
   ├─ AI читает JSON
   ├─ Применяет СОХРАНЁННЫЕ правила из старого промпта
   ├─ ОТОБРАЖАЕТ фазу из JSON (report.phase) ⚠️ НЕ ВЫЧИСЛЯЕТ!
   ├─ ОТОБРАЖАЕТ вероятность из JSON (report.probability) ⚠️ НЕ ВЫЧИСЛЯЕТ!
   ├─ Формирует человекочитаемый отчёт
   └─ Добавляет план торговли

3. Отчёт возвращается пользователю
```

---

## 📌 КРИТИЧЕСКИ ВАЖНО

### ДЛЯ PYTHON:
2. **Supabase — единственный источник данных** — не парсим X-Ray
3. **JSON строго типизирован** — AI не додумывает данные
4. **VETO и TAM обязательны** — AI получает уже готовые флаги

### ДЛЯ AI:
1. **Только интерпретация** — не считает, только объясняет
2. **Сохранённые правила обязательны** — разделы 0, 2, 4, 8-13
3. **Формат отчёта строгий** — все обязательные блоки
4. **Якорь и поток в начале** — это теперь центр анализа

```

### AI Prompt:
```
Ты получил JSON-отчёт от Python CORE ENGINE.
Твоя задача: интерпретировать данные и создать человекочитаемый отчёт.

ОБЯЗАТЕЛЬНЫЕ БЛОКИ:
1. Якорная точка и поток
2. Фаза рынка
3. VETO-фильтры
4. TAM-V3.3 аномалии
5. Синтез уровней (1-4)
6. Почему я могу ошибаться
7. План торговли
8. Итоговое резюме

Применяй ВСЕ правила из разделов 0, 2, 4, 8-13 старого промпта.
НЕ считай ничего сам — только интерпретируй готовые цифры.

JSON-отчёт:
{отчёт}
```



