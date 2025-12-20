import math
import re

# --- HELPER FUNCTIONS ---
def sign(x):
    if x > 0: return 1
    elif x < 0: return -1
    else: return 0



# --- 1. PREPARE LOGIC FLAGS ---
def prepare_logic_flags(m, location_ui):
    """
    Превращает выбор в UI (Зона + Действие) в логические флаги Секции 4.2.
    KB Section 4.2 PATTERNS → UI SELECTORS
    """
    # 1. Получаем выбор из UI
    ui_zone = location_ui.get('zone', 'Air')           # значение селектора 1
    ui_action = location_ui.get('action', 'Hold')      # значение селектора 2
    
    # Дефолтные значения
    at_edge = False
    edge_type = "AIR"
    edge_status = "NO_EDGE"

    # ===== ЛОГИКА СЕКЦИИ 4.2: ПАРСИНГ ЛОКАЦИИ =====
    
    # ОСНОВНОЙ ПАТТЕРН: Зона (определяет at_edge + edge_type)
    if ui_zone == "Air":
        at_edge = False
        edge_type = "AIR"
        edge_status = "NO_EDGE"
        
    elif ui_zone == "Support":
        at_edge = True
        edge_type = "S"
        
    elif ui_zone == "Resistance":
        at_edge = True
        edge_type = "R"
    
    else:
        # Неизвестное значение зоны → дефолт (Air)
        at_edge = False
        edge_type = "AIR"
        edge_status = "NO_EDGE"
    
    # РАСШИРЕННЫЙ СТАТУС: Действие (определяет edge_status у уровня)
    # Применяется ТОЛЬКО если at_edge == TRUE
    if at_edge == True:
        if ui_action == "BREAK":
            edge_status = "BREAK"           # KB: "закрылась за уровнем"
            
        elif ui_action == "PROBE":
            edge_status = "PROBE"           # KB: "прошел тело, тень вернулась"
            
        elif ui_action == "AT_EDGE_BORDERLINE":
            edge_status = "AT_EDGE_BORDERLINE"  # KB: "на границе"
            
        elif ui_action == "AT_EDGE_TAIL":
            edge_status = "AT_EDGE_TAIL"    # KB: "тело на уровне, тень выше"
            
        else:  # ui_action == "AT_EDGE" / "Hold" или другое
            edge_status = "AT_EDGE"         # KB: базовое "под R" / "над S"

    # ===== ОСТАЛЬНАЯ ЛОГИКА (БЕЗ ИЗМЕНЕНИЙ) =====
    
    # SENS now comes strictly from tf_params (t_base in app.py)
    SENS = m.get('tf_sens', 0.5)
    A_tol = SENS * 0.33
    
    oi_unload = m.get('oi_unload', False)
    oi_counter = m.get('oi_counter', False)
    oi_set = m.get('oi_set', False)
    oi_in_sens = m.get('oi_in_sens', True)
    
    # CVD Noise check
    is_cvd_noise = abs(m.get('cvd_pct', 0) or 0) < (2.0 * SENS / 0.90) if SENS > 0 else False
    
    return {
        "at_edge": at_edge,
        "edge_type": edge_type,
        "edge_status": edge_status,
        
        "sens": SENS,
        "a_tol": A_tol,
        "t_set": m.get('t_set_pct', 0),
        "t_counter": m.get('t_counter_pct', 0),
        "t_unload": m.get('t_unload_pct', 0),
        
        "oi_unload": oi_unload,
        "oi_counter": oi_counter,
        "oi_set": oi_set,
        "oi_in_sens": oi_in_sens,
        
        "is_cvd_noise": is_cvd_noise,
        
        # Pass raw UI action for logging if needed, though strictly edge_status is key
        "ui_action": ui_action
    }

# --- 2. CALCULATE AQS ---
def calculate_aqs(m, flags):
    """
    Calculates AQS based on KB Section 4.5
    AQS = (Geometry * 0.50) + (Flow * 0.30) + (OI * 0.20)
    """
    price_sign = m.get('price_sign', 0)
    cvd_sign = m.get('cvd_sign', 0)
    clv = m.get('clv_pct', 50)
    lt = m.get('lower_tail_pct', 0)
    ut = m.get('upper_tail_pct', 0)
    
    # A. Geometry (0.50)
    geom_score = 0.0
    # Optional edge quality mod from dominant_reject (KB 4.4)
    # B. Parsing Dominant Reject (KB 4.4)
    # Corrected to strict nested structure
    edge_quality_mod = 0.0
    dr = m.get('dominant_reject', '-')
    
    if dr is None or dr == '-':
        edge_quality_mod = 0.0
        
    elif 'bull' in str(dr) and ('Valid' in str(dr) or 'Ideal' in str(dr)):
        # ВЕРХНЯЯ ТЕНЬ (бычий отказ)
        if price_sign == 1 and cvd_sign == -1:
            edge_quality_mod = 0.05
        else:
            edge_quality_mod = -0.05
            
    elif 'bear' in str(dr) and ('Valid' in str(dr) or 'Ideal' in str(dr)):
        # НИЖНЯЯ ТЕНЬ (медвежий отказ)
        if price_sign == -1 and cvd_sign == 1:
            edge_quality_mod = 0.05
        else:
            edge_quality_mod = -0.05

    if price_sign == 1 and cvd_sign == -1: # Long Setup
        # CLV Score
        if clv >= 65: clv_s = 1.0
        elif clv >= 60: clv_s = 0.9
        elif clv >= 55: clv_s = 0.7
        elif clv >= 50: clv_s = 0.5
        else: clv_s = 0.0
        
        # Tail Score
        if lt >= 35: tail_s = 1.0
        elif lt >= 30: tail_s = 0.8
        elif lt >= 25: tail_s = 0.6
        else: tail_s = 0.0
        
        geom_score = (clv_s * 0.6) + (tail_s * 0.4)
        
    elif price_sign == -1 and cvd_sign == 1: # Short Setup
        if clv <= 35: clv_s = 1.0
        elif clv <= 40: clv_s = 0.9
        elif clv <= 45: clv_s = 0.7
        elif clv <= 50: clv_s = 0.5
        else: clv_s = 0.0
        
        if ut >= 35: tail_s = 1.0
        elif ut >= 30: tail_s = 0.8
        elif ut >= 25: tail_s = 0.6
        else: tail_s = 0.0
        
        geom_score = (clv_s * 0.6) + (tail_s * 0.4)
    
    geom_score = max(0.0, min(1.0, geom_score + edge_quality_mod))
    
    # B. Flow Coherence (0.30)
    ratio_s = 1.0 if m.get('ratio_stable') else 0.5
    dtrades = abs(m.get('dtrades_pct', 0) or 0)
    
    if dtrades >= 2.0: dt_s = 1.0
    elif dtrades >= 1.0: dt_s = 0.8
    elif dtrades >= 0.5: dt_s = 0.6
    else: dt_s = 0.3
    
    tilt = m.get('tilt_pct', 0) or 0
    tilt_sign_check = 1.0 if (sign(tilt) == cvd_sign) else 0.5
    
    oe = m.get('oe', 0) or 0
    oe_s = min(oe / 1.5, 1.0)
    
    flow_score = (ratio_s * 0.4) + (dt_s * 0.35) + (tilt_sign_check * 0.15) + (oe_s * 0.10)
    flow_score = max(0.0, min(1.0, flow_score))
    
    # C. OI Cleanliness (0.20)
    doi = abs(m.get('doi_pct', 0) or 0)
    zone_threshold = flags['a_tol'] if flags['at_edge'] else flags['sens']
    
    if doi <= zone_threshold: oi_zone_s = 1.0
    elif doi <= zone_threshold * 2: oi_zone_s = 0.7
    else: oi_zone_s = 0.3
    
    liq = m.get('liq_share_pct', 0) or 0
    if liq <= 0.20: liq_s = 1.0
    elif liq <= 0.30: liq_s = 0.7
    else: liq_s = 0.3
    
    oi_score = (oi_zone_s * 0.7) + (liq_s * 0.3)
    oi_score = max(0.0, min(1.0, oi_score))
    
    aqs = (geom_score * 0.50) + (flow_score * 0.30) + (oi_score * 0.20)
    return round(aqs, 3)

# --- 3. CLASSIFIER ---
def classify_main(m, flags, aqs):
    """
    Main Logic tree KB Section 4.7
    """
    
    # 0. GATES (KB 4.6)
    # Безопасное извлечение данных (None -> 0)
    liq = m.get('liq_share_pct', 0) or 0
    rng = m.get('range', 0) or 0
    rng_pct = m.get('range_pct', 0) or 0
    
    # Gate 1: Сквиз Ликвидаций
    if m.get('liq_squeeze') or liq > 0.30:
        return "NO_LABEL", 0, f"Сквиз ликвидаций (LiqShare = {liq:.2f}% > 30%). Первый тест — пропуск.", "none"
    
    # Gate 2: Отсутствие диапазона (Флэт/Доджи)
    # Строго по БЗ: range == 0 или range_pct < 0.01
    elif rng == 0 or rng_pct < 0.01:
        return "NO_LABEL", 0, "Диапазон = 0. Нет ясной структуры свечи.", "none"
    
    # Gate 3: Мертвая свеча (Цена и CVD стоят)
    price_sign = m.get('price_sign', 0)
    cvd_sign = m.get('cvd_sign', 0)
    
    if abs(price_sign) < 0.01 and abs(cvd_sign) < 0.01:
        return "NO_LABEL", 0, "Цена и CVD не движутся. Мёртвая свеча.", "none"
    
    # 1. Divergence Check
    dpx = m.get('dpx', 0)
    pvd = m.get('price_vs_delta', 'neutral')
    
    # KB 4.1 Divergence Flag Parsing
    valid_diver = ["mismatch", "div"]
    valid_match = ["match"]
    
    # Basic Divergence flag
    flag_diver = (dpx == -1) or (pvd in valid_diver)
    
    conflict_comment = ""
    prob_mod = 0
    
    if dpx == 1 and pvd in valid_match:
         # No Diver
         return "NO_LABEL", 0, "Нет дивергенции цены и агрессии.", "none"
    elif dpx == -1 and pvd in valid_diver:
         # Clean Diver
         pass
    elif (dpx == 1 and pvd in valid_diver) or (dpx == -1 and pvd in valid_match):
         # Conflict
         prob_mod = -15
         conflict_comment = " (Конфликт сигналов: dpx и price_vs_delta противоречат, dpx приоритет)"
    else:
         prob_mod = -10
         conflict_comment = " (Конфликт: неясное состояние флагов)"
         
    # Check CVD Noise
    if flags['is_cvd_noise']:
        # Recalculate threshold for display (as used in prepare_logic_flags)
        sens = flags['sens']
        cvd_min = (2.0 * sens / 0.90) if sens > 0 else 0
        cvd_pct = m.get('cvd_pct', 0)
        return "NO_LABEL", 0, f"|CVD%| = {cvd_pct:.2f}% < CVD_MIN = {cvd_min:.2f}% → шум, не анализируем", "none"
        
    # 2. Main Classification Logic (KB 4.7)
    
    cls = "НЕВОЗМОЖНО_КЛАССИФИЦИРОВАТЬ"
    prob_base = 0
    summary = ""
    direction = "none"
    
    doi = m.get('doi_pct', 0) or 0
    
    # --- SCENARIO 1: AT EDGE ---
    if flags['at_edge']:
        
        if flags['oi_unload']:
             cls = "РАЗГРУЗКА_ПОЗИЦИЙ"
             prob_base = 85
             direction = "EXIT"
             summary = "Разгрузка позиций на уровне. Деньги уходят."
             
        elif abs(doi) <= flags['a_tol']:
             if aqs >= 0.70:
                 cls = "СЕРТИФИЦИРОВАННОЕ_ПОГЛОЩЕНИЕ"
                 prob_base = aqs * 100
                 direction = "ЛОНГ" if price_sign == 1 else "ШОРТ"
                 summary = "Активный лимитный игрок держит уровень (Absorption)."
             elif aqs >= 0.50:
                 cls = "РАСХОЖДЕНИЕ_БЕЗ_КЛАССА"
                 prob_base = aqs * 100
                 summary = "Есть расхождение, но AQS недостаточно высок для подтверждения."
             else:
                 cls = "НЕВОЗМОЖНО_КЛАССИФИЦИРОВАТЬ"
                 summary = "AQS < 0.50, шум."
                 
        elif abs(doi) <= flags['sens']: # Between A_tol and SENS
             cls = "ДИВЕР_НА_КРОМКЕ"
             prob_base = min(aqs * 100, 60)
             direction = "ОСТОРОЖНО"
             summary = "ОИ на границе пороговой зоны."
             
        elif (doi > flags['sens']) and (doi <= flags['t_counter']):
             # KB says: SENS < doi <= T_vstrechniy.
             cls = "ВСТРЕЧНЫЙ_НАБОР"
             prob_base = min(aqs * 80, 85)
             direction = "ЛОНГ" if price_sign == 1 else "ШОРТ"
             summary = "Встречная позиция формируется, ОИ растет умеренно."
             
        else: # > T_counter
             cls = "ВСТРЕЧНЫЙ_НАБОР"
             prob_base = aqs * 70
             direction = "РИСК"
             summary = "Сильный рост ОИ. Возможен пробой или климакс."

    # --- SCENARIO 2: AIR ---
    else:
        if doi <= -flags['sens']: # KB: doi <= -SENS
            cls = "РАЗГРУЗКА_ПОЗИЦИЙ"
            # Recalculate prob base relative to magnitude? Or just static high?
            # KB says Unload is strong signal.
            # Use strict math for prob?
            # Existing code used t_unload. Let's use SENS as base for scale or just static.
            # "min(abs(doi / SENS) * 100, 90)" makes sense?
            # User code had prob_base = 85 for unload? 
            # In previous snippet it was dynamic.
            # Let's use dynamic scaling against SENS or hard 85?
            # "min(abs(doi / flags['sens']) * 50, 90)" maybe too low if doi=sens.
            # Let's keep it safe:
            ratio = abs(doi) / flags['sens'] if flags['sens'] > 0 else 1
            prob_base = min(ratio * 60, 90) # if doi = -2*sens -> 120 -> 90. if doi = -sens -> 60.
            
            direction = "EXIT"
            summary = "Активное закрытие позиций против цены."
            
        elif abs(doi) < flags['sens']:
            cls = "РАСХОЖДЕНИЕ_БЕЗ_КЛАССА"
            prob_base = aqs * 100
            direction = "МОНИТОР"
            summary = "Дивергенция без поддержки ОИ."
            
        elif (doi > flags['sens']) and (doi <= flags['t_counter']):
            cls = "ВСТРЕЧНЫЙ_НАБОР" # Early
            prob_base = min(aqs * 75, 75)
            direction = "ЛОНГ (ранний)" if price_sign == 1 else "ШОРТ (ранний)"
            summary = "Набор позиций в воздухе. Ранний сигнал."
            
        else:
            cls = "ВСТРЕЧНЫЙ_НАБОР"
            prob_base = min(aqs * 65, 75)
            direction = "РИСК"
            summary = "Сильный встречный ОИ в воздухе."
            
    # Final Probability mods
    prob_final = prob_base + prob_mod
    
    # Append conflict info if any
    if conflict_comment:
        summary += conflict_comment
    
    # Tilt Penalty
    tilt = m.get('tilt_pct', 0) or 0
    if abs(tilt) >= 10:
        if (price_sign == 1 and tilt < 0) or (price_sign == -1 and tilt > 0):
            prob_final -= 10
            summary += " (Conflict Tilt)"

    if cls == "NO_LABEL": prob_final = 0
    prob_final = max(min(prob_final, 99), 15) if cls != "NO_LABEL" else 0
    
    return cls, round(prob_final), summary, direction


# --- 4. GENERATE REPORT ---
def generate_diver_report(m, location_ui):
    """
    Generates text report KB 6.1
    """
    flags = prepare_logic_flags(m, location_ui)
    aqs = calculate_aqs(m, flags)
    cls, prob, summary, direction = classify_main(m, flags, aqs)
    
    ts_str = str(m.get('ts', '')).replace('T', ' ')[:16]
    
    # Quality Desc
    if prob >= 75: q_desc = "Высокая"
    elif prob >= 50: q_desc = "Средняя"
    else: q_desc = "Низкая"
    
    report = f"""КЛАССИФИКАЦИЯ СВЕЧИ

Дата: {ts_str} | ТФ: {m.get('tf')} | {m.get('symbol_clean')} | Локация: {flags['edge_type']} ({location_ui.get('action')})

КЛАСС: **{cls}**
НАПРАВЛЕНИЕ: **{direction}**

ВЕРОЯТНОСТИ И ОЦЕНКИ:
• Надёжность сигнала: **{prob}%** ({q_desc})
• AQS Балл: {aqs:.2f}

ЧТО ПРОИЗОШЛО:
{summary}

ФАКТОРЫ АНАЛИЗА:
• CVD: {m.get('cvd_pct', 0):.2f}% ({'Шум' if flags['is_cvd_noise'] else 'Активно'})
• ΔOI: {m.get('doi_pct', 0):.2f}% (Порог: {m.get('porog_final', 0):.2f}%)
• Геометрия: CLV {m.get('clv_pct'):.0f}%, Хвост L:{m.get('lower_tail_pct'):.0f}% / U:{m.get('upper_tail_pct'):.0f}%
• Локация: {'У уровня' if flags['at_edge'] else 'В воздухе'}
• Поток сделок: Tilt {m.get('tilt_pct'):.1f}%

ВЫВОД: Ситуация {'требует внимания' if prob > 50 else 'неопределенная/шум'}.

ДЕЙСТВИЕ: {'Рассмотреть вход' if prob > 60 else 'Ждать подтверждения'}.

"""
    return report

# --- 3.5 VALIDATION ---
def validate_metrics(data):
    """
    Validates existence of mandatory fields (User Req 3.1).
    Uses internal App field names.
    Returns None if valid, else Error String.
    """
    # Required keys in our system (app.py)
    required_keys = [
        "ts", "symbol_clean", "tf", "range", "body_pct", "clv_pct",
        "upper_tail_pct", "lower_tail_pct", "price_sign", "cvd_pct",
        "cvd_sign", "dtrades_pct", "ratio_stable", "doi_pct",
        "liq_share_pct", "liq_squeeze", "oe", "oipos", "oi_path",
        "dpx", "price_vs_delta", "avg_trade_buy", "avg_trade_sell",
        "tilt_pct", "range_pct", "implied_price"
    ]
    
    missing = []
    for key in required_keys:
        if key not in data or data.get(key) is None:
             # Exception: 'range' might be 0.0 (valid). 'None' is invalid.
             missing.append(key)
    
    if missing:
        return f"ERROR: Отсутствуют поля: {', '.join(missing)}. Заполните или проверьте парсинг."
    
    return None

def run_expert_analysis(metrics_data, zone, action):
    # 1. Validation
    err = validate_metrics(metrics_data)
    if err:
        return err
        
    # 2. Generation
    return generate_diver_report(metrics_data, {'zone': zone, 'action': action})
