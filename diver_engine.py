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
    # Strict validation: expect keys to exist (checked by validate_metrics)
    SENS = m.get('tf_sens') 
        
    A_tol = SENS * 0.33
    
    oi_unload = m.get('oi_unload')
    oi_counter = m.get('oi_counter')
    oi_set = m.get('oi_set')
    oi_in_sens = m.get('oi_in_sens', True)
    
    # CVD Noise check
    is_cvd_noise = abs(m.get('cvd_pct', 0) or 0) < (2.0 * SENS / 0.90) if SENS > 0 else False
    
    return {
        "at_edge": at_edge,
        "edge_type": edge_type,
        "edge_status": edge_status,
        
        "sens": SENS,
        "a_tol": A_tol,
        "t_set": m.get('t_set_pct'),      # No default: must exist
        "t_counter": m.get('t_counter_pct'), # No default
        "t_unload": m.get('t_unload_pct'),   # No default
        
        "oi_unload": oi_unload, # No default
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
    Правильная классификация с учетом торговой логики.
    ПАНИКА (разгрузка) > ПОГЛОЩЕНИЕ > остальное
    
    ПРАВКИ:
    1. ✅ Вернули "ПРОПУСК" вместо "none"
    2. ✅ Поправили логику Встречного набора (проверка направления)
    3. ✅ Убрали хардкод 0.30, используем m['liq_squeeze'] и flags.get('liq_threshold')
    4. ✅ Для AQS < 0.50 возвращаем prob_final = 0 (полный игнор)
    5. ✅ Возвращаем 4 значения для совместимости (без prob_mod_composite)
    
    Возвращает: (cls, prob_final, summary, direction)
    """
    
    # ========================================================================
    # 0. GATES (KB 4.6)
    # ========================================================================
    # Используем флаг из app.py, так как там уже учтен глобальный лимит
    liq = m.get('liq_share_pct', 0) or 0
    # Пытаемся получить порог для красивого вывода, если его нет - дефолт 0.30
    liq_threshold = flags.get('liq_threshold', 0.30)
    
    if m.get('liq_squeeze') or liq > liq_threshold:
        return "NO_LABEL", 0, f"Сквиз ликвидаций (LiqShare {liq:.2f}% > {liq_threshold:.2f}%). Пропуск.", "ПРОПУСК"
        
    rng = m.get('range', 0) or 0
    if rng == 0:
        return "NO_LABEL", 0, "Диапазон 0.", "ПРОПУСК"
        
    price_sign = m.get('price_sign', 0)
    cvd_sign = m.get('cvd_sign', 0)
    
    # Gate 3: Мертвая свеча
    if abs(price_sign) < 0.01 and abs(cvd_sign) < 0.01:
        return "NO_LABEL", 0, "Цена и CVD не движутся. Мёртвая свеча.", "ПРОПУСК"
    
    # ========================================================================
    # 1. Divergence Check (KB 4.1)
    # ========================================================================
    dpx = m.get('dpx', 0)
    pvd = m.get('price_vs_delta', 'neutral')
    
    valid_diver = ["mismatch", "div"]
    valid_match = ["match"]
    
    # Basic Divergence flag (Restored for debug/legacy compatibility)
    flag_diver = (dpx == -1) or (pvd in valid_diver)
    
    conflict_comment = ""
    prob_mod = 0
    prob_mod_composite = 0
    
    if dpx == 1 and pvd in valid_match:
        return "NO_LABEL", 0, "Нет дивергенции.", "ПРОПУСК"
    elif (dpx == 1 and pvd in valid_diver) or (dpx == -1 and pvd in valid_match):
        prob_mod = -15
        conflict_comment = " (Конфликт сигналов)"
        
    # CVD Noise Check
    if flags['is_cvd_noise']:
        return "NO_LABEL", 0, "CVD шум", "ПРОПУСК"
    
    # ========================================================================
    # 2. Main Classification (KB 4.7 + ТОРГОВАЯ ЛОГИКА)
    # ========================================================================
    cls = "НЕВОЗМОЖНО_КЛАССИФИЦИРОВАТЬ"
    prob_base = 0
    summary = ""
    direction = "ПРОПУСК"
    
    doi = m.get('doi_pct', 0) or 0
    
    # ========================================================================
    # --- SCENARIO 1: AT_EDGE (ЦЕНА У УРОВНЯ) ---
    # ========================================================================
    if flags['at_edge']:
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # TIER 1: РАЗГРУЗКА (ПАНИКА) ← ГЛАВНЫЙ ДОМИНИРУЮЩИЙ СИГНАЛ!
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if doi <= flags['t_unload']:
            cls = "РАЗГРУЗКА_ПОЗИЦИЙ"
            ratio = abs(doi) / abs(flags['t_unload']) if flags['t_unload'] != 0 else 1
            prob_base = min(ratio * 80, 95)
            direction = "EXIT (ликвидация на уровне)"
            summary = f"⚠️ КРИТИЧЕСКИЙ: ОИ упал ({doi:.2f}%). Массовое закрытие (Unload), риск разворота!"
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # TIER 2: ПОГЛОЩЕНИЕ (контролируемое)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        elif abs(doi) <= flags['a_tol']:
            if aqs >= 0.70:
                cls = "СЕРТИФИЦИРОВАННОЕ_ПОГЛОЩЕНИЕ"
                prob_base = aqs * 100
                direction = "ЛОНГ" if price_sign == 1 else "ШОРТ"
                summary = "Активный лимитный игрок держит уровень."
            elif aqs >= 0.50:
                cls = "РАСХОЖДЕНИЕ_БЕЗ_КЛАССА"
                prob_base = aqs * 100
                direction = "МОНИТОР"
                summary = "Есть расхождение, но AQS < 0.70"
            else:
                # Если AQS < 0.50, полный игнор (prob_final = 0)
                cls = "NO_LABEL"
                prob_base = 0
                direction = "ПРОПУСК"
                summary = "AQS < 0.50, шум"
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # TIER 3: ДИВЕР НА КРОМКЕ
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        elif abs(doi) <= flags['sens']:
            cls = "ДИВЕР_НА_КРОМКЕ"
            prob_base = min(aqs * 100, 60)
            direction = "ОСТОРОЖНО"
            summary = "ОИ на границе пороговой зоны."
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # TIER 4: ВСТРЕЧНЫЙ НАБОР (ранний)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        elif abs(doi) <= flags['t_counter']:
            cls = "ВСТРЕЧНЫЙ_НАБОР"
            prob_base = min(aqs * 80, 85)
            direction = "ЛОНГ (ранний)" if price_sign == 1 else "ШОРТ (ранний)"
            summary = "Встречная позиция формируется."
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # TIER 5: ОЧЕНЬ ВЫСОКИЙ ОИ
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        else:
            # Проверяем: ОИ растет в сторону цены (тренд) или против (встречный)?
            
            if (price_sign == 1 and doi > 0) or (price_sign == -1 and doi < 0):
                # ОИ растет ПО ТРЕНДУ (тренд подтверждается)
                cls = "ПОДТВЕРЖДЕНИЕ_ТРЕНДА"
                prob_base = min(aqs * 85, 90)
                direction = "ЛОНГ (сильный)" if price_sign == 1 else "ШОРТ (сильный)"
                summary = "Сильный импульс: ОИ растет по тренду. Подтверждение направления."
            else:
                # ОИ растет ПРОТИВ ТРЕНДА (встречный набор или дивер)
                cls = "ВСТРЕЧНЫЙ_НАБОР"
                prob_base = aqs * 70
                direction = "РИСК"
                summary = "Сильный ОИ против тренда. Встречный набор или контртренд."
    
    # ========================================================================
    # --- SCENARIO 2: AIR (ЦЕНА В ВОЗДУХЕ) ---
    # ========================================================================
    else:  # at_edge == False
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # TIER 1: РАЗГРУЗКА (в воздухе, БЕЗ уровня)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if doi <= flags['t_unload']:
            cls = "РАЗГРУЗКА_ПОЗИЦИЙ"
            ratio = abs(doi) / abs(flags['t_unload']) if flags['t_unload'] != 0 else 1
            prob_base = min(ratio * 60, 90)
            direction = "EXIT"
            summary = "Активное закрытие позиций в воздухе."
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # TIER 2: РАСХОЖДЕНИЕ
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        elif abs(doi) <= flags['sens']:
            cls = "РАСХОЖДЕНИЕ_БЕЗ_КЛАССА"
            prob_base = aqs * 100
            direction = "МОНИТОР"
            summary = "ОИ слабо движется. Ждём развития."
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # TIER 3: ВСТРЕЧНЫЙ НАБОР (ранний, в воздухе)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        elif abs(doi) <= flags['t_counter']:
            cls = "ВСТРЕЧНЫЙ_НАБОР"
            prob_base = min(aqs * 75, 75)
            direction = "ЛОНГ (ранний)" if price_sign == 1 else "ШОРТ (ранний)"
            summary = "Набор позиций в воздухе (ранний)."
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # TIER 4: ОЧЕНЬ ВЫСОКИЙ ОИ В ВОЗДУХЕ
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        else:
            if (price_sign == 1 and doi > 0) or (price_sign == -1 and doi < 0):
                # ОИ растет ПО ТРЕНДУ (в воздухе тренд подтверждается)
                cls = "ПОДТВЕРЖДЕНИЕ_ТРЕНДА"
                prob_base = min(aqs * 80, 85)
                direction = "ЛОНГ (сильный)" if price_sign == 1 else "ШОРТ (сильный)"
                summary = "Сильный импульс в воздухе: ОИ растет по тренду."
            else:
                # ОИ растет ПРОТИВ ТРЕНДА (встречный набор)
                cls = "ВСТРЕЧНЫЙ_НАБОР"
                prob_base = min(aqs * 65, 75)
                direction = "РИСК"
                summary = "Встречный ОИ в воздухе (высокий)."
    
    # ========================================================================
    # 3. ФИНАЛЬНАЯ КОРРЕКЦИЯ ВЕРОЯТНОСТИ
    # ========================================================================
    prob_final = prob_base + prob_mod
    
    if conflict_comment:
        summary += conflict_comment
    
    # Tilt Penalty
    tilt = m.get('tilt_pct', 0) or 0
    if abs(tilt) >= 10:
        if (price_sign == 1 and tilt < 0) or (price_sign == -1 and tilt > 0):
            prob_final -= 10
            summary += " (Конфликт Tilt)"
    
    # prob_mod_composite добавлен внутрь расчета, но не возвращается наружу
    prob_final += prob_mod_composite
    
    # Для NO_LABEL или AQS < 0.50 → prob_final = 0 (полный игнор)
    if cls == "NO_LABEL":
        prob_final = 0
    elif cls == "НЕВОЗМОЖНО_КЛАССИФИЦИРОВАТЬ":
        prob_final = 0  
    else:
        # Нормализация вероятности (только для валидных классов)
        prob_final = max(min(prob_final, 99), 20)  # Min 20% для валидного сигнала
    
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
    
    # Format Location String
    loc_str = flags['edge_type']
    if flags['at_edge']:
         # Use edge_status (internal code) or map back to UI?
         # Internal code (BREAK, PROBE) is fine and clear.
         loc_str += f" ({flags['edge_status']})"
    
    report = f"""КЛАССИФИКАЦИЯ СВЕЧИ

Дата: {ts_str} | ТФ: {m.get('tf')} | {m.get('symbol_clean')} | Локация: {loc_str}

КЛАСС: {cls}
НАПРАВЛЕНИЕ: {direction}

ВЕРОЯТНОСТИ И ОЦЕНКИ:
• Надёжность сигнала: {prob}% ({q_desc})
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
        "tilt_pct", "range_pct", "implied_price",
        # Strict Validation Keys Added:
        "tf_sens", "t_set_pct", "t_counter_pct", "t_unload_pct"
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
