"""Report Generator Module - X-Ray and Composite Reports"""

from datetime import datetime


def fmt_num(val, decimals=2, is_pct=False):
    """Format number for display."""
    if val is None: return "−"
    if isinstance(val, bool): return "true" if val else "false"
    if isinstance(val, (int, float)):
        s = f"{val:,.{decimals}f}".replace(",", " ").replace(".", ",")
        if is_pct: s += "%"
        return s
    return str(val)


def generate_xray(d):
    """Generate X-Ray text report from metrics dictionary."""
    if d.get('ts'):
        try:
            ts_obj = datetime.fromisoformat(d['ts'])
            ts_str = ts_obj.strftime("%d.%m.%Y %H:%M")
        except:
            ts_str = str(d.get('parsed_ts') or d.get('ts'))
    else:
        ts_str = "No Timestamp"
    dr = d.get('dominant_reject') or "−"
    
    lines = [
        f"ts: {ts_str}",
        f"exchange: {d.get('exchange')}",
        f"symbol: {d.get('raw_symbol')}",
        f"tf: {d.get('tf')}",
        f"open: {fmt_num(d.get('open'))}",
        f"high: {fmt_num(d.get('high'))}",
        f"low: {fmt_num(d.get('low'))}",
        f"close: {fmt_num(d.get('close'))}",
        f"volume: {fmt_num(d.get('volume'), 0)}",
        f"buy_volume: {fmt_num(d.get('buy_volume'), 0)}",
        f"sell_volume: {fmt_num(d.get('sell_volume'), 0)}",
        f"buy_trades: {fmt_num(d.get('buy_trades'), 0)}",
        f"sell_trades: {fmt_num(d.get('sell_trades'), 0)}",
        f"oi_open: {fmt_num(d.get('oi_open'), 0)}",
        f"oi_high: {fmt_num(d.get('oi_high'), 0)}",
        f"oi_low: {fmt_num(d.get('oi_low'), 0)}",
        f"oi_close: {fmt_num(d.get('oi_close'), 0)}",
        f"liq_long: {fmt_num(d.get('liq_long'), 0)}",
        f"liq_short: {fmt_num(d.get('liq_short'), 0)}",
        f"range: {fmt_num(d.get('range'))}",
        f"body_pct: {fmt_num(d.get('body_pct'), 2, True)}",
        f"clv_pct: {fmt_num(d.get('clv_pct'), 2, True)}",
        f"upper_tail_pct: {fmt_num(d.get('upper_tail_pct'), 2, True)}",
        f"lower_tail_pct: {fmt_num(d.get('lower_tail_pct'), 2, True)}",
        f"price_sign: {d.get('price_sign')}",
        f"dominant_reject: {dr}",
        f"cvd_pct: {fmt_num(d.get('cvd_pct'), 2, True)}",
        f"cvd_sign: {d.get('cvd_sign')}",
        f"cvd_small: {fmt_num(d.get('cvd_small'))}",
        f"dpx: {fmt_num(d.get('dpx'))}",
        f"price_vs_delta: {d.get('price_vs_delta')}",
        f"dtrades_pct: {fmt_num(d.get('dtrades_pct'), 2, True)}",
        f"ratio_stable: {fmt_num(d.get('ratio_stable'))}",
        f"tilt_pct: {fmt_num(d.get('tilt_pct'), 2, True)}",
        f"doi_pct: {fmt_num(d.get('doi_pct'), 2, True)}",
        f"oi_in_sens: {fmt_num(d.get('oi_in_sens'))}",
        f"oi_set: {fmt_num(d.get('oi_set'))}",
        f"oi_counter: {fmt_num(d.get('oi_counter'))}",
        f"oi_unload: {fmt_num(d.get('oi_unload'))}",
        f"oipos: {fmt_num(d.get('oipos'), 2, True)}",
        f"oi_path: {d.get('oi_path')}",
        f"oe: {fmt_num(d.get('oe'))}",
        f"liqshare_pct: {fmt_num(d.get('liq_share_pct'), 2, True)}",
        f"limb_pct: {fmt_num(d.get('limb_pct'), 2, True)}",
        f"liq_squeeze: {fmt_num(d.get('liq_squeeze'))}",
        f"range_pct: {fmt_num(d.get('range_pct'), 2, True)}",
        f"implied_price: {fmt_num(d.get('implied_price'))}",
        f"avg_trade_buy: {fmt_num(d.get('avg_trade_buy'))}",
        f"avg_trade_sell: {fmt_num(d.get('avg_trade_sell'))}"
    ]
    return "\n".join(lines)


# Backward compatibility alias
generate_full_report = generate_xray


def generate_composite(candles_list):
    """
    Calculates volume-weighted composite report for a group of candles.
    Requires minimum 3 exchanges.
    """
    import math
    
    if not candles_list or len(candles_list) < 3:
        return None

    # Thresholds (matching Google Sheets logic)
    THRESH = {
        'CVD': 1.0, 'TR': 0.5, 'TILT': 2.0,
        'DOI': 0.5, 'LIQ_HIGH': 0.30, 'LIQ_LOW': 0.10
    }

    missing_data_report = {}

    def get_val(d, key):
        v = d.get(key)
        if v is None:
            return None
        return v if (isinstance(v, (int, float)) and not math.isnan(v)) else None

    def sign_char(val, thr):
        if val is None:
            return '?'
        if abs(val) < thr:
            return '0'
        return '+' if val > 0 else '-'

    def dispersion(values, thr):
        valid_vals = [v for v in values if v is not None]
        signs = set()
        for v in valid_vals:
            if v > thr:
                signs.add(1)
            elif v < -thr:
                signs.add(-1)
        return "смешанный" if (1 in signs and -1 in signs) else "ок"

    def weighted(key, metric_name_for_report):
        """Volume-weighted average with missing data tracking."""
        valid_candles = []
        missing_exchanges = []
        
        for c in candles_list:
            if get_val(c, key) is not None:
                valid_candles.append(c)
            else:
                missing_exchanges.append(c.get('exchange', 'Unknown'))
        
        if missing_exchanges:
            missing_data_report[metric_name_for_report] = missing_exchanges

        if not valid_candles:
            return None
        
        subset_vol = sum(get_val(c, 'volume') or 0 for c in valid_candles)
        if subset_vol == 0:
            return None
        
        return sum((get_val(c, key) or 0) * (get_val(c, 'volume') or 0) for c in valid_candles) / subset_vol

    # Calculate metrics
    comp = {
        'cvd':  weighted('cvd_pct', 'CVD'),
        'tr':   weighted('dtrades_pct', 'Trades'),
        'tilt': weighted('tilt_pct', 'Tilt'),
        'doi':  weighted('doi_pct', 'Delta OI'),
        'liq':  weighted('liq_share_pct', 'Liquidation'),
        'clv':  weighted('clv_pct', 'CLV'),
        'upper': weighted('upper_tail_pct', 'Upper Tail'),
        'lower': weighted('lower_tail_pct', 'Lower Tail'),
        'body':  weighted('body_pct', 'Body')
    }

    def safe_fmt(val, dec=2):
        return f"{val:.{dec}f}%" if val is not None else "—"

    # Interpretations
    if comp['liq'] is not None:
        if comp['liq'] > THRESH['LIQ_HIGH']:
            liq_eval = 'ведут ликвидации'
        elif comp['liq'] <= THRESH['LIQ_LOW']:
            liq_eval = 'фон'
        else:
            liq_eval = 'умеренно'
    else:
        liq_eval = '—'

    if comp['tilt'] is not None:
        if comp['tilt'] >= THRESH['TILT']:
            tilt_int = 'sell тяжелее'
        elif comp['tilt'] <= -THRESH['TILT']:
            tilt_int = 'buy тяжелее'
        else:
            tilt_int = 'нейтр'
    else:
        tilt_int = '—'

    if comp['clv'] is not None:
        if comp['clv'] >= 70:
            clv_int = 'принятие сверху'
        elif comp['clv'] <= 30:
            clv_int = 'принятие снизу'
        else:
            clv_int = 'середина диапазона'
    else:
        clv_int = '—'

    # Liq Tilt Sums
    ll_vals = [get_val(c, 'liq_long') for c in candles_list]
    ls_vals = [get_val(c, 'liq_short') for c in candles_list]
    sum_ll = sum(v for v in ll_vals if v is not None)
    sum_ls = sum(v for v in ls_vals if v is not None)
    
    has_liq_data = any(v is not None for v in ll_vals) or any(v is not None for v in ls_vals)
    
    if has_liq_data:
        liq_tilt = 'Long доминируют' if sum_ll > sum_ls else ('Short доминируют' if sum_ls > sum_ll else 'сбалансировано')
    else:
        liq_tilt = '—'

    disp_cvd = dispersion([get_val(c, 'cvd_pct') for c in candles_list], THRESH['CVD'])
    disp_doi = dispersion([get_val(c, 'doi_pct') for c in candles_list], THRESH['DOI'])

    # Per-exchange details
    def fmt_item(c, key, thr):
        val = get_val(c, key)
        if val is None:
            return f"{c.get('exchange','?')} —"
        sign = '(+)' if val > thr else ('(−)' if val < -thr else '(0)')
        return f"{c.get('exchange','?')} {val:.2f}% {sign}"

    per_cvd = "; ".join([fmt_item(c, 'cvd_pct', THRESH['CVD']) for c in candles_list])
    per_tr  = "; ".join([fmt_item(c, 'dtrades_pct', THRESH['TR']) for c in candles_list])
    per_doi = "; ".join([fmt_item(c, 'doi_pct', THRESH['DOI']) for c in candles_list])

    instr = candles_list[0].get('raw_symbol', 'Unknown')
    tf = candles_list[0].get('tf', '-')
    exchanges_str = ", ".join([c.get('exchange','?') for c in candles_list])

    report = f"""КОМПОЗИТНАЯ СВОДКА
• Инструмент/TF: {instr} / {tf} • Биржи: {len(candles_list)} ({exchanges_str})

1) CVD (дельта активного объёма):
   - Композит: {safe_fmt(comp['cvd'])} , знак: {sign_char(comp['cvd'], THRESH['CVD'])} [дисперсия: {disp_cvd}]
   - По биржам: {per_cvd}
2) Δ по числу сделок (Trades):
   - Композит: {safe_fmt(comp['tr'])} , знак: {sign_char(comp['tr'], THRESH['TR'])}
   - По биржам: {per_tr}
3) Перекос среднего размера сделки (Tilt, sell vs buy):
   - Композит: {safe_fmt(comp['tilt'])} , интерпретация: {tilt_int}
4) Ликвидации:
   - Доля: {safe_fmt(comp['liq'])} • Перекос: {liq_tilt} • Оценка: {liq_eval}
5) Open Interest:
   - Композит ΔOI%: {safe_fmt(comp['doi'])} , знак: {sign_char(comp['doi'], THRESH['DOI'])} [дисперсия: {disp_doi}]
   - По биржам: {per_doi}

6) Геометрия свечи:
   - CLV: {safe_fmt(comp['clv'])} ({clv_int})
   - Тени: верхняя {safe_fmt(comp['upper'])} / нижняя {safe_fmt(comp['lower'])}
   - Тело: {safe_fmt(comp['body'])}
"""
    # Warning Section
    if missing_data_report:
        report += "\n⚠️ ВНИМАНИЕ: Неполные данные\n"
        for metric, bad_exchanges in missing_data_report.items():
            report += f"• {metric}: {', '.join(bad_exchanges)} (исключен)\n"

    return report


# Backward compatibility alias
generate_composite_report = generate_composite
