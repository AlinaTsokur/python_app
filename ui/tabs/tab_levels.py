"""
Tab Levels - UI модуль для расчёта уровней поддержки/сопротивления.

Этот модуль отвечает за вкладку "Уровни" в приложении.
Позволяет рассчитывать уровни по историческим данным из БД.
"""

import streamlit as st
import pandas as pd
from datetime import timedelta
import levels_engine


def render(supabase):
    """
    Отрисовывает вкладку "Уровни".
    
    Аргументы:
        supabase: Клиент Supabase для запросов к БД
    """
    
    # === СЕКЦИЯ 1: ФИЛЬТРЫ ===
    c1, c2, c3 = st.columns([1, 1.5, 3], gap="small")
    
    with c1:
        all_tfs = ["1h", "4h", "1d", "1w"]
        selected_tfs_lvl = st.multiselect(
            "TF", 
            all_tfs, 
            default=["4h", "1d"], 
            placeholder="TF", 
            label_visibility="collapsed",
            key="levels_tf_filter"
        )
        
    with c2:
        date_range_lvl = st.date_input(
            "Период", 
            value=[], 
            label_visibility="collapsed",
            key="levels_date_filter"
        )
        
    with c3:
        if st.button("🚀 Рассчитать уровни", type="primary"):
            st.session_state['levels_results'] = {}
            st.session_state['pine_script_dynamic'] = ""
            
            with st.spinner("Считаем уровни..."):
                try:
                    if not selected_tfs_lvl:
                        st.error("⚠️ Выберите хотя бы один таймфрейм!")
                    else:
                        d_start, d_end = None, None
                        if len(date_range_lvl) == 2:
                            d_start, d_end = date_range_lvl
                        elif len(date_range_lvl) == 1:
                            d_start = date_range_lvl[0]
                        
                        levels_results = {}
                        
                        for tf in selected_tfs_lvl:
                            # Query с поддержкой регистра (4h и 4H)
                            query = supabase.table("candles").select("*").in_("tf", [tf.lower(), tf.upper()]).order("ts", desc=True)
                            
                            if d_start:
                                query = query.gte("ts", d_start.isoformat())
                            if d_end:
                                d_end_full = d_end + timedelta(days=1)
                                query = query.lt("ts", d_end_full.isoformat())
                            
                            # Динамические лимиты
                            if not d_start:
                                if tf == "4h":
                                    limit_val = 180
                                elif tf == "1d":
                                    limit_val = 365
                                else:
                                    limit_val = 300
                                query = query.limit(limit_val)
                            else:
                                query = query.limit(1000)

                            res = query.execute()
                            candles = res.data[::-1] if res.data else []
                            
                            if candles:
                                mx = 8 if tf == "1d" else 10
                                lvls = levels_engine.build_levels(
                                    candles, 
                                    lookback=len(candles), 
                                    max_levels=mx, 
                                    timeframe=tf
                                )
                                levels_results[tf.upper()] = lvls
                        
                        st.session_state['levels_results'] = levels_results
                            
                except Exception as e:
                    st.error(f"Ошибка: {e}")

    # === СЕКЦИЯ 2: РЕЗУЛЬТАТЫ ===
    if st.session_state.get('levels_results'):
        st.divider()
        
        if not any(st.session_state['levels_results'].values()):
            st.warning("⚠️ Уровни не найдены. Попробуйте увеличить историю или выбрать другой период.")
            return

        # Текстовый отчёт
        st.subheader("📋 Отчет (Copyable)")
        
        report_lines = []
        for tf, lvls in st.session_state['levels_results'].items():
            if not lvls:
                line = f"**{tf} LEVELS:** (Нет уровней. Мало данных или низкая волатильность)"
            else:
                segments = [f"{l['mid']:.2f} (x{l['touches']})" for l in lvls]
                line = f"{tf} LEVELS: " + " / ".join(segments)
            report_lines.append(line)
            
        full_report = "\n\n".join(report_lines)
        st.code(full_report, language="markdown")

        # Debug expander
        with st.expander("🔍 Детали (отладка)", expanded=False):
            for tf, lvls in st.session_state['levels_results'].items():
                st.markdown(f"**{tf} Debug Data:**")
                if lvls:
                    st.dataframe(pd.DataFrame(lvls), use_container_width=True)
                else:
                    st.text("No levels found.")
