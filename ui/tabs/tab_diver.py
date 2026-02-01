"""
Tab Diver - UI модуль для анализа дивергенций.

Этот модуль отвечает за вкладку "Дивер" в приложении.
Позволяет анализировать свечи на дивергенции в ручном режиме или из БД.
"""

import streamlit as st
from datetime import datetime
from core.parsing_engine import parse_raw_input, calculate_metrics, fmt_num
from core import diver_engine


def render(db, processor, config_loader, supabase):
    """
    Отрисовывает вкладку "Дивер".
    
    Аргументы:
        db: Инстанс DatabaseManager для работы с БД
        processor: Инстанс PipelineProcessor для обработки данных
        config_loader: Функция загрузки конфигурации (load_configurations)
        supabase: Клиент Supabase для прямых запросов
    """
    
    # === СЕКЦИЯ 1: ВЫБОР РЕЖИМА ===
    mode = st.radio(
        "Источник данных", 
        ["Выбрать из базы данных", "Ручной ввод"], 
        horizontal=True, 
        label_visibility="collapsed"
    )
    
    selected_metrics = None
    
    # === РЕЖИМ РУЧНОГО ВВОДА ===
    if "Ручной" in mode:
        raw_text = st.text_area(
            "Вставьте данные свечи", 
            height=150, 
            label_visibility="collapsed", 
            placeholder="Вставьте свечи здесь...", 
            key="manual_candle_input"
        )
        
        # Кнопка парсинга
        c_paw, _ = st.columns([1, 10])
        with c_paw:
            if st.button("🐾", key="btn_manual_paw", type="primary"):
                if raw_text:
                    try:
                        final_save_list, orphan_errors = processor.process_batch(raw_text)
                        
                        if orphan_errors:
                            st.error("\n".join(orphan_errors))
                        
                        if final_save_list:
                            m = final_save_list[0]
                            st.session_state['manual_diver_candle'] = m
                            st.rerun()
                        elif not orphan_errors:
                            st.warning("Не удалось распознать данные. Проверьте формат.")
                            
                    except Exception as e:
                        st.error(f"Системная ошибка: {e}")
        
        # Отображение результата ручного ввода
        if st.session_state.get('manual_diver_candle'):
            c_left, c_right = st.columns([1, 1])
            
            # --- ЛЕВАЯ ЧАСТЬ: EXPANDER ---
            with c_left:
                m_data = st.session_state['manual_diver_candle']
                try:
                    ts_obj = datetime.fromisoformat(m_data.get('ts'))
                    ts_str = ts_obj.strftime('%d.%m.%Y %H:%M')
                except:
                    ts_str = str(m_data.get('ts', '')).replace('T', ' ')[:16]
                
                warn_icon = " ⚠️" if m_data.get('missing_fields') else ""
                label = f"{ts_str} · {m_data.get('exchange')} · {m_data.get('symbol_clean')} · {m_data.get('tf')} · O {fmt_num(m_data.get('open'))}{warn_icon}"
                
                with st.expander(label, expanded=False):
                    if m_data.get('missing_fields'):
                        st.warning(f"Не найдены поля: {', '.join(m_data['missing_fields'])}")
                         
                    if m_data.get('x_ray_composite'):
                        t_xray, t_comp = st.tabs(["X-RAY", "⚡️ COMPOSITE"])
                        with t_xray:
                            if m_data.get('x_ray'):
                                st.code(m_data['x_ray'], language="yaml")
                        with t_comp:
                            st.code(m_data['x_ray_composite'], language="yaml")
                    else:
                        if m_data.get('x_ray'):
                            st.code(m_data['x_ray'], language="yaml")

            # --- ПРАВАЯ ЧАСТЬ: КОНТРОЛЫ ---
            with c_right:
                _render_analysis_controls(m_data, "manu_diver", "manual_diver_report")
            
            # --- ОТЧЁТ (внизу слева) ---
            if st.session_state.get('manual_diver_report'):
                with c_left:
                    st.code(st.session_state['manual_diver_report'], language="text")

    # === РЕЖИМ ВЫБОРА ИЗ БД ===
    else:
        c_tf, c_date, c_sel = st.columns([1, 1.5, 3], gap="small")
        
        with c_tf:
            all_tfs = ["1m", "5m", "15m", "1h", "4h", "1d", "1w"]
            filter_tfs = st.multiselect(
                "TF", 
                all_tfs, 
                default=[], 
                placeholder="TF", 
                label_visibility="collapsed",
                key="diver_db_tf_filter"
            )
            
        with c_date:
            filter_dates = st.date_input(
                "Период", 
                value=[], 
                label_visibility="collapsed",
                key="diver_db_date_filter"
            )
        
        # Парсинг дат
        d_start, d_end = None, None
        if len(filter_dates) == 2:
            d_start, d_end = filter_dates
        elif len(filter_dates) == 1:
            d_start = filter_dates[0]
             
        db_df = db.load_candles(limit=500, start_date=d_start, end_date=d_end, tfs=filter_tfs)
        
        selected_metrics = None
        
        with c_sel:
            if not db_df.empty:
                options_map = {}
                for idx, row in db_df.iterrows():
                    try:
                        ts_str = str(row['ts']).replace('T', ' ')[:16]
                        label = f"{ts_str} | {row.get('symbol_clean')} | {row.get('tf')} | O: {row.get('open')}"
                        options_map[label] = row.to_dict()
                    except:
                        continue
                
                sel_label = st.selectbox(
                    "Выберите свечу", 
                    list(options_map.keys()),
                    index=None,
                    placeholder="Выберите свечу для анализа",
                    label_visibility="collapsed"
                )
                
                if sel_label:
                    raw_db_metrics = options_map[sel_label]
                    config = config_loader() 
                    selected_metrics = calculate_metrics(raw_db_metrics, config)
            else:
                st.markdown(
                    """
                    <div style="
                        background-color: rgba(100, 181, 246, 0.1); 
                        color: #64B5F6;
                        padding: 8px 12px; 
                        border-radius: 4px; 
                        width: fit-content;
                        font-size: 14px;
                        border: 1px solid rgba(100, 181, 246, 0.2);
                    ">
                        ℹ️ Нет данных
                    </div>
                    """, 
                    unsafe_allow_html=True
                )

        # Отображение выбранной свечи из БД
        if selected_metrics:
            m_data = selected_metrics
            d_left, d_right = st.columns([1, 1])
            
            # --- ЛЕВАЯ ЧАСТЬ ---
            with d_left:
                try:
                    ts_obj = datetime.fromisoformat(str(m_data.get('ts')))
                    ts_str = ts_obj.strftime('%d.%m.%Y %H:%M')
                except:
                    ts_str = str(m_data.get('ts', '')).replace('T', ' ')[:16]
                
                missing_f = m_data.get('missing_fields', [])
                warn_icon = " ⚠️" if missing_f else ""
                
                label = f"{ts_str} · {m_data.get('exchange')} · {m_data.get('symbol_clean')} · {m_data.get('tf')} · O {m_data.get('open')}{warn_icon}"
                
                with st.expander(label, expanded=False):
                    xray_val = m_data.get('x_ray')
                    comp_val = m_data.get('x_ray_composite')
                    
                    if comp_val:
                        t_xray, t_comp = st.tabs(["X-RAY", "⚡️ COMPOSITE"])
                        with t_xray:
                            if xray_val:
                                st.code(xray_val, language="yaml")
                        with t_comp:
                            st.code(comp_val, language="yaml")
                    else:
                        if xray_val:
                            st.code(xray_val, language="yaml")
                             
                # Отчёт под expander
                if st.session_state.get('db_diver_report'):
                    report_txt = st.session_state['db_diver_report']
                    st.code(report_txt, language="text")
                    
                    if st.button("💾 Сохранить отчет в БД", key="save_diver_db_btn"):
                        c_id = m_data.get('id')
                        if c_id:
                            try:
                                supabase.table('candles').update({
                                    'report_diver': report_txt
                                }).eq('id', c_id).execute()
                                st.toast("Отчет сохранен в БД! ✅", icon="✅")
                            except Exception as e:
                                st.error(f"Ошибка сохранения: {e}")
                        else:
                            st.warning("Не найден ID свечи для сохранения.")

            # --- ПРАВАЯ ЧАСТЬ ---
            with d_right:
                _render_db_analysis_controls(m_data, config_loader)


def _render_analysis_controls(m_data, mk_base, report_key):
    """Рендерит контролы анализа для ручного режима."""
    r1, r2, r3 = st.columns([2, 2, 1.5], gap="small")
    
    with r1:
        m_zone = st.selectbox(
            "📍 Зона", 
            ["🌪 В воздухе", "🟢 Поддержка", "🔴 Сопротивление"],
            key=f"zone_{mk_base}",
            label_visibility="collapsed",
            index=None,
            placeholder="📍 Зона"
        )
    
    is_air_m = (m_zone == "🌪 В воздухе")
    
    with r2:
        m_action = st.selectbox(
            "⚡️ Действие", 
            [
                "🛡 Удержание",
                "⚔️ Пробой",
                "🎣 Л.Пробой",
                "🪜 На границе",
                "🕯 Тело на уровне"
            ],
            key=f"act_{mk_base}",
            label_visibility="collapsed",
            index=None,
            placeholder="⚡️ Действие" if not is_air_m else "⛔️ Недоступно в воздухе",
            disabled=is_air_m
        )
    
    with r3:
        if st.button("🔮 Анализ", key=f"btn_{mk_base}", type="primary", use_container_width=True):
            z_map = {
                "🌪 В воздухе": "Air",
                "🟢 Поддержка": "Support",
                "🔴 Сопротивление": "Resistance"
            }
            a_map = {
                "🛡 Удержание": "AT_EDGE",
                "⚔️ Пробой": "BREAK",
                "🎣 Л.Пробой": "PROBE",
                "🪜 На границе": "AT_EDGE_BORDERLINE",
                "🕯 Тело на уровне": "AT_EDGE_TAIL"
            }
            
            zone_code = z_map.get(m_zone)
            action_code = a_map.get(m_action)
            
            if not zone_code or (not action_code and zone_code != "Air"):
                st.toast("⚠️ Выберите Зону и Действие!", icon="⚠️")
            else:
                report = diver_engine.run_expert_analysis(m_data, zone_code, action_code)
                st.session_state[report_key] = report
                st.rerun()


def _render_db_analysis_controls(m_data, config_loader):
    """Рендерит контролы анализа для режима БД с ITB."""
    mk_base = "db_diver"
    
    r1, r2, r3, r4 = st.columns([1.2, 1.2, 0.7, 0.7], gap="small")
    
    with r1:
        d_zone = st.selectbox(
            "📍 Зона", 
            ["🌪 В воздухе", "🟢 Поддержка", "🔴 Сопротивление"],
            key=f"zone_{mk_base}",
            label_visibility="collapsed",
            index=None,
            placeholder="📍 Зона"
        )
    
    is_air_d = (d_zone == "🌪 В воздухе")
    
    with r2:
        d_action = st.selectbox(
            "⚡️ Действие", 
            [
                "🛡 Удержание",
                "⚔️ Пробой",
                "🎣 Л.Пробой",
                "🪜 На границе",
                "🕯 Тело на уровне"
            ],
            key=f"act_{mk_base}",
            label_visibility="collapsed",
            index=None,
            placeholder="⚡️ Действие" if not is_air_d else "⛔️ Недоступно в воздухе",
            disabled=is_air_d
        )
    
    z_map = {
        "🌪 В воздухе": "Air",
        "🟢 Поддержка": "Support",
        "🔴 Сопротивление": "Resistance"
    }
    a_map = {
        "🛡 Удержание": "AT_EDGE",
        "⚔️ Пробой": "BREAK",
        "🎣 Л.Пробой": "PROBE",
        "🪜 На границе": "AT_EDGE_BORDERLINE",
        "🕯 Тело на уровне": "AT_EDGE_TAIL"
    }

    with r3:
        if st.button("🔮 Анализ", key=f"btn_{mk_base}", type="primary", use_container_width=True):
            zone_code = z_map.get(d_zone)
            action_code = a_map.get(d_action)
            
            if not zone_code or (not action_code and zone_code != "Air"):
                st.toast("⚠️ Выберите Зону и Действие!", icon="⚠️")
            else:
                report = diver_engine.run_expert_analysis(m_data, zone_code, action_code)
                st.session_state['db_diver_report'] = report
                st.rerun()

    with r4:
        if st.button("🛠 ИТБ", type="secondary", key="btn_toggle_itb", use_container_width=True):
            st.session_state['show_itb_form'] = not st.session_state.get('show_itb_form', False)

    # --- ITB ФОРМА ---
    if st.session_state.get('show_itb_form'):
        itb_ph = f"Вставьте данные нарезки ({str(m_data.get('ts'))})..."
        itb_text = st.text_area(
            "Данные нарезки", 
            height=200, 
            key="itb_input_area", 
            label_visibility="collapsed", 
            placeholder=itb_ph
        )
         
        if st.button("🚀 Запустить ITB Анализ", type="primary", key="btn_run_itb_real"):
            if not itb_text.strip():
                st.error("Пустой ввод!")
            else:
                slices = []
                config = config_loader()
                lines = itb_text.strip().split('\n')
                is_valid = True
                
                for i, line in enumerate(lines):
                    if not line.strip():
                        continue
                    try:
                        raw_s = parse_raw_input(line)
                        met_s = calculate_metrics(raw_s, config)
                        slices.append(met_s)
                    except Exception as e:
                        st.error(f"Ошибка в строке {i+1}: {e}")
                        is_valid = False
                        break
                
                if is_valid:
                    try:
                        z_code = z_map.get(d_zone)
                        a_code = a_map.get(d_action)
                        if z_code and (a_code or z_code == "Air"):
                            base_cls, base_prob = diver_engine.get_base_analysis(m_data, z_code, a_code)
                            m_data['cls'] = base_cls
                            m_data['prob_final'] = base_prob
                        
                        res_itb = diver_engine.run_intrabar_analysis(m_data, slices)
                        st.session_state['itb_result'] = res_itb
                    except Exception as e:
                        st.error(f"Ошибка движка ITB: {e}")
    
    # Результат ITB
    if st.session_state.get('itb_result'):
        st.code(st.session_state['itb_result'], language="text")
