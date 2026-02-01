"""
Tab Lab - UI модуль для лаборатории загрузки batch-данных.

Этот модуль отвечает за вкладку "Лаборатория" в приложении.
Позволяет парсить и загружать обучающие сегменты с метками.
"""

import streamlit as st
import pandas as pd
import batch_parser


def render(supabase, config_loader):
    """
    Отрисовывает вкладку "Лаборатория".
    
    Аргументы:
        supabase: Клиент Supabase для запросов к БД
        config_loader: Функция загрузки конфигурации (load_configurations)
    """
    
    # === СЕКЦИЯ 1: ВВОД ДАННЫХ ===
    lab_text = st.text_area(
        "Batch Input", 
        label_visibility="collapsed", 
        height=300, 
        key="lab_text_area", 
        placeholder="Вставьте свечи и метки (Strong Up/Down)..."
    )
    
    col_lab_parse, col_lab_save, col_lab_status = st.columns([1, 3, 7])
    
    with col_lab_parse:
        if st.button("🐾 ", type="primary"):
            if not lab_text.strip():
                st.warning("Введите текст.")
            else:
                lab_config = config_loader()
                st.session_state['lab_segments'], st.session_state['lab_candles'], st.session_state['lab_warnings'] = batch_parser.parse_batch_with_labels(lab_text, config=lab_config)
                st.session_state['lab_checked'] = True
                st.rerun()

    # === СЕКЦИЯ 2: РЕЗУЛЬТАТЫ ===
    if st.session_state.get('lab_checked'):
        st.divider()
        warnings = st.session_state.get('lab_warnings', [])
        segments = st.session_state.get('lab_segments', [])
        candles = st.session_state.get('lab_candles', [])
        
        # Критические предупреждения
        if warnings:
            st.error(f"⚠️ ОБНАРУЖЕНО {len(warnings)} ПРОБЛЕМ")
            for w in warnings:
                st.markdown(f"- {w}")
            st.warning("Рекомендуем исправить текст перед загрузкой, иначе проблемные сегменты будут пропущены.")
        
        # Статистика
        st.write(f"**Найдено свечей:** {len(candles)}")
        
        # Таблица сегментов
        if segments:
            seg_data = []
            for i, s in enumerate(segments):
                meta = s['META']
                stats = s['CONTEXT']['STATS']
                imp = s['IMPULSE']
                
                row = {
                    "Symbol": meta.get('symbol', 'Unknown'),
                    "TF": meta.get('tf', 'Unknown'),
                    "Direction": imp.get('y_dir'),
                    "Strength": imp.get('y_size'),
                    "Candles": stats.get('candles_count'),
                    "Vol (M)": f"{stats.get('sum_volume', 0)/1_000_000:.2f}M",
                    "Liq Ratio": stats.get('liq_dominance_ratio')
                }
                seg_data.append(row)
            
            if seg_data:
                st.dataframe(pd.DataFrame(seg_data), use_container_width=True)
            
            # Кнопка сохранения
            with col_lab_save:
                if st.button(f"💾 Загрузить {len(segments)} сегментов в БД", type="primary"):
                    with st.spinner("Тотальная запись (Транзакция)..."):
                        try:
                            s_count, c_count = batch_parser.save_batch_transactionally(supabase, segments, candles)
                            with col_lab_status:
                                st.success(f"✅ УСПЕХ! Записано: {s_count} сегментов, {c_count} свечей.")
                            st.balloons()
                            # Очистка state
                            st.session_state['lab_checked'] = False
                            st.session_state['lab_segments'] = []
                        except Exception as e:
                            st.error(f"❌ ОШИБКА ЗАПИСИ: {e}")
                            st.error("Транзакция отменена. Данные откатились (Rollback). База чиста.")
        else:
            st.info("Валидных сегментов не найдено.")
