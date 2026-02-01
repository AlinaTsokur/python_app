"""
Tab Reports - UI модуль для парсинга и отображения отчётов по свечам.

Этот модуль отвечает за вкладку "Отчеты" в приложении.
Позволяет вставлять сырые данные свечей, парсить их и отображать X-RAY отчёты.
"""

import streamlit as st
from datetime import datetime


def fmt_num(val):
    """
    Форматирует числовое значение для отображения.
    
    Примеры:
        fmt_num(1234.567) → "1,234.57"
        fmt_num(None) → "—"
    """
    if val is None:
        return "—"
    try:
        return f"{float(val):,.2f}"
    except (ValueError, TypeError):
        return str(val)


def render(db, processor):
    """
    Отрисовывает вкладку "Отчеты".
    
    Аргументы:
        db: Инстанс DatabaseManager для работы с БД
        processor: Инстанс PipelineProcessor для обработки данных
    """
    
    # === СЕКЦИЯ 1: ВВОД ДАННЫХ ===
    # Текстовое поле для вставки сырых данных свечей
    input_text = st.text_area(
        "Вставьте данные свечи", 
        height=150, 
        label_visibility="collapsed", 
        placeholder="Вставьте свечи здесь..."
    )
    
    # Текущая дата/время (для справки, не используется напрямую)
    user_date = datetime.now().date()
    user_time = datetime.now().time()
    
    # === СЕКЦИЯ 2: КНОПКИ УПРАВЛЕНИЯ ===
    # Создаём 3 колонки: кнопка парсинга | кнопка сохранения | пустое место
    col_action, col_save, _ = st.columns([1, 4, 20], gap="small")
    
    # Кнопка "Лапка" - запускает парсинг
    with col_action:
        process = st.button("🐾", type="primary")
    
    # === СЕКЦИЯ 3: ОБРАБОТКА НАЖАТИЯ КНОПКИ ===
    if process and input_text:
        # Вызываем pipeline_processor для обработки сырого текста
        # Возвращает: список обработанных свечей + список ошибок валидации
        final_save_list, orphan_errors = processor.process_batch(input_text)
        
        # Сохраняем результат в session_state (чтобы не потерять при rerun)
        st.session_state.processed_batch = final_save_list
        st.session_state.validation_errors = orphan_errors
        
        # Перезагружаем страницу для отображения результатов
        st.rerun()
    
    # === СЕКЦИЯ 4: ОТОБРАЖЕНИЕ ОШИБОК ВАЛИДАЦИИ ===
    # Если есть ошибки (например, свечи других бирж не совпали с Binance)
    if 'validation_errors' in st.session_state and st.session_state.validation_errors:
        st.error("⛔️ ОШИБКА ВАЛИДАЦИИ КОМПОЗИТА")
        st.warning("Обнаружены данные других бирж, которые не совпали с Binance. Сохранение заблокировано.")
        
        # Выводим каждую ошибку отдельным блоком
        for msg in st.session_state.validation_errors:
            st.code(msg, language="text")
    
    # === СЕКЦИЯ 5: ОТОБРАЖЕНИЕ ОБРАБОТАННЫХ СВЕЧЕЙ ===
    if 'processed_batch' in st.session_state and st.session_state.processed_batch:
        batch = st.session_state.processed_batch
        
        # Кнопка "Сохранить" - сохраняет все свечи в БД
        with col_save:
            if st.button(f"💾 Сохранить {len(batch)}", type="secondary", key="save_btn_top"):
                if db.save_candles_batch(batch):
                    st.toast("Успешно сохранено!", icon="💾")
                    st.cache_data.clear()  # Очищаем кэш для обновления данных
        
        # === СЕКЦИЯ 6: РЕНДЕР КАЖДОЙ СВЕЧИ ===
        for idx, full_data in enumerate(batch):
            
            # Формируем красивую метку времени
            try:
                ts_obj = datetime.fromisoformat(full_data['ts'])
                ts_str = ts_obj.strftime('%d.%m.%Y %H:%M')
            except:
                ts_str = str(full_data.get('ts'))
            
            # Формируем заголовок expandera с предупреждением если есть пропущенные поля
            warn_icon = " ⚠️" if full_data.get('missing_fields') else ""
            label = f"{ts_str} · {full_data.get('exchange')} · {full_data.get('symbol_clean')} · {full_data.get('tf')} · O {fmt_num(full_data.get('open'))}{warn_icon}"
            
            # Раскрывающийся блок для каждой свечи
            with st.expander(label):
                
                # Предупреждение о пропущенных полях (если есть)
                if full_data.get('missing_fields'):
                    st.warning(f"⚠️ Отсутствуют данные: {', '.join(full_data['missing_fields'])}.\nЗначения заменены на 0, чтобы расчеты не упали.")
                
                # Контейнер с фиксированной высотой для отчётов
                with st.container(height=300):
                    
                    # Если есть композитный отчёт - показываем две вкладки
                    if full_data.get('x_ray_composite'):
                        t_xray, t_comp = st.tabs(["X-RAY", "⚡️ COMPOSITE"])
                        
                        # Вкладка X-RAY: основной анализ одной свечи
                        with t_xray:
                            if full_data.get('x_ray'):
                                st.code(full_data['x_ray'], language="yaml")
                        
                        # Вкладка COMPOSITE: сводный анализ по нескольким биржам
                        with t_comp:
                            st.code(full_data['x_ray_composite'], language="yaml")
                    else:
                        # Если композита нет - показываем только X-RAY
                        if full_data.get('x_ray'):
                            st.code(full_data['x_ray'], language="yaml")
