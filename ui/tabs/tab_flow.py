import streamlit as st
from datetime import datetime, time
import pandas as pd
from core import flow_engine # Импортируем наш новый движок (core engine)

def render(db):
    """
    Рендеринг интерфейса Unified Flow Engine V3.4 (Вкладка "Поток").
    Аргументы:
        db: Экземпляр DatabaseManager, переданный из app.py
    """
    # --- 1. Панель управления (Компактная строка) ---
    # Разбиваем строку на колонки: Символ, Дата1, Время1, Дата2, Время2, Кнопка
    c_sym, c_d1, c_t1, c_d2, c_t2, c_btn = st.columns([1.5, 1.2, 1, 1.2, 1, 1.5])
    
    # Значения по умолчанию: Сегодняшний день и 4 дня назад
    today = datetime.now()
    default_start = today - pd.Timedelta(days=4)

    # Выбор символа (Валютной пары)
    with c_sym:
        symbols = db.get_unique_symbols()
        default_idx = 0
        if "ETHUSDT" in symbols:
            default_idx = symbols.index("ETHUSDT")
        symbol = st.selectbox("Валютная пара", symbols, index=default_idx)

    # Выбор даты и времени НАЧАЛА
    with c_d1:
        # ВАЖНО: st.date_input требует объект date, а не datetime
        d_start = st.date_input("Дата начала", value=default_start.date())
    with c_t1:
        t_start = st.time_input("Время", value=time(0, 0))
        
    # Выбор даты и времени КОНЦА
    with c_d2:
        # ВАЖНО: st.date_input требует объект date
        d_end = st.date_input("Дата конца", value=today.date())
    with c_t2:
        t_end = st.time_input("Время", value=time(23, 59))
        
    # Собираем дату и время в единый datetime
    start_dt = datetime.combine(d_start, t_start)
    end_dt = datetime.combine(d_end, t_end)
    
    # Кнопка запуска
    with c_btn:
        # Добавляем отступы, чтобы выровнять кнопку по высоте с полями ввода
        st.write("") 
        st.write("")
        run_pressed = st.button("ЗАПУСТИТЬ АНАЛИЗ", type="primary", use_container_width=True)

    # --- 2. Обработка действия и Вывод результатов ---
    if run_pressed:
        
        with st.spinner("Анализ данных..."):
            # Вызываем движок (Strict Engine)
            # Передаем параметры, полученные из UI
            report = flow_engine.run_full_analysis(
                db=db,
                symbol=symbol,
                start_ts=start_dt,
                end_ts=end_dt
            )
            
        # Обработка ошибок (Строгий режим)
        # Если есть критические ошибки — выводим их и ОСТАНАВЛИВАЕМ выполнение
        if report.get("errors"):
            for err in report["errors"]:
                st.error(f"❌ {err}")
            st.stop() 

        # Вывод предупреждений (не критично)
        if report.get("warnings"):
            for warn in report["warnings"]:
                st.warning(f"⚠️ {warn}")

        # Карточка Метаданных (Статистика выполнения)
        meta = report["meta"]
        data = report["data"]
        
        st.divider()
        m1, m2, m3 = st.columns(3)
        m1.metric("Загружено (RAW)", data['candles_fetched']) # Сколько строк получено из БД
        m2.metric("После фильтра", data['candles_loaded'])   # Сколько строк попало в диапазон
        m3.metric("Итоговый TF", data['final_tf'] or "N/A")  # Определенный таймфрейм
        
        # Показываем эффективный диапазон дат (реально загруженные данные)
        if meta['effective_start_ts']:
            st.caption(f"Эффективный диапазон: {meta['effective_start_ts']} -> {meta['effective_end_ts']}")

        # Блоки анализа (Заглушки для Этапа 2)
        st.subheader("Результаты анализа")
        
        # Блок Якоря
        if report["anchor"]:
            st.success("⚓ Якорь найден")
            st.json(report["anchor"])
        else:
            st.info("⚓ Якорь: Расчет на Этапе 2")
            
        # Блок Потока
        if report["flow"]:
            st.success("🌊 Поток")
            st.json(report["flow"])
        else:
            st.info("🌊 Поток: Расчет на Этапе 2")

        # Отладочный вывод полного JSON-контракта
        with st.expander("📄 Сырые данные (JSON Contract)", expanded=True):
            st.json(report)
