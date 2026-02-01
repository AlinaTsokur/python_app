"""
Tab Training - UI модуль для обучения модели.

Этот модуль отвечает за вкладку "Обучение" в приложении.
Запускает 6-этапный конвейер обучения модели.
"""

import streamlit as st
import json
from pathlib import Path
from offline import stage1_loader, stage2_features, stage3_bins, stage4_rules, stage5_bins_stats, stage6_mine_stats


def render():
    """Отрисовывает вкладку "Обучение"."""
    
    st.header("🏁 Центр Обучения Модели (V2.1)")
    
    col_cfg, col_stat = st.columns([1, 2])
    
    with col_cfg:
        st.subheader("1. Параметры")
        tr_symbol = st.selectbox("Тикер", ["ETH", "BTC", "SOL", "BNB"], index=0)
        tr_tf = st.selectbox("Таймфрейм", ["1D", "4h", "1h", "15m"], index=0)
        tr_exchange = st.text_input("Биржа", "Binance")
        tr_profile = st.selectbox(
            "Профиль токенов", 
            ["STRICT", "SMALLN"], 
            index=1, 
            help="STRICT: полные бины (Q1-Q5), SMALLN: сжатые зоны (LOW/MID/HIGH)"
        )
        
        start_btn = st.button("🚀 ЗАПУСТИТЬ ОБУЧЕНИЕ", type="primary", use_container_width=True)
        
    with col_stat:
        st.subheader("2. Прогресс")
        
        if start_btn:
            _run_training_pipeline(tr_symbol, tr_tf, tr_exchange, tr_profile)


def _run_training_pipeline(symbol, tf, exchange, profile):
    """Запускает 6-этапный конвейер обучения."""
    
    status = st.status("Запуск конвейера...", expanded=True)
    
    # Шаг 1: Загрузка данных
    status.write("📥 Шаг 1: Загрузка данных (Offline Pooling)...")
    success1, msg1, count1 = stage1_loader.run_pipeline(symbol, tf, exchange)
    
    if not success1:
        status.update(label="❌ Ошибка на этапе загрузки!", state="error")
        st.error(msg1)
        return
    
    status.write(f"✅ Данные загружены: {msg1}")
    
    # Шаг 2: Генерация признаков
    status.write("🧠 Шаг 2: Генерация признаков (Simulation)...")
    try:
        success2, msg2, count2 = stage2_features.run_simulation(symbol, tf, exchange)
        
        if not success2:
            status.update(label="❌ Ошибка генерации признаков!", state="error")
            st.error(msg2)
            return
        
        status.write(f"✅ Признаки созданы: {msg2}")
    except Exception as e:
        status.update(label="❌ Критическая ошибка (Stage 2)", state="error")
        st.error(str(e))
        return
    
    # Шаг 3: Построение bins
    status.write("📊 Шаг 3: Построение bins (квантили)...")
    try:
        success3, msg3 = stage3_bins.run_binning(symbol, tf, exchange)
        
        if not success3:
            status.update(label="❌ Ошибка построения bins!", state="error")
            st.error(msg3)
            return
        
        status.write(f"✅ Bins созданы: {msg3}")
    except Exception as e:
        status.update(label="❌ Критическая ошибка (Stage 3)", state="error")
        st.error(str(e))
        return
    
    # Шаг 4: Поиск паттернов
    status.write("🔍 Шаг 4: Поиск паттернов (Mining)...")
    try:
        success4, msg4 = stage4_rules.run_mining(symbol, tf, exchange, profile=profile)
        
        if not success4:
            status.update(label="❌ Ошибка поиска паттернов!", state="error")
            st.error(msg4)
            return
        
        status.write(f"✅ Паттерны найдены: {msg4}")
    except Exception as e:
        status.update(label="❌ Критическая ошибка (Stage 4)", state="error")
        st.error(str(e))
        return
    
    # Шаг 5: STATS bins
    status.write("📈 Шаг 5: STATS квантили...")
    try:
        success5, msg5 = stage5_bins_stats.run_bins_stats(symbol, tf, exchange)
        
        if not success5:
            status.update(label="❌ Ошибка STATS bins!", state="error")
            st.error(msg5)
            return
        
        status.write(f"✅ STATS bins: {msg5}")
    except Exception as e:
        status.update(label="❌ Ошибка Stage 5", state="error")
        st.error(str(e))
        return
    
    # Шаг 6: STATS правила
    status.write("🔬 Шаг 6: STATS правила...")
    try:
        success6, msg6 = stage6_mine_stats.run_mine_stats(symbol, tf, exchange)
        
        if not success6:
            status.update(label="❌ Ошибка STATS правил!", state="error")
            st.error(msg6)
            return
        
        status.write(f"✅ STATS правила: {msg6}")
        status.update(label="🎉 Обучение завершено!", state="complete")
        st.balloons()
        _display_found_rules(symbol, tf, exchange)
    except Exception as e:
        status.update(label="❌ Ошибка Stage 6", state="error")
        st.error(str(e))


def _display_found_rules(symbol, tf, exchange):
    """Отображает найденные паттерны в удобном виде."""
    
    clean_symbol = symbol.replace("/", "").replace(":", "")
    clean_tf = tf.replace("/", "")
    clean_ex = exchange.replace("/", "")
    
    filepath = Path(f"offline/data/{clean_symbol}_{clean_tf}_{clean_ex}_rules.json")
    if not filepath.exists():
        return
    
    with open(filepath, "r") as f:
        data = json.load(f)
    
    rules = data.get("rules", [])
    meta = data.get("meta", {})
    
    if not rules:
        st.info("Паттерны не найдены (edge threshold слишком высокий для малого N)")
        return
    
    st.divider()
    st.subheader("📊 Найденные паттерны")
    
    # Метаданные
    cols = st.columns(5)
    cols[0].metric(
        "N сетапов", 
        meta.get("N_setups", "?"),
        help="Количество исторических сетапов, использованных для обучения"
    )
    cols[1].metric(
        "Найдено правил", 
        meta.get("n_rules", len(rules)),
        help="Количество паттернов, прошедших фильтры"
    )
    cols[2].metric(
        "Min support", 
        meta.get("min_support_abs", "?"),
        help="Минимальное количество сетапов, где должен встретиться паттерн"
    )
    cols[3].metric(
        "Min edge", 
        f"{meta.get('min_edge_threshold', 0):.1%}",
        help="Минимальное преимущество над базовой вероятностью для включения правила"
    )
    cols[4].metric(
        "Base P(UP)", 
        f"{meta.get('base_P_UP', 0.5):.1%}",
        help="Базовая вероятность роста (% сетапов с y_dir=UP)"
    )
    
    # Таблица правил
    for i, rule in enumerate(rules):
        pattern_str = " → ".join(rule.get("pattern", []))
        direction = "🔻 DOWN" if rule.get("edge_down", 0) > rule.get("edge_up", 0) else "🔺 UP"
        edge = max(rule.get("edge_up", 0), rule.get("edge_down", 0))
        
        with st.expander(f"**Правило {i+1}** | {direction} | Edge: {edge:.1%} | Support: {rule.get('support', 0)}"):
            st.caption("🔍 **Паттерн (последовательность токенов):**")
            st.code(pattern_str, language=None)
            
            st.caption("📈 **Вероятности:**")
            col1, col2, col3 = st.columns(3)
            col1.metric("P(UP)", f"{rule.get('p_up_smooth', 0):.1%}", help="Сглаженная вероятность роста")
            col2.metric("P(DOWN)", f"{rule.get('p_down_smooth', 0):.1%}", help="Сглаженная вероятность падения")
            col3.metric("Wins", f"{rule.get('wins_up', 0)}/{rule.get('wins_down', 0)}", help="Побед UP / Побед DOWN")
            
            tti = rule.get("tti_probs", {})
            st.caption("⏱️ **ETA (время до импульса):**")
            eta_cols = st.columns(3)
            eta_cols[0].metric("NEAR", f"{tti.get('NEAR', 0):.0%}", help="Импульс через 0-1 свечу")
            eta_cols[1].metric("MID", f"{tti.get('MID', 0):.0%}", help="Импульс через 2-4 свечи")
            eta_cols[2].metric("EARLY", f"{tti.get('EARLY', 0):.0%}", help="Импульс через 5+ свечей")
