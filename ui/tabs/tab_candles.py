"""
Tab Candles - UI модуль для просмотра и редактирования свечей в БД.

Этот модуль отвечает за вкладку "Свечи" в приложении.
Позволяет просматривать, фильтровать, редактировать и удалять свечи из базы данных.
"""

import streamlit as st
import pandas as pd


def render(db):
    """
    Отрисовывает вкладку "Свечи" (База данных).
    
    Аргументы:
        db: Инстанс DatabaseManager для работы с БД
    """
    
    # === СЕКЦИЯ 1: ФИЛЬТРЫ ===
    # Панель фильтров: TF | Актив | Даты | Лимит записей
    f1, f2, f3, f4 = st.columns([2, 2, 2, 1])
    
    with f1:
        # Мультиселект таймфреймов
        all_tfs = ["1m", "5m", "15m", "1h", "4h", "1d", "1w"]
        selected_tfs = st.multiselect(
            "Таймфреймы", 
            all_tfs, 
            default=[], 
            placeholder="Все TF", 
            label_visibility="collapsed"
        )
    
    with f2:
        # Мультиселект активов (из БД)
        all_symbols = db.get_unique_symbols()
        selected_symbols = st.multiselect(
            "Активы",
            all_symbols,
            default=[],
            placeholder="Все активы",
            label_visibility="collapsed"
        )
        
    with f3:
        # Выбор диапазона дат
        date_range = st.date_input("Период", value=[], label_visibility="collapsed")
        start_d, end_d = None, None
        if len(date_range) == 2:
            start_d, end_d = date_range
        elif len(date_range) == 1:
            start_d = date_range[0]
            
    with f4:
        # Лимит количества записей
        limit_rows = st.number_input(
            "Limit", 
            value=100, 
            min_value=1, 
            step=50, 
            label_visibility="collapsed"
        )

    # === СЕКЦИЯ 2: ЗАГРУЗКА ДАННЫХ ===
    df = db.load_candles(limit=limit_rows, start_date=start_d, end_date=end_d, tfs=selected_tfs, symbols=selected_symbols)

    if not df.empty:
        # Добавляем колонку note если её нет
        if 'note' not in df.columns:
            df['note'] = ""
        
        # Добавляем колонку для удаления
        df.insert(0, "delete", False)
        
        # Конвертируем timestamp
        df['ts'] = pd.to_datetime(df['ts'], errors='coerce')

        # === СЕКЦИЯ 3: ПАНЕЛЬ УПРАВЛЕНИЯ ===
        c1, c2, c3 = st.columns([0.2, 0.2, 0.6], vertical_alignment="bottom")
        
        # Кнопка СОХРАНИТЬ
        with c1:
            if st.button("💾 Сохранить", key="btn_save_top", type="primary"):
                if "db_editor" in st.session_state and "edited_rows" in st.session_state["db_editor"]:
                    changes_map = st.session_state["db_editor"]["edited_rows"]
                    if changes_map:
                        count = 0
                        for idx, changes in changes_map.items():
                            # Исключаем колонку delete из изменений
                            valid_changes = {k: v for k, v in changes.items() if k != 'delete'}
                            if valid_changes:
                                row_id = df.iloc[idx]['id']
                                db.update_candle(row_id, valid_changes)
                                count += 1
                        if count > 0:
                            st.toast(f"✅ Обновлено {count} свечей")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.info("Нет смысловых изменений.")
                    else:
                        st.info("Нет изменений.")
        
        # Кнопка УДАЛИТЬ
        with c2:
            if st.button("🗑 Удалить выделенные", key="btn_del_top", type="secondary"):
                ids_to_del = []
                
                # 1. Проверяем "Выделить все"
                if st.session_state.get("select_all_del_top"):
                    ids_to_del = df['id'].tolist()
                
                # 2. Проверяем индивидуальные чекбоксы
                elif "db_editor" in st.session_state and "edited_rows" in st.session_state["db_editor"]:
                    changes_map = st.session_state["db_editor"]["edited_rows"]
                    for idx, changes in changes_map.items():
                        if changes.get("delete") is True:
                            if idx < len(df):
                                ids_to_del.append(df.iloc[idx]['id'])

                ids_to_del = list(set(ids_to_del))

                if ids_to_del:
                    if db.delete_candles(ids_to_del):
                        st.toast(f"Удалено {len(ids_to_del)} записей!")
                        st.cache_data.clear()
                        st.rerun()
                else:
                    st.warning("Ничего не выделено.")

        # Чекбокс "Выделить все"
        with c3:
            if st.checkbox("Выделить все", key="select_all_del_top"):
                df['delete'] = True

        # === СЕКЦИЯ 4: ТАБЛИЦА ДАННЫХ ===
        visible_cols = ['ts', 'tf', 'x_ray', 'x_ray_composite', 'note', 'raw_data']
        
        edited_df = st.data_editor(
            df,
            key="db_editor",
            column_order=["delete"] + visible_cols,
            use_container_width=True,
            hide_index=True,
            height=800,
            column_config={
                "delete": st.column_config.CheckboxColumn("🗑", default=False, width=30),
                "ts": st.column_config.DatetimeColumn("Time", format="DD.MM.YYYY HH:mm", width="small"),
                "x_ray": st.column_config.TextColumn("X-RAY", width="medium"),
                "x_ray_composite": st.column_config.TextColumn("Composite", width="medium"),
                "note": st.column_config.TextColumn("Note ✏️", width="small"),
                "raw_data": st.column_config.TextColumn("Raw", width="medium"),
            }
        )
        
    else:
        # === СЕКЦИЯ 5: ПУСТАЯ БД ===
        st.markdown(
            """
            <div style="
                background-color: rgba(100, 181, 246, 0.1); 
                color: #64B5F6;
                padding: 8px 16px; 
                border-radius: 8px; 
                width: fit-content;
                border: 1px solid rgba(100, 181, 246, 0.2);
                margin-bottom: 10px;
            ">
                ℹ️ База данных пуста.
            </div>
            """, 
            unsafe_allow_html=True
        )
