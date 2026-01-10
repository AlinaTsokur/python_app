# Новое ТЗ

План реализации: Система выявления предимпульсных сигналов Futures v2.1 (Clean) — FINAL + Anchors
Описание цели
Создать "чистую" версию (v2.1) системы выявления предимпульсных сигналов. Система анализирует исторические данные свечей для поиска contiguous паттернов (offline) и обнаруживает их в реальном времени (online) для прогнозирования ценовых импульсов. Строгое соблюдение ТЗ v2.1 и всех правок (PATCH).

[PATCH CONTROL — ОБЯЗАТЕЛЬНО]

1) PATCH-лист должен быть зафиксирован как единый артефакт:
   - Файл: PATCHLOG_v2.1.md (или раздел “PATCHLOG” в этом документе).
   - Поля: patch_id, дата, краткое описание, затронутые модули/этапы, статус (approved).

2) Единственный источник истины:
   - Реализация и тесты обязаны ссылаться на конкретную версию PATCHLOG (version + date).
   - Любая логика, отсутствующая в ТЗ v2.1 и/или в PATCHLOG, считается НЕразрешённой.

3) Запрет “неявных патчей”:
   - Разработчик не имеет права менять трактовку PATCH “по памяти/по переписке”.
   - Любое уточнение/изменение = новая запись в PATCHLOG с новым patch_id и пометкой approved.

4) Приоритет документов (если есть конфликт):
   1) ТЗ v2.1 FINAL (Clean)
   2) PATCHLOG_v2.1.md (только approved записи)
   3) Этот план реализации

Требуется проверка пользователем (IMPORTANT)
Код пока не выполняется: этот документ описывает архитектуру и этапы.
Код не будет написан до утверждения плана.

Зависимости: supabase (или мок), pandas, numpy, scikit-learn.

[PATCH] DEPENDENCIES CONSTRAINTS — ОБЯЗАТЕЛЬНО

1) scikit-learn разрешён ТОЛЬКО для калибровки confidence по ТЗ:
   - Platt scaling (логистическая регрессия) при N < 100
   - Isotonic regression при N >= 100
   Любое другое использование ML/моделей/auto-feature engineering запрещено (вне ТЗ).
2) Воспроизводимость:
   - Зафиксировать версии зависимостей (requirements.txt / poetry.lock).
   - Окружение offline и online должно быть совместимо по версиям, чтобы артефакты (bins/rules/calibration) совпадали.
3)  numpy (квантили):
   - Зафиксировать версию numpy (requirements.txt / poetry.lock), т.к. квантильный метод должен быть воспроизводим.
   - Метод квантилей фиксируется как method='linear' (см. PATCH-04 в Этапе 2 и Этапе 4).

[PATCH-05] RANK_BINS_MIN_SAMPLES (approved)
- Дата: 2026-01-10
- Модуль: Stage 3 (3_build_bins.py)
- Описание: Rank-поля (vol_rank, doi_rank, liq_rank) требуют минимум 50 сэмплов для расчета квантилей. Если n_samples < 50, bins[field] = None. Это предотвращает фрагментацию паттернов из-за нестабильных rank-бинов.
- Статус: approved


Строгие ограничения: Никакого эвристического инжиниринга признаков сверх ТЗ.
Аксиома "Из полей": price_sign, cvd_sign, cvd_pct, clv_pct берутся СТРОГО из c.*. Запрещен пересчет из OHLC / buy-sell / чего-либо ещё.
Этап 1: Настройка и Слой Данных
[NEW] offline/1_load_data.py
Цель: Загрузка сегментов из Supabase или локального дампа JSON. Фильтр: WHERE symbol=? AND tf=? AND exchange=? (TF не смешиваем). 


Валидация: NaN в ИСПОЛЬЗУЕМЫХ полях → сетап исключается (детерминированно, по списку ниже).

Критичные поля (NaN → DROP всего сетапа):
1) Все обязательные CORE-поля (каждая свеча CONTEXT.DATA):
- price_sign
- cvd_sign
- cvd_pct
- clv_pct
- oi_set
- oi_unload
- oi_counter
- oi_in_sens

2) Все числовые поля, используемые в STATS (по буферу ≤30):
- cvd_pct
- clv_pct
- liq_long
- liq_short
- upper_tail_pct
- lower_tail_pct
- high
- low

Некритичные поля (NaN допустим, сетап НЕ дропаем):
- Rank-метрики при i < 5 (vol_rank/doi_rank/liq_rank) — это НЕ NaN, а null по определению ТЗ; они не участвуют в биннинге и не должны вызывать дроп.
- Любые поля, не используемые в CORE_STATE и STATS (например open/close/volume/прочие “лишние” поля, если они не участвуют в расчётах по ТЗ).

[ADD] Правило “CORE_STATE: Missing/NULL → drop setup” (жёсткое)
Если в любом сетапе для данного (symbol, tf, exchange) хотя бы в одной свече отсутствует или NULL любое обязательное поле CORE_STATE, то выкидываем целиком этот сетап из обучения (не используем его ни для bins, ни для rules, ни для backtest).

[ADD] Ограничение длины сегмента
Если len(CONTEXT.DATA) > 30 → DROP сегмент (или SKIP) и лог: DROP_SEGMENT_TOO_LONG.

Список обязательных полей CORE_STATE (должны присутствовать в каждой свече CONTEXT.DATA):
- price_sign
- cvd_sign
- cvd_pct
- clv_pct
- oi_set
- oi_unload
- oi_counter
- oi_in_sens

Детерминированное применение:
1) Перед обучением проверяем сетап: проходим все свечи CONTEXT.DATA.
2) Если хотя бы одно из полей выше отсутствует или равно NULL → setup_status="DROP_CORE_MISSING" → исключить сетап.
3) NaN отдельно: если в любом из этих полей NaN → также исключить сетап (как и ранее).


[PATCH] LOAD DATA — ПРИОРИТЕТНОЕ ПРАВИЛО ИСКЛЮЧЕНИЯ ПО NULL/NaN (ЕДИНЫЙ ИСТОЧНИК)

Определения:
- NaN = числовой NaN (float('nan') / numpy.nan) в поле.
- NULL = отсутствует поле ИЛИ значение None/null.

Правила (строго, в приоритете):

1) CORE_STATE обязательные поля: NULL/NaN/отсутствует → DROP весь сетап
Если в любой свече CONTEXT.DATA отсутствует или равен NULL/NaN любой из обязательных CORE_STATE полей:
- price_sign
- cvd_sign
- cvd_pct
- clv_pct
- oi_set
- oi_unload
- oi_counter
- oi_in_sens
то:
setup_status = "DROP_CORE_MISSING"
сетап полностью исключается из обучения (bins/rules/backtest).

2) NaN в любом числовом поле свечи (любой признак, не только CORE) → DROP весь сетап
Если в любой свече CONTEXT.DATA обнаружен NaN в любом числовом поле (включая поля, не входящие в CORE_STATE),
то:
setup_status = "DROP_NAN_PRESENT"
сетап полностью исключается из обучения.

3) NULL в не-CORE полях → НЕ DROP автоматически (без новых правил)
Если поле НЕ входит в список обязательных CORE_STATE полей и оно отсутствует/NULL,
то сетап не дропается автоматически этим правилом.
(Примечание: rank-метрики vol_rank/doi_rank/liq_rank могут быть NULL при i<5 по определению — это допустимо.)

Алгоритм проверки сетапа (1_load_data.py) — детерминированно:
- Проверить все свечи CONTEXT.DATA:
  - если найдено нарушение п.1 → DROP_CORE_MISSING
  - иначе если найден NaN по п.2 → DROP_NAN_PRESENT
  - иначе → USE
Только setup_status="USE" идёт в bins, rules, backtest.

Логирование:
- При DROP писать точную причину (DROP_CORE_MISSING или DROP_NAN_PRESENT) и поле/ts первой найденной ошибки.

Важно:
- NULL допустим для rank-метрик при i < 5 (vol_rank/doi_rank/liq_rank). Такие NULL НЕ должны приводить к исключению сетапа.

“Отсутствие/NULL в STATS-полях (liq/tails/high/low/close/oi_close и т.п.) не является основанием DROP по PATCH-02; обработка таких NULL определяется только в коде расчёта STATS как value=None и далее биннингом bin=null.”

[PATCH] PROJECT STRUCTURE — SOURCE OF TRUTH

1) Структура и назначение файлов полностью соответствуют ТЗ v2.1 (раздел “Структура проекта / файлы”).
   Перечень offline/ и online/ является обязательным и финальным.

2) Запрещено добавлять новые вычислительные слои/скрипты/пайплайны, которые меняют логику ТЗ
   (например: “feature selection”, “extra filters”, “ML classifier”, “heuristic rules”, “additional signals”),
   если это не описано в ТЗ и не добавлено отдельным PATCH-блоком.

3) Любые изменения структуры (новые файлы, переименование, объединение этапов) — только через явный PATCH с причиной и точкой вставки в план.

Этап 2: Инжиниринг Признаков Core и Бины

[NEW] offline/ 2_simulate_states.py
Логика:

Итерация 1..i для каждого сетапа (симуляция онлайна).
Формируем последовательность Seq = [CORE_STATE_1, ..., CORE_STATE_K] для каждого сетапа.

Поля Core (строго):

div_type: из c.price_sign и c.cvd_sign (строго 5 классов из ТЗ).

oi_flags: битовая маска 
(oi_set<<0) | (oi_unload<<1) | (oi_counter<<2) | (oi_in_sens<<3)

cvd_pct: строго c.cvd_pct.
clv_pct: строго c.clv_pct.

Boost (строго как в ТЗ):
vol_*, doi_*, liq_* — расчет rank/share по префиксу 1..i.

Boost метрики (rank через percentile_rank, строго по ТЗ 3.5a):
Для каждого шага i:

vol_rank_i = percentile_rank(volume_i среди V[1..i])
   если i < 5 → vol_rank_i = null

doi_rank_i = percentile_rank(abs(doi_pct_i) среди DOI[1..i])
   если i < 5 → doi_rank_i = null

liq_rank_i = percentile_rank(liq_total_i среди LIQ[1..i]),
   где liq_total_i = liq_long_i + liq_short_i
   если i < 5 → liq_rank_i = null

percentile_rank(x среди X) = count(v < x для v в X) / len(X)
(строгое "<", X включает текущий элемент; см. ТЗ 3.5a)

Rank/Null (строго):
Если i < 5, то vol_rank_i, doi_rank_i, liq_rank_i = null.
null значения не биннятся и не участвуют в CORE_STATE (они boost).

Стабильность признаков (строго):
Признак используется только если он присутствует в 100% свечей обучающей выборки для 
(symbol, tf, exchange)

[PATCH] СТАБИЛЬНОСТЬ ПРИЗНАКОВ — ОБЛАСТЬ ПРИМЕНЕНИЯ
Правило 100% присутствия применяется ТОЛЬКО к:
1) полям CORE_STATE:
   - price_sign, cvd_sign (для div_type)
   - oi_set, oi_unload, oi_counter, oi_in_sens (для oi_flags)
2) числовым полям, которые биннятся в CORE_STATE:
   - cvd_pct
   - clv_pct

Правило 100% НЕ применяется к остальным полям свечи (например open/high/low/close/volume и т.п.).
Цель: не менять “контракт данных свечи”, а контролировать только состав CORE_STATE.

[NEW] offline/ 3_build_bins.py
Логика биннинга (строго):
Собираем значения с каждого шага i каждого сетапа (режим "как будто онлайн").
Считаем квантили q20/q40/q60/q80 → получаем Q1..Q5 по всей совокупности шагов i всех сетапов (CORE + BOOST). 

[PATCH] BINNING: NULL + OUT-OF-RANGE ПОВЕДЕНИЕ (ОБЯЗАТЕЛЬНО)
1) NULL:
- Если значение признака = null → bin НЕ присваиваем (bin=null).
- Такое значение НЕ участвует в построении квантилей и НЕ учитывается при подсчёте распределений.
(Важно: это относится ко всем признакам, не только к rank.)
2) OUT-OF-RANGE (значение вне квантильных порогов):
Пусть пороги: q20, q40, q60, q80.

Назначение бина детерминированно:
- если x <= q20 → Q1
- если q20 < x <= q40 → Q2
- если q40 < x <= q60 → Q3
- если q60 < x <= q80 → Q4
- если x >  q80 → Q5

Примечание: никакого “clamp по -100..+100” не вводим; используем только пороги квантилей.

[PATCH] QUANTILE METHOD — ДЕТЕРМИНИЗМ (PATCH-04, approved)
Квантили считать строго через:
numpy.quantile(data, q=[0.20, 0.40, 0.60, 0.80], method='linear')
Правила:
- method='linear' фиксирован (другие методы запрещены)
- q20/q40/q60/q80 хранить как float64 (не округлять)
- версия numpy фиксируется в requirements/lockfile (см. DEPENDENCIES CONSTRAINTS)

Артефакт: bins.json. (у нас должны в Supabase хранится)
Этап 3: Майнинг Паттернов (DATA)

[NEW] offline/ 4_mine_rules_data.py
Алгоритм: Модифицированный PrefixSpan для contiguous последовательностей (frequent substrings, не subsequences). Ограничения:

max_pattern_length = 15
min_support_abs = max(3, ceil(0.02 * N))

Метрики (строго):

support, wins_up/down: считаются по уникальным сетапам (повторы внутри сетапа support не увеличивают).
Beta-Prior и сглаживание вероятности направления: как в ТЗ (base rate + prior_strength=10).
edge_up/down = P_smooth - base_rate

TTI histogram (взвешенная, защита от доминирования сетапа):
если паттерн P встретился в сетапе M раз, каждый вклад = 1/M.
[ADD] TTI/ETA bucket-probs + smoothing (строго по ТЗ v2.1, п.9.4)
После построения tti_hist[P][tti] (с весами 1/M) обязателен расчёт bucket-вероятностей ETA и их сглаживание:
Суммируем по бакетам:


count_NEAR = Σ tti_hist[P][tti] для tti ∈ {0,1}
count_MID = Σ tti_hist[P][tti] для tti ∈ {2,3,4}
count_EARLY= Σ tti_hist[P][tti] для tti ≥ 5
total = count_NEAR + count_MID + count_EARLY


Сглаживание (фиксировано):


P_NEAR(P) = (count_NEAR + 1) / (total + 3)
P_MID(P) = (count_MID + 1) / (total + 3)
P_EARLY(P) = (count_EARLY + 1) / (total + 3)


Эти значения сохраняются в rules_data.json как tti_probs и используются в онлайне для ETA-агрегации. (у нас должны в Supabase хранится)
Edge Threshold (строго):
min_edge_threshold = max(0.03, 1/sqrt(N))

K_rules (строго):
K_rules = min(50, max(10, floor(N/10)))

Отбор (Data Selection) — жадный алгоритм с Coverage (строго):
Определения:
- setups_with_pattern(P) = множество ID уникальных сетапов, в которых паттерн P встретился ≥ 1 раз (на любом шаге i)
- covered_setups = множество ID сетапов, уже покрытых выбранными правилами
- new_coverage(P) = |setups_with_pattern(P) \ covered_setups|
- coverage(P) = |setups_with_pattern(P)| (это равно support(P), т.к. support считается по уникальным сетапам)

Алгоритм:
1) Сформировать список кандидатов:
   candidate(P) допускается, если:
   support(P) >= min_support_abs И (edge_up(P) >= min_edge_threshold ИЛИ edge_down(P) >= min_edge_threshold)

2) Для каждого кандидата определить направление валидности:
   - UP-правило валидно, если edge_up(P) >= min_edge_threshold
   - DOWN-правило валидно, если edge_down(P) >= min_edge_threshold
   (Если валидны оба, разрешено хранить обе метрики в одном правиле; направление определяется на скоринге через p_up_smooth.)

3) Отсортировать кандидатов по:
   primary: max(edge_up(P), edge_down(P)) DESC
   secondary: support(P) DESC
   tertiary: len(P) DESC (стабильный тай-брейк: при равных edge/support предпочесть более длинный паттерн)

4) Инициализация:
   covered_setups = ∅
   selected_rules = []

5) Жадный проход:
   for P in candidates_sorted:
       if len(selected_rules) >= K_rules:
           break

       # защита: слабые правила не проходят
       if edge_up(P) < min_edge_threshold AND edge_down(P) < min_edge_threshold:
           continue

       # реальное использование coverage:
       new_cov = setups_with_pattern(P) \ covered_setups
       if |new_cov| == 0:
           continue  # SKIP: правило не добавляет нового покрытия

       selected_rules.append(P)
       covered_setups = covered_setups ∪ setups_with_pattern(P)

6) Если после прохода selected_rules пуст (редкий крайний случай при очень малом N):
   разрешено fallback: взять top-1 правило по сортировке из пункта (3), при условии что оно проходит edge-фильтр.
   Иначе вернуть пустой набор правил.

Итог:
- selected_rules содержит ≤ K_rules правил
- каждое выбранное правило гарантированно имеет edge >= min_edge_threshold
- каждое выбранное правило добавило хотя бы один новый сетап к покрытию

[PATCH] PATCH-08-RULES-DATA-CANON (APPROVED) — RULES_DATA CANONICALIZATION (строго)
Каноническая сериализация паттерна:
Каждый CORE_STATE сериализуется в строку: DIV={div_type}|F={oi_flags}|CVD={Qx}|CLV={Qx}
 Пример: DIV=match_up|F=5|CVD=Q3|CLV=Q2
Бины обозначаются строго как Q1..Q5 (не числовые индексы)
Пробелы в строке запрещены


Паттерн = массив строк [state_str_1, ..., state_str_L] в порядке времени (от первой свечи к последней)


Дедупликация (строго):
Ключ дедупликации = pattern (только массив state_str)
Если такой pattern уже встречался → не создавать новый rule, а объединять накопления для этого pattern
Объединение выполняется без нарушения базового правила ТЗ:
 support/wins считаются по уникальным сетапам (повторы внутри одного сетапа support не увеличивают)


Индекс для онлайн-матчинга:
Для каждого паттерна P вычислять last_state_string = pattern[-1]
Строить индекс rules_by_len_last[L][last_state_string] -> [список правил длины L с этим last_state]
Этот индекс используется в онлайне для быстрого отбора кандидатов при матчинге



Артефакт: rules_data.json. (у нас должны в Supabase хранится)

Этап 4: Слой STATS

[PATCH-03 | APPROVED] STATS FEATURES — ОПРЕДЕЛЕНИЕ ФОРМУЛ
patch_id: PATCH-03-STATS-FORMULAS
затронуто: ТЗ 12.2 / План Этап 4 / offline/5_build_bins_stats.py

12.2a STATS FEATURES — ТОЧНЫЕ ФОРМУЛЫ (источник истины)

Окно STATS = текущий online-буфер из последних <= buffer_size_online свечей.
Обозначение: buffer[1..w], где w = текущее число свечей в буфере (1..30).
Все формулы должны работать для любого w >= 1.

Требование “из полей”:
- Использовать только поля свечей (c.*), без пересчёта из OHLC/volume.
- Для OI использовать строго поле c.oi_close.

Формулы STATS (считаются на каждом шаге по текущему buffer):

1) sum_cvd_pct
   sum_cvd_pct = Σ buffer[j].cvd_pct, j=1..w
   Единицы: проценты (суммируются как есть).

2) net_oi_change (процентное изменение OI от первой свечи окна к последней)
   oi_first = buffer[1].oi_close
   oi_last  = buffer[w].oi_close
   если oi_first == 0 → net_oi_change = 0
   иначе net_oi_change = ((oi_last - oi_first) / oi_first) * 100
   Единицы: проценты.

3) sum_liq_long
   sum_liq_long = Σ buffer[j].liq_long, j=1..w

4) sum_liq_short
   sum_liq_short = Σ buffer[j].liq_short, j=1..w
   Единицы: исходные единицы объёма (как в данных).

5) avg_upper_tail_pct
   avg_upper_tail_pct = mean(buffer[j].upper_tail_pct), j=1..w

6) avg_lower_tail_pct
   avg_lower_tail_pct = mean(buffer[j].lower_tail_pct), j=1..w
   Единицы: проценты.

7) body_range_pct (по окну буфера)
   MAX_high = max(buffer[j].high), j=1..w
   MIN_low  = min(buffer[j].low),  j=1..w
   body_start = buffer[1].close
   body_end   = buffer[w].close
   если MAX_high == MIN_low → body_range_pct = 0
   иначе body_range_pct = ABS(body_end - body_start) / (MAX_high - MIN_low) * 100
   Единицы: проценты.

8) liq_dominance_ratio (как в Anchors, повтор для полноты)
   sum_liq_long  = Σ buffer[j].liq_long,  j=1..w
   sum_liq_short = Σ buffer[j].liq_short, j=1..w

   если sum_liq_short > 0:
      liq_dominance_ratio = sum_liq_long / sum_liq_short
   если sum_liq_short == 0 и sum_liq_long > 0:
      liq_dominance_ratio = null   (undefined; не биннится)
   если sum_liq_short == 0 и sum_liq_long == 0:
      liq_dominance_ratio = 1.0

Правило NULL/отсутствующих полей для STATS:
- Если для расчёта конкретного STATS-признака в текущем буфере хотя бы в одной свече отсутствует поле или оно null → значение этого STATS-признака на данном шаге = null (не биннится).
- NaN по-прежнему запрещён: если NaN встречается в используемом поле → исключить весь сетап из обучения (как в общих правилах LOAD DATA).

Правило биннинга STATS:
- null → bin = null; не участвует в квантилях и распределениях.
- Квантили q20/q40/q60/q80 считать как в PATCH-04: numpy.quantile(..., method='linear').

[NEW] offline/ 5_build_bins_stats.py
Логика (строго):
1) На каждом шаге i сформировать текущий буфер последних <=30 свечей (заканчивая свечой i).
2) Посчитать все STATS-признаки строго по формулам из “12.2a STATS FEATURES — ТОЧНЫЕ ФОРМУЛЫ (PATCH-03)”.
3) Собрать все значения по всем шагам i всех сетапов в общий пул (null исключить).
4) Для каждого STATS-признака построить квантили q20/q40/q60/q80 → Q1..Q5 (PATCH-04, method='linear').

Артефакт: bins_stats.json

[PATCH] QUANTILE METHOD — ДЕТЕРМИНИЗМ (PATCH-04, approved)
Квантили считать строго через:
numpy.quantile(data, q=[0.20, 0.40, 0.60, 0.80], method='linear')
Правила:
- method='linear' фиксирован (другие методы запрещены)
- q20/q40/q60/q80 хранить как float64 (не округлять)
- версия numpy фиксируется в requirements/lockfile (см. DEPENDENCIES CONSTRAINTS)

Артефакт: bins_stats.json. (у нас должны в Supabase хранится)

[NEW] offline/ 6_mine_rules_stats.py
Алгоритм: Apriori (макс. 3 условия).

[PATCH] BINNING STATS: NULL + OUT-OF-RANGE (та же логика как bins.json)
Правило для bins_stats.json идентично bins.json (см. PATCH для offline/3):

1) NULL:
   Если значение STATS = null → bin не присваивается (bin=null)
   Такое значение не участвует в построении квантилей

2) OUT-OF-RANGE:
   При назначении бина для каждого значения:
   - если x <= q20 → Q1
   - если q20 < x <= q40 → Q2
   - если q40 < x <= q60 → Q3
   - если q60 < x <= q80 → Q4
   - если x > q80 → Q5

Примечание: никакого clamp или обрезания; экстремальные значения просто валятся в крайний бин (Q1 или Q5).

[ADD] Канонизация STATS-условий (обязательно для детерминизма и дедупликации правил)
Для каждого STATS-правила (конъюнкции 1..3 условий feat==Qx) вводится строгий канонический формат, чтобы одинаковые правила всегда имели одинаковый вид:

1) Упорядочивание условий:
   Перед сохранением/сравнением сортировать список условий по ключу feat в лексикографическом порядке.
2) Запрет дубликатов внутри правила:
   Если в одном правиле один и тот же feat встречается более одного раза — правило считается некорректным и не допускается (максимум одно условие на один feat).
3) Каноническая сериализация:
   Сохранять условия в rules_stats.json только в отсортированном виде:
   conditions: [{"feat":"...", "bin":"Qx"}, ...]
   и использовать этот же порядок при построении ключей/дедупликации/индексации.
4) Дедупликация правил:
   Правила с одинаковым набором (feat, bin) после канонизации считаются одним правилом (объединяются/не создаются повторно) до расчёта итоговых метрик support/wins/p_up_smooth/edge.

 Отбор: те же правила min_support_abs и min_edge_threshold, как и для DATA. 
Артефакт: rules_stats.json. (у нас должны в Supabase хранится)

Этап 5: Онлайн Детектор

[NEW] online/config.json
Конфиг включает (строго):
buffer_size = 30
max_flicker_rate = 0.3
threshold_range
alpha_default = 0.3
min_edge_threshold_rule
objective
пути к artifacts: bins.json, bins_stats.json, rules_data.json, rules_stats.json, calibration.pkl

[NEW] online/signal_detector.py
Класс Detector:
process_candle(candle) -> signal_json
reset() (опционально, для чистого прогона)


Выход JSON (строго):
{
 "direction": "UP" | "DOWN" | "NONE",
  "confidence": 0,
  "eta": "EARLY" | "MID" | "NEAR" | null,
  "matched_data": [...],
  "matched_stats": [...]
}

[ADD] Основной режим запуска: Replay from DB (ts_start)
Вход (из UI):
current_candle (распарсена, может ещё не быть в БД)
ts_start (timestamp начала участка)
symbol/tf/exchange берём из current_candle


DB fetch (контракт):
выбрать из БД все свечи, где:
 symbol == symbol AND tf == tf AND exchange == exchange AND ts >= ts_start
сортировка: ts ASC


Дедуп по ts:
если в fetched есть свеча с ts == current_candle.ts → заменить её на current_candle
иначе → добавить current_candle и пересортировать ASC


Получаем: candles_seq (ASC по ts)
Жёсткий лимит длины (обязателен):
если len(candles_seq) > 30 → вернуть ошибку и СТОП (детектор не запускать)
error_code = "SEGMENT_TOO_LONG"
details: { "length": L, "max": 30 }


Прогон (Replay):
detector.reset()
для каждой свечи c из candles_seq по порядку:
last_signal = detector.process_candle(c)
вернуть last_signal как результат проверки участка

[PATCH] PATCH-01-BUFFER (APPROVED) — BUFFER STRUCTURE (устранение конфликта CORE_STATE vs STATS)

Куда вставить в план:
Этап 5: Онлайн Детектор → раздел “Буфер (обязателен, строго)” / “13.1 Буфер”.
Действие: заменить текущий блок “Буфер (обязателен, строго)” на этот (целиком).
(Если в документе есть “13.1 Буфер” в Anchors — заменить и там, чтобы не было дублей.)


ПРАВКА (строго):


buffer_size = 30
buffer хранит последние ≤30 закрытых свечей (FIFO) в виде минимального контейнера, который включает:
1) CORE_STATE — для матчинга DATA-паттернов
2) RAW-поля — минимальный набор для расчёта STATS по текущему окну


Структура элемента буфера (PATCH-01, уточнено):
core_state:
  - div_type
  - oi_flags
  - cvd_pct_bin
  - clv_bin


raw (минимум для STATS; значения берём строго из входной candle, без пересчёта “из OHLC” для полей core):
  - ts
  - cvd_pct
  - clv_pct
  - oi_set
  - oi_unload
  - oi_counter
  - oi_in_sens
  - oi_close           // ДОБАВЛЕНО: нужно для net_oi_change, если он считается по OI
  - liq_long
  - liq_short
  - upper_tail_pct
  - lower_tail_pct
  - high
  - low
  - close              // ДОБАВЛЕНО: нужно для body_range_pct (start/end)
  
Обновление буфера на каждом process_candle(candle) (строго):
1) построить core_state из полей candle:
   div_type: из candle.price_sign и candle.cvd_sign (5 классов)
   oi_flags: (oi_set<<0) | (oi_unload<<1) | (oi_counter<<2) | (oi_in_sens<<3)
   cvd_pct_bin: бин candle.cvd_pct через bins.json
   clv_bin:     бин candle.clv_pct через bins.json
2) сформировать raw-объект как выше (значения из candle.*)
3) buffer.append({"core_state": core_state, "raw": raw})
4) если len(buffer) > buffer_size → удалить самый старый элемент (FIFO, pop(0))


Назначение:
- core_state используется только для DATA-матчинга (pattern suffix match)
- raw используется только для расчёта STATS по окну buffer (≤30)
- буфер — единый источник данных для online scoring (и в Replay, т.к. Replay прогоняет process_candle по свечам последовательно)

Внутри process_candle(candle) (строго)
update_buffer(candle) (см. “Буфер (обязателен, строго)”)

CORE_STATE_last (по последнему элементу буфера):
div_type из c.price_sign и c.cvd_sign (5 классов)
oi_flags из oi_set/oi_unload/oi_counter/oi_in_sens (битмаска 0..15)
cvd_pct_bin из c.cvd_pct через bins.json
clv_bin из c.clv_pct через bins.json


Матчинг DATA:


сформировать contiguous суффиксы длины L = 1..min(len(buffer), max_pattern_length)
использовать индекс rules_by_len_last[(L, last_state_string)]
точное сравнение pattern[] со суффиксом (детерминированно)


STATS:


посчитать STATS по окну всего буфера (≤30)
забиннить через bins_stats.json
найти совпавшие STATS-правила


Scoring:
Scoring — 0a ПРОСТРАНСТВА И ИМЕНОВАНИЕ (строго, чтобы не было двусмысленности)
Есть 2 разных пространства:
1) Logit-пространство (log-odds):
   logit(p) = ln(p / (1 - p))
   Значения могут быть любыми: (-∞ .. +∞)
2) Вероятность (probability):
   p ∈ [0 .. 1]
   p = sigmoid(logit) = 1 / (1 + exp(-logit))
Правило именования (строго):
- avg_logit_*  — ВСЕГДА logit-пространство
- P_up_*       — ВСЕГДА вероятность [0..1]
- margin       — ВСЕГДА [0..1], margin = abs(2*P_up_final - 1)
Важно:
- "avg_logit_stats = 0" означает НОЛЬ В LOGIT-ПРОСТРАНСТВЕ,
  это эквивалент P_up_stats = sigmoid(0) = 0.5 (нейтрально)


веса: w = 1 + ln(support)
avg_logit_data: по matched DATA, иначе logit(base_P_UP)
avg_logit_stats: по matched STATS, иначе 0 (в logit-пространстве; это P_up_stats=0.5)
avg_logit_final = avg_logit_data + alpha * avg_logit_stats
P_up_final = sigmoid(avg_logit_final)
direction_raw = "UP" если P_up_final > 0.5 иначе "DOWN"


Confidence + threshold:
margin = abs(2*P_up_final - 1)
confidence_raw = calibration(margin) (только calibrator)
confidence = round(100 * confidence_raw) → clamp 0..100
если confidence < threshold → direction="NONE" иначе direction=direction_raw


Flicker:
учитывать только шаги, где confidence >= threshold
если flicker_rate > max_flicker_rate:
confidence = round(confidence * (1 - flicker_rate))
если confidence < threshold → direction="NONE"


ETA:
если есть matched DATA:
агрегировать tti_probs (EARLY/MID/NEAR) с весами w
нормализовать, argmax → eta
иначе eta = null
Этап 6: Бэктест и Оптимизация

[NEW] offline/ 7_backtest_optimize.py

Симуляция (строго, как Replay): для каждого сегмента формируем candles_seq (ASC по ts), жёстко проверяем len(candles_seq) <= 30, затем прогоняем detector.reset(); for candle in candles_seq: last_signal = detector.process_candle(candle). 
Если len(candles_seq) > 30 → SKIP_SEGMENT_TOO_LONG (не участвует в метриках/калибровке/оптимизации). Запрещено truncate/clamp.

Grid Search: alpha_range (0.0-0.5), threshold_range (0.50-0.75).

Калибровка confidence (строго по ТЗ):
Цель (простыми словами):
Калибровка confidence обязана учиться и работать ТОЛЬКО на margin, чтобы не было разночтений.
Определения (строго):
- P_up_final ∈ [0,1] — вероятность направления UP после объединения DATA+STATS.
- margin ∈ [0,1] — сила сигнала (насколько далеко от 0.5):
  margin = abs(2*P_up_final - 1)
Жёсткое правило входа (строго):
- Входной признак для calibrator = ТОЛЬКО margin.
- Запрещено калибровать напрямую:
  - P_up_final
  - avg_logit_final или любые logit-значения
Сбор данных для калибровки (строго):
- В бэктесте собираем пары:
  (margin_pred, accuracy_actual)
где:
  margin_pred = abs(2*P_up_final - 1)
  accuracy_actual ∈ {0,1}
Важно (чтобы не было двусмысленности в поведении):
- Какие именно шаги добавляются в калибровку (все шаги / только где confidence>=threshold / только где direction!=NONE)
  НЕ меняем здесь и НЕ додумываем.
- Это правило определено отдельно в ТЗ/Плане в разделе "Backtest: определение TP/FP/FN и какие сигналы считаем".
- PATCH-12 фиксирует только входной X для calibrator: X = margin.
N для выбора метода калибровки (строго):
- N_calibration = количество собранных пар (margin_pred, accuracy_actual).
Выбор метода (строго):
- Если N_calibration < 100:
  Platt scaling (LogisticRegression)
  Вход: margin (shape Nx1), Выход: confidence_raw ∈ [0,1]
- Если N_calibration >= 100:
  IsotonicRegression
  Вход: margin (shape N), Выход: confidence_raw ∈ [0,1]
Применение (строго):
confidence_raw = calibrator.predict(margin_pred)
confidence = round(100 * confidence_raw)
Сохранение:
calibrator сохраняется в calibration.pkl (scikit-learn сериализация). 
Метрики (строго):
accuracy, precision, recall, F1, FP_rate
avg_lead_time (по TP)
avg_flicker_rate, signal_count
распределение lead_time по EARLY/MID/NEAR

План верификации
Автоматизированные тесты
Unit:
oi_flags корректность битов.
div_type → ровно 5 классов.
TTI 1/M — проверка взвешивания.
[ADD] Unit: TTI bucket smoothing (строго по ТЗ)

Проверить формулу сглаживания для tti_probs:
P_NEAR = (count_NEAR + 1) / (total + 3)
P_MID  = (count_MID  + 1) / (total + 3)
P_EARLY= (count_EARLY+ 1) / (total + 3)

Критерии:
- сумма P_NEAR + P_MID + P_EARLY ≈ 1
- если какой-то bucket имел 0 сырых голосов, после smoothing его вероятность > 0
Coverage skip logic: слабое правило не проходит ниже min_edge_threshold.
Integration:

Прогон пайплайна на малом датасете.
Сверка структуры выходных JSON с эталоном.
Ручная верификация
Artifacts Check:

bins.json и bins_stats.json содержат q20/q40/q60/q80 (Q1..Q5) и построены по всем шагам i.
rules_data.json содержит tti_probs, сумма вероятностей ≈ 1.
online process_candle() возвращает строго заданный JSON.

Anchors (минимальные якоря, чтобы план не “уехал” от ТЗ)
Эти пункты — обязательные фиксаторы реализации, без расширения ТЗ:
div_type: строго 5 классов как в ТЗ (match_up/match_down/div_price_up_delta_down/div_price_down_delta_up/neutral_or_zero).

CORE_STATE использует bins, т.е. ключ паттерна: 
(div_type, oi_flags, cvd_pct_bin, clv_bin), а не raw значения.
liq_dominance_ratio (правило нуля, PATCH):

[ADD] Статус PATCH для liq_dominance_ratio (обязательный)

Правило обработки нулей для liq_dominance_ratio является утверждённым PATCH и обязательно к реализации.
Запрещено заменять это правило на альтернативные константы/INF/999 или любые иные обработчики, не описанные ниже.

если sum_liq_short > 0: sum_liq_long / sum_liq_short
если sum_liq_short == 0 и sum_liq_long > 0: None (undefined)
если sum_liq_short == 0 и sum_liq_long == 0: 1.0
body_range_pct (строго):

ABS(end_price - start_price) / (MAX(high) - MIN(low)) * 100
если MAX(high) == MIN(low) → 0
Threshold gating (строго): confidence < threshold ⇒ direction="NONE" (не сигнал).

Fallbacks (строго):
DATA no matches → avg_logit_data = logit(base_P_UP)
STATS no matches → avg_logit_stats = 0

Calibration граница (как в ТЗ): N < 100 → Platt, N >= 100 → Isotonic.

STATS не источник паттернов: паттерны только из CORE_STATE; STATS — только второй слой через alpha.
support/wins считаются только по уникальным сетапам (повторы внутри сетапа не увеличивают support).
Цель grid-search фиксируется в config:
objective: "F1" | "precision@recall"
min_edge_threshold_rule: "max(0.03, 1/sqrt(N))" 

Важно (жёсткое правило длины сегмента):
 Если len(candles_seq) > 30, то запрещено автоматически “обрезать до 30” (truncate/clamp) и продолжать расчёт.
 Поведение должно быть только одно из двух (фиксируем):
Backtest/Offline: сегмент помечается как SKIP_SEGMENT_TOO_LONG и не участвует в метриках/калибровке/оптимизации.
UI/Replay: вернуть ошибку SEGMENT_TOO_LONG и остановить запуск детектора.


с учетом всех корректировочных вопросов, которые мы предварительно обсудили с этим ии. А вот наш оригинальный план ТЗ который мы составляли в этом чате: ТЗ: Система выявления предимпульсных сигналов (Futures) — v2.1 FINAL (Clean)

0) Аксиомы и что утвердили
0.1 Режим
“Online API детектора = candle-by-candle (process_candle).
В продукте основной запуск = Replay from DB (ts_start): берём свечи из БД + current_candle и прогоняем по одной.”

0.2 Данные
История состоит только из участков до импульса: CONTEXT.DATA.
Сам импульс в JSON не хранится; есть только лейблы на сетап:
y_dir ∈ {UP, DOWN}
y_size ∈ {S, M, L} (опционально в первом релизе)


0.3 Термины 
Сетап = один участок контекста до импульса = весь CONTEXT.DATA (одна строка базы).
Шаг i = симуляция онлайна внутри сетапа: “мы дошли до i-й закрытой свечи”.
State = дискретное состояние свечи (CORE_STATE + бины).
Паттерн P = contiguous последовательность CORE_STATE длины L (без пропусков).
Срабатывание паттерна = факт, что P совпал с суффиксом буфера на шаге i.

0.4 Длина
Длина паттерна не фиксирована.
Для майнинга вводим эвристику max_pattern_length = 15 (ограничение поиска/ресурсов, не “обрезание логики”).

0.5 Volume
Объём в ядре используем строго c.volume (как есть).
buy_volume/sell_volume — active volume → не заменяет volume.

0.6 “Из полей, не пересчитывать” (жёстко)
Для core и диверов:
price_sign берём только из поля c.price_sign
cvd_sign берём только из поля c.cvd_sign
cvd_pct берём только из поля c.cvd_pct
clv_pct берём только из поля c.clv_pct
Запрет: пересчитывать эти поля из OHLC / buy-sell / чего-либо ещё.

0.7 STATS слой (архитектура)

Два типа правил в системе:
1) DATA-правила: паттерны в CORE_STATE (из offline/4_mine_rules_data.py)
   Эти правила ОСНОВНЫЕ, они используются для матчинга в онлайне
   
2) STATS-правила: конъюнкции условий на STATS-признаки (из offline/6_mine_rules_stats.py)
   Эти правила ВТОРОЙ СЛОЙ, они используются только для усиления/контекста

Уточнение: "STATS не источник паттернов" означает:
- CORE_STATE-паттерны НЕ содержат STATS-признаков (это источник CORE-паттернов)
- STATS-правила НЕ используются для независимого выявления сигналов, только как модификатор

Архитектура онлайн-скоринга:
1) Матчим CORE_STATE-паттерны → получаем avg_logit_data
2) Матчим STATS-правила → получаем avg_logit_stats
3) Комбинируем: avg_logit_final = avg_logit_data + alpha * avg_logit_stats
4) Переводим в вероятность и confidence

Вес alpha подбирается grid-search, дефолт alpha_default = 0.3.
1) Цель системы

Offline
Снять “почерк” предимпульсного поведения на symbol/tf/exchange:
найти повторяющиеся contiguous структуры (паттерны) в последовательности CORE_STATE,
оценить их статистически (UP/DOWN + ETA/TTI),
построить набор правил rules_data.json + rules_stats.json,
построить bins bins.json + bins_stats.json,
построить калибровку уверенности.

Online
На закрытии каждой свечи:
сматчить конец текущего буфера с правилами,
выдать:
direction ∈ {UP, DOWN} или NONE,
confidence% (калиброванная),
ETA bucket ∈ {EARLY, MID, NEAR} (если есть),
объяснение: какие правила сработали.
2) Формат данных и хранение
2.1 JSON одного сетапа (история)
{
  "META": { "symbol": "...", "tf": "...", "exchange": "...", "total_candles": 50, "impulse_split_index": 49 },
  "CONTEXT": {
    "STATS": { ... },
    "DATA": [ { candle }, { candle }, ... ]
  }
}
2.1a УТОЧНЕНИЕ: Длина сетапа, индексирование свечей и контроль impulse_split_index
Дано: JSON setup с META.impulse_split_index и CONTEXT.DATA (массив свечей контекста до импульса).
Определение K (длина сетапа):
K = len(CONTEXT.DATA)
K всегда равно количеству свечей в массиве DATA.
Определение шага i (1-indexed):
i ∈ {1, 2, ..., K}
i = порядковый номер текущей свечи в CONTEXT.DATA (по времени, слева направо).
Определение TTI (time-to-impulse):
tti = K - i
K и i определены в п.2.1a
tti показывает, сколько свечей осталось в DATA после текущей позиции i.
Пример:
K = 50
i = 1  → tti = 49
i = 49 → tti = 1
i = 50 → tti = 0
Контракт использования META.impulse_split_index (жёстко):
impulse_split_index — это 0-indexed индекс последней свечи КОНТЕКСТА внутри массива CONTEXT.DATA.
Следовательно, по контракту данных ДОЛЖНО выполняться:
impulse_split_index = K - 1
Если impulse_split_index != K - 1:
это ошибка данных/разметки сегмента (off-by-one или неконсистентный JSON),
и такой сетап должен быть помечен как некорректный и исключён из обучения/бэктеста
(или возвращать ошибку в валидаторе загрузки данных, детерминированно).
Примечание:
Импульсные свечи в CONTEXT.DATA не хранятся.
CONTEXT.DATA содержит только контекст до импульса и заканчивается на последней контекстной свече (индекс K-1, шаг i=K).
Лейблы хранятся отдельно в таблице:
y_dir: UP/DOWN
y_size: S/M/L

2.2 Online вход
Online вход (UI): current_candle + ts_start.
Система делает DB fetch по (symbol, tf, exchange, ts>=ts_start), дедуп по ts, затем прогон.

2.3 Supabase: таблица 
segments
Минимум:
id uuid pk
created_at timestamptz
symbol text
tf text
exchange text
ts_start timestamptz (DATA[0].ts)
ts_end timestamptz (DATA[last].ts)
y_dir text
y_size text
data jsonb (META + CONTEXT)

Правило обучения: не смешиваем TF. Фильтр:
WHERE symbol=? AND tf=? AND exchange=?.
3) Feature Engineering: что считаем
3.1 Правило стабильности признака (детерминированно)
Признак входит в ядро, только если он присутствует в 100% свечей обучающей выборки для данного (symbol, tf, exchange).
Это правило применяется к:
полям для CORE_STATE,
числовым полям, которые биннятся в CORE_STATE.

3.2 div_type (обязательный, жёстко)
Источник:
P = c.price_sign
D = c.cvd_sign
Классы:
match_up если P>0 && D>0
match_down если P<0 && D<0
div_price_up_delta_down если P>0 && D<0
div_price_down_delta_up если P<0 && D>0
neutral_or_zero если P==0 || D==0 (или не попало выше)

3.3 CORE числовые поля (жёстко “из полей”)
cvd_pct = c.cvd_pct
clv_pct = c.clv_pct

3.4 OI flags (обязательный блок)
Битмаска из 4 флагов:
oi_set
oi_unload
oi_counter
oi_in_sens
oi_flags ∈ [0..15] (4 бита).

3.5 Rolling/Boost метрики (не входят в ключ паттерна)
Считаются на шаге i по префиксу 1..i (симуляция онлайна offline):
Volume:
vol_top1_share_i = max(V[1..i]) / sum(V[1..i]) (если sum=0 → 0)
vol_rank_i = percentile_rank(V_i среди V[1..i]) (если i<5 → null)

3.5a ОПРЕДЕЛЕНИЕ percentile_rank (строго, CANON)

Для любого шага i берём префикс значений X = [x_1, x_2, ..., x_i] (включая текущую свечу).
Тогда:

percentile_rank(x_i среди X) = (количество значений строго меньше x_i в X) / len(X)

Формула (точно):
numerator = count(x_j < x_i для j=1..i)
rank = numerator / i

Свойства:
- rank ∈ [0.0 .. 1.0]
- ties: используется строгое "<" (равные НЕ считаются “меньше”)
- если i < 5 → rank = null (не биннится, не участвует в CORE_STATE)


[PATCH] ОПРЕДЕЛЕНИЕ percentile_rank ДЛЯ BOOST-RANK
Определяем percentile_rank(x среди X) детерминированно:

- Берём список X = значения показателя на префиксе [1..i].
- Считаем rank как долю значений строго меньше x:
    rank = (count(v < x) for v in X) / len(X)
- Диапазон rank: [0.0 .. 1.0].
- При равных значениях (ties) используется строгое сравнение "<" (равные НЕ считаются “меньше”).
- Если i < 5 → rank = null (как уже указано в ТЗ).

OI по |doi_pct|:
doi_top1_share_i = max(Doi)/sum(Doi) (sum=0 → 0)
doi_rank_i = percentile_rank(Doi_i среди Doi) (если i<5 → null)

Liquidations:
liq_total = liq_long + liq_short
liq_top1_share_i = max(Liq)/sum(Liq) (sum=0 → 0)
liq_rank_i = percentile_rank(Liq_i среди Liq) (если i<5 → null)

Важно: здесь нет INF/делений на медиану. Мы используем rank/share, чтобы не плодить ad-hoc.

4) Биннинг (Q1..Q5) — как строим bins
4.1 Что бинним

CORE бинним:
cvd_pct
clv_pct

BOOST бинним:
vol_top1_share
vol_rank (если не null)
doi_top1_share
doi_rank (если не null)
liq_top1_share
liq_rank (если не null)

4.2 На каких данных строим квантили (важно)
Квантили считаем по всем шагам i всех сетапов, т.е. в режиме “как будто онлайн”:
для CORE: используем c.cvd_pct, c.clv_pct на каждом шаге i,


для BOOST: используем рассчитанные *_share_i, *_rank_i на каждом шаге i.
То есть bins отражают реальное распределение в прод-режиме, а не “только последняя свеча”.

4.3 Пороги
Для каждого признака строим q20/q40/q60/q80 → получаем Q1..Q5.

4.4 Правила обработки NaN/null
Если значение null (например rank при i<5) — bin не присваиваем, и в CORE_STATE это поле не участвует (оно и так boost).
NaN в данных запрещён: при обнаружении сетап исключается из обучения (детерминированно).
Файл: bins.json.

5) Candle State
5.1 CORE_STATE (ключ паттерна — уменьшенная размерность)
Чтобы не словить комбинаторный взрыв:
CORE_STATE = (div_type, oi_flags, cvd_pct_bin, clv_bin)
div_type: 5
oi_flags: 16
cvd_pct_bin: 5
clv_bin: 5
Итого уникальных CORE_STATE: 5*16*5*5 = 2000.

5.2 BOOST_STATE (для скоринга/фильтра, не в ключе)
vol_top1_share_bin, vol_rank_bin
doi_top1_share_bin, doi_rank_bin
liq_top1_share_bin, liq_rank_bin

6) Contiguous patterns (важно!) и что мы майним
6.1 Требование
Паттерны должны быть contiguous, т.е. без пропусков: это подстрока последовательности CORE_STATE.

6.2 Что является “базой последовательностей”
Одна последовательность для майнинга = один сетап целиком:
Seq = [CORE_STATE_1, CORE_STATE_2, ..., CORE_STATE_K]

7) Майнинг паттернов (DATA слой)
7.1 Алгоритм
Используем PrefixSpan-подобный рост префикса, но в режиме contiguous (по сути: frequent substrings с прунингом по support).
Требование для разработчика: алгоритм должен гарантировать, что паттерны — contiguous.

7.2 Параметры майнинга (adaptive support)
Пусть N = количество сетапов в обучающей базе (для данного tf/symbol/exchange).
min_support_abs = max(3, ceil(0.02 * N))
max_pattern_length = 15
buffer_size_online = 30
8) Support и wins — строго по уникальным сетапам
8.1 Support
support(P) = число уникальных сетапов, где паттерн P встретился хотя бы один раз (на любом шаге i).
Повторы внутри одного сетапа support не увеличивают.

8.2 wins_up / wins_down
Считаются так же по сетапам:
wins_up(P) = количество уникальных сетапов с y_dir=UP, где P встретился ≥1 раз.
wins_down(P) = support(P) - wins_up(P).

9) TTI/ETA: распределение времени до импульса
9.1 Определение tti
Для сетапа длины K и шага i:
tti = K - i (сколько свечей осталось до старта импульса по разметке).

9.2 Bucket
Фиксировано:
NEAR: tti ∈ {0, 1}
MID: tti ∈ {2, 3, 4}
EARLY: tti ≥ 5

9.3 Счёт TTI (каждое срабатывание отдельно) + защита от доминирования одного сетапа
Для каждого паттерна P и каждого сетапа:
пусть P сработал в этом сетапе M раз на шагах i1..iM, каждое срабатывание добавляет вклад 1/M:
tti_hist[P][tti_j] += 1/M
Так: мы учитываем все появления, но один сетап не “переписывает” распределение.

9.4 Bucket-вероятности + сглаживание
Считаем:
count_NEAR = Σ tti_hist[P][tti] по tti в NEAR
count_MID аналогично
count_EARLY аналогично
total = count_NEAR + count_MID + count_EARLY

Сглаживание:
P_NEAR(P) = (count_NEAR + 1) / (total + 3)
P_MID(P)  = (count_MID  + 1) / (total + 3)
P_EARLY(P)= (count_EARLY+ 1) / (total + 3)

10) Вероятность направления: Beta-Prior (исправление малых support)
10.1 Base rate
base_P_UP = (#UP сетапов) / N
base_P_DOWN = 1 - base_P_UP

10.2 Prior
Фиксируем:
prior_strength = 10

alpha = base_P_UP * prior_strength + 1
beta  = (1 - base_P_UP) * prior_strength + 1

10.3 Smoothing
P_UP_smooth(P) = (wins_up(P) + alpha) / (support(P) + alpha + beta)
P_DOWN_smooth(P) = 1 - P_UP_smooth(P)

10.4 Edge
edge_up(P) = P_UP_smooth(P) - base_P_UP
edge_down (P) = P_DOWN_smooth(P) - base_P_DOWN
11) Отбор DATA-правил: coverage + защита от “пустого покрытия”
11.1 Кандидаты
Кандидат допускается, если support >= min_support_abs и (edge_up >= min_edge_threshold или edge_down >= min_edge_threshold)

11.2 Порог по edge (фиксируем)
min_edge_threshold = max(0.03, 1 / sqrt(N))
(детерминированно; не “на глаз”)

Правило UP валидно, если: edge_up(P) >= min_edge_threshold
DOWN валидно, если edge_down(P) >= min_edge_threshold

11.3 Coverage-based selection (жадный)
Сортируем кандидаты по edge (убывание), затем по support (убывание).
Идём по списку, добавляя правила, пока не набрали K_rules.
K_rules = min(50, max(10, floor(N/10)))

11.4 Защита от “плохого покрытия”
При выборе очередного правила:
если edge_r < min_edge_threshold → skip, даже если оно покрывает “остатки”.
(То есть coverage не может протащить слабое правило.)
Файл: rules_data.json.

12) STATS слой (контекст)
12.1 Что такое STATS в проде
STATS считаются по текущему буферу (последние <=buffer_size_online свечей), потому что в онлайне у тебя нет “всего сетапа”.

12.2 Список STATS признаков (фиксировано, максимум 8)

Бинним в Q1..Q5:
sum_cvd_pct
net_oi_change
liq_dominance_ratio
sum_liq_long
sum_liq_short
avg_upper_tail_pct
avg_lower_tail_pct
body_range_pct
Файл: bins_stats.json.

12.3 Формат STATS-правила
Конъюнкция 1..3 условий:
(feat1==Qx) AND (feat2==Qy) [AND (feat3==Qz)]

12.4 Майнинг STATS-правил
Apriori:
Уровень 1: одиночные условия с support>=min_support_abs
Уровень 2..3: расширяем только те, у кого все подмножества проходят support
max_conditions_stats = 3
Статистика и сглаживание:
support/wins по сетапам
Beta-Prior так же, как в DATA
Отбор:
по edge + coverage, с тем же min_edge_threshold.
Файл: rules_stats.json.
13.1 Буфер (строго)

buffer_size_online = 30.

Буфер хранит последние ≤30 закрытых свечей (FIFO). Каждый элемент буфера — минимальный контейнер свечи из двух частей:

1) core_state (для матчинга паттернов rules_data):
   - div_type
   - oi_flags
   - cvd_pct_bin
   - clv_bin

2) raw (минимально необходимые поля для расчёта STATS по окну, см. 12.2):
   - cvd_pct (float, исходное значение из c.cvd_pct)
   - clv_pct (float, исходное значение из c.clv_pct)
   - oi_set, oi_unload, oi_counter, oi_in_sens (флаги из свечи)
   - oi_close (float, исходное значение из c.oi_close)  // нужно для net_oi_change
   - close (float, исходное значение из c.close)        // нужно для body_range_pct (start/end)
   - liq_long, liq_short (float)
   - upper_tail_pct, lower_tail_pct (float)
   - high, low (float)
   - ts (timestamp)

Формат буфера (логическая структура):
[
  {
    "core_state": {div_type, oi_flags, cvd_pct_bin, clv_bin},
    "raw": {cvd_pct, clv_pct, oi_set, oi_unload, oi_counter, oi_in_sens, oi_close, close, liq_long, liq_short, upper_tail_pct, lower_tail_pct, high, low, ts}
  },
  ...
]

Обновление буфера на каждом process_candle(candle):
- построить core_state и raw из входной candle
- buffer.append(item)
- если len(buffer) > 30 → удалить самый старый (pop(0))

13.2 На каждом шаге replay (для каждой свечи из candles_seq) вызываем process_candle
Добавить свечу в буфер (если >30 — удалить старую).
Построить CORE_STATE_last:
div_type из price_sign/cvd_sign
oi_flags из 4 флагов
cvd_pct_bin из c.cvd_pct через bins.json
clv_bin из c.clv_pct через bins.json
Сгенерировать contiguous суффиксы длиной L=1..min(len(buffer), max_pattern_length):
[state_last]
[state_{-2}, state_last]
…
Найти совпавшие DATA-правила.
Посчитать STATS по текущему буферу → забиннить → найти совпавшие STATS-правила.
14) Онлайн-скоринг: агрегация, alpha, confidence, ETA, flicker
14.1 Вес правила
w = 1 + ln(support)

14.2 Avg Logit (нормализованный log-odds) — DATA
Для каждого matched DATA rule r:
p_r = r.P_UP_smooth
logit_r = ln(p_r/(1-p_r))
w_r = 1 + ln(r.support)
avg_logit_data = (Σ w_r*logit_r) / (Σ w_r)
Если матчей нет → avg_logit_data = logit(base_P_UP) (детерминированный fallback).
P_up_data = sigmoid(avg_logit_data)

14.3 Avg Logit — STATS
Аналогично получаем avg_logit_stats и P_up_stats.
Если нет матчей → avg_logit_stats = 0 (нейтрально).

14.4 Комбинация DATA + STATS
avg_logit_final = avg_logit_data + alpha * avg_logit_stats
P_up_final = sigmoid(avg_logit_final)
direction = UP если P_up_final > 0.5 иначе DOWN

14.5 Threshold поведение в проде
Считаем margin = |2*P_up_final - 1| (0..1)
Далее confidence (см. 14.6)
Если confidence < threshold → выдаём NONE (не сигнал), но логируем как weak-match.

14.6 Calibration (адаптивно по размеру выборки)
Во время backtest собираем пары (margin_pred, accuracy_actual).
Если N < 100 → Platt scaling (логистическая регрессия) на margin
Если N >= 100 → Isotonic regression на margin
confidence = calibrated(margin)
confidence% = round(100*confidence)

14.7 Flicker: явное использование (не просто лог)
Онлайн ведём историю сигналов с confidence >= threshold:
flicker_count: сколько раз сменилось направление
flicker_rate = flicker_count / K (K = число шагов в текущем буфере, min 1)
Если flicker_rate > max_flicker_rate:
confidence *= (1 - flicker_rate) (понижаем уверенность детерминированно)
если после понижения confidence < threshold → выдаём NONE
max_flicker_rate = 0.3 (фиксируем в config).

14.8 ETA (TTI) в онлайне
Для каждого matched DATA rule r есть распределение P_NEAR, P_MID, P_EARLY.
Агрегация:
для каждого r: вес w_r = 1 + ln(support_r)
суммируем взвешенно:
S_NEAR += w_r * P_NEAR(r)
S_MID  += w_r * P_MID(r)
S_EARLY+= w_r * P_EARLY(r)
нормализуем:
ETA_probs = normalize([S_EARLY,S_MID,S_NEAR])
ETA_bucket = argmax(ETA_probs)
Если DATA матчей нет → ETA = null.

15) Backtest + метрики + подбор alpha/threshold
15.1 Симуляция “как в реале”
Для каждого сетапа:
прогоняем свечи 1..K по одной,
на каждом шаге вызываем online detector (тот же код),
сохраняем сигналы и confidence.

15.2 Определение TP/FP/FN (исправлено)
Берём последний сигнал перед концом контекста с confidence >= threshold:
если нет такого сигнала → FN
если есть и direction == y_dir → TP
если есть и direction != y_dir → FP
Также считаем:
final_tti = K - i_final (lead time)
flicker_rate (на сетап)
signal_count (сколько раз было confidence>=threshold)

15.3 Метрики качества
Считаем по всем сетапам:
accuracy, precision, recall, F1
FP_rate
avg_lead_time (по TP)
распределение lead_time по бакетам EARLY/MID/NEAR
avg_flicker_rate

15.4 Подбор alpha и threshold (grid search)
Параметры:
threshold_range = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75]
alpha_range = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5]
Цель:
либо max F1
либо max precision при recall >= R_min (если важнее “реже, но метче”)

15.4.1 Алгоритм выбора alpha (детально)
Grid search по alpha_range
Считаем метрику качества для каждого alpha
alpha_optimal = argmax(metric(alpha))
Если несколько alpha дают одинаковую метрику → выбираем меньший alpha (консервативнее)
Edge case: если N < 10 → alpha = 0.0 (только DATA)
Итог сохраняем в config.json.

16) Формат правил и индекс для быстрого матчинга
16.1 Формат хранения паттерна
Каждый CORE_STATE сериализуется в строку:
DIV=<div_type>|F=<oi_flags>|CVD=<Q>|CLV=<Q>
Паттерн = массив таких строк.

16.2 Индекс для онлайн
Чтобы онлайн не искал “в лоб” по всем правилам:
строим словарь rules_by_len_last:
ключ: (L, last_state_string)
значение: список правил длины L с таким last_state

Матчинг:
для каждого L берём суффикс длины L,
смотрим bucket правил по (L, last_state) и сравниваем массивы.
Это детерминированно и быстро.

17) Структура проекта (файлы)
offline/
1_load_data.py — загрузка сегментов из Supabase (фильтры tf/symbol/exchange)
2_simulate_states.py — симуляция онлайна по шагам i, построение CORE_STATE последовательностей
3_build_bins.py — bins.json (CORE + BOOST)
4_mine_rules_data.py — майнинг contiguous паттернов, support, wins, Beta-prior, TTI histogram → rules_data.json
5_build_bins_stats.py — bins_stats.json
6_mine_rules_stats.py — Apriori правила → rules_stats.json
7_backtest_optimize.py — симуляция, grid-search alpha/threshold, обучение calibration (Platt/Isotonic) → calibration.pkl + config.json

online/
signal_detector.py — класс Detector: buffer build_core_state match rules avg_logit scoring ETA aggregation flicker penalty threshold gating calibrated confidence
config.json — единый источник истины

18) CONFIG (единый источник истины)
{
  "buffer_size": 30,
  "core_state_fields": ["div_type", "oi_flags", "cvd_pct_bin", "clv_bin"],
  "min_support_abs_rule": "max(3, ceil(0.02*N))",
  "max_pattern_length": 15,
  "tti_buckets": {
    "NEAR": [0, 1],
    "MID": [2, 3, 4],
    "EARLY": [5, 1000000]
  },

  "prior_strength": 10,
  "stats_features": [
    "sum_cvd_pct",
    "net_oi_change",
    "liq_dominance_ratio",
    "sum_liq_long",
    "sum_liq_short",
    "avg_upper_tail_pct",
    "avg_lower_tail_pct",
    "body_range_pct"
  ],
  "max_conditions_stats": 3,
  "weight_fn": "1 + ln(support)",
  "aggregate": "avg_logit",
  "max_flicker_rate": 0.3,
  "threshold_range": [0.50, 0.55, 0.60, 0.65, 0.70, 0.75],
  "alpha_range": [0.0, 0.1, 0.2, 0.3, 0.4, 0.5],
  "alpha_default": 0.3
}

Что стоит добавить (минимально, 1–2 страницы)
A) JSON-схемы файлов (точно)
bins.json / bins_stats.json — точный формат:

имя признака → q20/q40/q60/q80

что делать, если значение вне диапазона

rules_data.json — точный формат rule:
pattern: [state_str...]
support
wins_up
p_up_smooth
edge_up
tti_probs: {EARLY, MID, NEAR}
last_state (для индекса)
len

rules_stats.json — формат:
conditions: [{feat, bin}]
support/wins/p_up_smooth/edge

config.json — уже есть, но лучше добавить:
min_edge_threshold_rule: "max(0.03, 1/sqrt(N))"
objective: "F1" | "precision@recall"

B) “Definition of Done” (приёмка)
5–7 пунктов, чтобы принять работу:
offline пайплайн даёт 5 артефактов: bins.json, bins_stats.json, rules_data.json, rules_stats.json, calibration.pkl, config.json (у нас должны в Supabase хранится)
online process_candle() возвращает строго заданный JSON
backtest печатает метрики: accuracy/precision/recall/F1, FP_rate, avg_lead_time, avg_flicker

C) 1 пример входа/выхода online
Один пример свечи + пример ответа детектора:
{ "direction":"UP", "confidence":72, "eta":"MID", "matched_data":[...], "matched_stats":[...] 

---

## [PATCH] ARTIFACT_KEY NAMING CONVENTION (approved)

### Формат ключа артефакта

Все артефакты в таблице `training_artifacts` используют ключ формата:
```
{type}_{symbol}_{tf}_{exchange}
```

**Примеры:**
- `bins_ETH_1D_Binance`
- `rules_ETH_4h_Binance`
- `stats_BTC_1D_Binance`

### Правила

1. Ключ формируется **автоматически кодом**, не хардкодится в config.json
2. Каждая комбинация (symbol, tf, exchange) = отдельный артефакт

### Версионирование

Primary Key таблицы: `(artifact_key, version)`

Это позволяет хранить **историю версий** одного артефакта:
- `bins_ETH_1D_Binance` + `1.0.0`
- `bins_ETH_1D_Binance` + `1.0.1`

При загрузке артефакта для online-детектора (берём последнюю версию):
```sql
SELECT * FROM training_artifacts 
WHERE artifact_key = 'bins_ETH_1D_Binance' 
ORDER BY version DESC LIMIT 1
```
