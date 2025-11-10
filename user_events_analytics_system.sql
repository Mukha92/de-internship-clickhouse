-- =====================================
--  🗄️  СОЗДАНИЕ ТАБЛИЦЫ СЫРЫХ СОБЫТИЙ
-- =====================================

-- Таблица для хранения сырых событий с TTL 30 дней
CREATE TABLE IF NOT EXISTS user_events (
  user_id UInt32, 
  event_type String, 
  points_spent UInt32, 
  event_time DateTime
) ENGINE = MergeTree() 
ORDER BY 
  (event_time, user_id) TTL event_time + INTERVAL 30 DAY;


-- =====================================
--  📊  СОЗДАНИЕ АГРЕГИРОВАННОЙ ТАБЛИЦЫ
-- =====================================

-- Таблица для агрегированных данных с TTL 180 дней
CREATE TABLE IF NOT EXISTS user_events_aggregated (
  event_date Date, 
  event_type String, 
  unique_users AggregateFunction(uniq, UInt32), 
  total_spent AggregateFunction(sum, UInt32), 
  total_actions AggregateFunction(count, UInt32)
) ENGINE = AggregatingMergeTree() 
ORDER BY 
  (event_date, event_type) TTL event_date + INTERVAL 180 DAY;


-- =====================================
--  🔄  СОЗДАНИЕ MATERIALIZED VIEW
-- =====================================

-- Materialized View для автоматического обновления агрегированной таблицы
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_user_events_aggregation TO user_events_aggregated AS 
SELECT 
  toDate(event_time) AS event_date, 
  event_type, 
  uniqState(user_id) AS unique_users, 
  sumState(points_spent) AS total_spent, 
  countState() AS total_actions 
FROM 
  user_events 
GROUP BY 
  event_date, 
  event_type;


-- =====================================
--  🧪  ВСТАВКА ТЕСТОВЫХ ДАННЫХ
-- =====================================

INSERT INTO user_events 
VALUES 
  (1, 'login', 0, now() - INTERVAL 10 DAY), 
  (2, 'signup', 0, now() - INTERVAL 10 DAY), 
  (3, 'login', 0, now() - INTERVAL 10 DAY), 
  (1, 'login', 0, now() - INTERVAL 7 DAY), 
  (2, 'login', 0, now() - INTERVAL 7 DAY), 
  (3, 'purchase', 30, now() - INTERVAL 7 DAY), 
  (1, 'purchase', 50, now() - INTERVAL 5 DAY), 
  (2, 'logout', 0, now() - INTERVAL 5 DAY), 
  (4, 'login', 0, now() - INTERVAL 5 DAY), 
  (1, 'login', 0, now() - INTERVAL 3 DAY), 
  (3, 'purchase', 70, now() - INTERVAL 3 DAY), 
  (5, 'signup', 0, now() - INTERVAL 3 DAY), 
  (2, 'purchase', 20, now() - INTERVAL 1 DAY), 
  (4, 'logout', 0, now() - INTERVAL 1 DAY), 
  (5, 'login', 0, now() - INTERVAL 1 DAY), 
  (1, 'purchase', 25, now()), 
  (2, 'login', 0, now()), 
  (3, 'logout', 0, now()), 
  (6, 'signup', 0, now()), 
  (6, 'purchase', 100, now());


-- =====================================
--  📈  ЗАПРОС ДЛЯ РАСЧЕТА RETENTION
-- =====================================

-- Retention: сколько пользователей вернулись в течение следующих 7 дней
WITH user_first_seen AS (
  -- Находим первую дату активности для каждого пользователя
  SELECT 
    user_id, 
    min(toDate(event_time)) as first_seen_date 
  FROM 
    user_events 
  GROUP BY 
    user_id
), 
-- Исключаем пользователей, для которых текущий день - день 0
-- (у них еще не было возможности вернуться в течение 7 дней)
valid_cohorts AS (
  SELECT 
    user_id, 
    first_seen_date 
  FROM 
    user_first_seen 
  WHERE 
    first_seen_date < today()
), 
-- Считаем размер каждой когорты (количество пользователей по дням первого визита)
cohort_size AS (
  SELECT 
    first_seen_date, 
    count() as total_users_day_0 
  FROM 
    valid_cohorts 
  GROUP BY 
    first_seen_date
), 
-- Находим пользователей, которые вернулись в течение 7 дней после первого визита
-- Возвращением считается любое событие в период с 1 по 7 день после первого визита
returned_users AS (
  SELECT 
    vc.first_seen_date, 
    countDistinct(vc.user_id) as returned_in_7_days 
  FROM 
    valid_cohorts vc 
    JOIN user_events ue ON vc.user_id = ue.user_id 
  WHERE 
    toDate(ue.event_time) BETWEEN vc.first_seen_date + 1 
    AND vc.first_seen_date + 7 
  GROUP BY 
    vc.first_seen_date
) 
-- Финальный результат: по каждой когорте показываем:
-- cohort_date - дата когорты (день первого визита)
-- total_users_day_0 - общее количество пользователей в когорте
-- returned_in_7_days - количество вернувшихся пользователей
-- retention_7d_percent - процент возврата (retention rate)
SELECT 
  cs.first_seen_date as cohort_date, 
  cs.total_users_day_0, 
  coalesce(ru.returned_in_7_days, 0) as returned_in_7_days, 
  round(
    coalesce(ru.returned_in_7_days, 0) * 100.0 / cs.total_users_day_0, 
    2
  ) as retention_7d_percent 
FROM 
  cohort_size cs 
  LEFT JOIN returned_users ru ON cs.first_seen_date = ru.first_seen_date 
ORDER BY 
  cs.first_seen_date;


-- =====================================
--  📊  ЗАПРОС ДЛЯ БЫСТРОЙ АНАЛИТИКИ
-- =====================================

-- Аналитика по дням с использованием агрегированной таблицы
SELECT 
  event_date, 
  event_type, 
  uniqMerge(unique_users) as unique_users, 
  sumMerge(total_spent) as total_spent, 
  countMerge(total_actions) as total_actions 
FROM 
  user_events_aggregated 
GROUP BY 
  event_date, 
  event_type 
ORDER BY 
  event_date, 
  event_type;


-- =====================================
--  🔍  ПРОВЕРКА СЫРЫХ ДАННЫХ
-- =====================================

-- Дополнительный запрос для проверки вставленных данных
SELECT 
  toDate(event_time) as event_date, 
  event_type, 
  uniq(user_id) as unique_users, 
  sum(points_spent) as total_points,
  count() as total_events 
FROM 
  user_events 
GROUP BY 
  event_date, 
  event_type 
ORDER BY 
  event_date, 
  event_type;