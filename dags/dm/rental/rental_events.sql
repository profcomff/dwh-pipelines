-- Генерация агрегатов по доступности и общему количеству вещей
WITH item_aggregates AS (
    SELECT 
        i.type_id,
        COUNT(*) FILTER (WHERE i.is_available) AS available_items,
        COUNT(*) AS total_items
    FROM DWH_RENTAL.item i
    WHERE i.valid_to_dt IS NULL
    GROUP BY i.type_id
),
-- Подсчет количества аренд по типам вещей
rental_counts AS (
    SELECT 
        i.type_id,
        COUNT(rs.api_id) AS rental_count
    FROM DWH_RENTAL.rental_session rs
    JOIN DWH_RENTAL.item i ON rs.item_id = i.api_id AND i.valid_to_dt IS NULL
    WHERE rs.valid_to_dt IS NULL
    GROUP BY i.type_id
),
-- Выбор последнего страйка для каждой сессии
strike_data AS (
    SELECT 
        s.*,
        ROW_NUMBER() OVER (PARTITION BY s.session_id ORDER BY s.create_ts DESC) AS rn
    FROM DWH_RENTAL.strike s
    WHERE s.valid_to_dt IS NULL
),
-- Подсчет количества страйков по пользователям
strike_counts AS (
    SELECT 
        s.user_id,
        COUNT(*) AS strike_count
    FROM DWH_RENTAL.strike s
    WHERE s.valid_to_dt IS NULL
    GROUP BY s.user_id
),
-- Определение часа пиковой активности
activity AS (
    SELECT 
        EXTRACT(HOUR FROM e.create_ts) AS activity_max_time,
        COUNT(*) AS activity_max
    FROM DWH_RENTAL.event e
    GROUP BY EXTRACT(HOUR FROM e.create_ts)
    ORDER BY COUNT(*) DESC
    LIMIT 1
),
-- Расчет среднего времени аренды
avg_rent AS (
    SELECT 
        i.type_id,
        AVG(EXTRACT(EPOCH FROM (rs.end_ts - rs.start_ts)) / 3600 AS avg_rent_hours
    FROM DWH_RENTAL.rental_session rs
    JOIN DWH_RENTAL.item i ON rs.item_id = i.api_id AND i.valid_to_dt IS NULL
    WHERE rs.valid_to_dt IS NULL
    GROUP BY i.type_id
),
-- Расчет среднего времени простоя
downtime_calc AS (
    SELECT 
        rs.api_id,
        i.type_id,
        rs.start_ts - LAG(rs.end_ts) OVER (PARTITION BY rs.item_id ORDER BY rs.start_ts) AS downtime
    FROM DWH_RENTAL.rental_session rs
    JOIN DWH_RENTAL.item i ON rs.item_id = i.api_id AND i.valid_to_dt IS NULL
    WHERE rs.valid_to_dt IS NULL
),
avg_downtime AS (
    SELECT 
        type_id,
        AVG(EXTRACT(EPOCH FROM downtime)) / 3600 AS avg_downtime_hours
    FROM downtime_calc
    WHERE downtime IS NOT NULL AND downtime > INTERVAL '0'
    GROUP BY type_id
)
-- Сборка финального результата
INSERT INTO DM_RENTAL.rental_events (
    uuid, user_id, item_id, admin_open_session_id, admin_close_session_id,
    reservation_ts, start_ts, end_ts, actual_return_ts, status, duration, delay,
    overdue_flag, conversion_flag, type_id, type, name, image_url, description,
    available_items, total_items, rental_count, avg_downtime_hours, avg_rent_hours,
    session_id, admin_strike_id, strike_reason, strike_date, strike_count,
    activity_max_time, activity_max
)
SELECT 
    rs.uuid,
    rs.user_id,
    rs.item_id,
    rs.admin_open_id AS admin_open_session_id,
    rs.admin_close_id AS admin_close_session_id,
    rs.reservation_ts,
    rs.start_ts,
    rs.end_ts,
    rs.actual_return_ts,
    rs.status,
    (rs.end_ts - rs.start_ts) AS duration,
    CASE 
        WHEN rs.actual_return_ts > rs.end_ts THEN rs.actual_return_ts - rs.end_ts
        ELSE NULL 
    END AS delay,
    (rs.actual_return_ts > rs.end_ts) AS overdue_flag,
    EXISTS (
        SELECT 1 
        FROM DWH_RENTAL.event e 
        WHERE e.session_id = rs.api_id 
          AND e.action_type = 'CONVERSION'
          AND e.valid_to_dt IS NULL
    ) AS conversion_flag,
    i.type_id,
    it.name AS type,
    it.name AS name, -- Уточнить источник названия вещи при необходимости
    it.image_url,
    it.description,
    ia.available_items,
    ia.total_items,
    rc.rental_count,
    INTERVAL '1 HOUR' * ad.avg_downtime_hours AS avg_downtime_hours,
    INTERVAL '1 HOUR' * ar.avg_rent_hours AS avg_rent_hours,
    rs.api_id AS session_id,
    sd.admin_id AS admin_strike_id,
    sd.reason AS strike_reason,
    sd.create_ts AS strike_date,
    COALESCE(sc.strike_count, 0) AS strike_count,
    a.activity_max_time,
    a.activity_max
FROM DWH_RENTAL.rental_session rs
JOIN DWH_RENTAL.item i 
    ON rs.item_id = i.api_id AND i.valid_to_dt IS NULL
JOIN DWH_RENTAL.item_type it 
    ON i.type_id = it.api_id AND it.valid_to_dt IS NULL
LEFT JOIN strike_data sd 
    ON rs.api_id = sd.session_id AND sd.rn = 1
LEFT JOIN strike_counts sc 
    ON rs.user_id = sc.user_id
JOIN item_aggregates ia 
    ON i.type_id = ia.type_id
JOIN rental_counts rc 
    ON i.type_id = rc.type_id
JOIN avg_downtime ad 
    ON i.type_id = ad.type_id
JOIN avg_rent ar 
    ON i.type_id = ar.type_id
CROSS JOIN activity a
WHERE rs.valid_to_dt IS NULL;