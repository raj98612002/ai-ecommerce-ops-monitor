WITH deliveries AS (
    SELECT * FROM {{ ref("stg_deliveries") }}
),
city_daily AS (
    SELECT
        delivery_date,
        city,
        delivery_partner,
        COUNT(*)                                            AS total_deliveries,
        SUM(CASE WHEN is_delayed = FALSE THEN 1 ELSE 0 END) AS on_time_deliveries,
        SUM(CASE WHEN is_delayed = TRUE  THEN 1 ELSE 0 END) AS delayed_deliveries,
        ROUND(SUM(CASE WHEN is_delayed = FALSE THEN 1 ELSE 0 END)::FLOAT
              / NULLIF(COUNT(*), 0), 4)                     AS on_time_rate,
        ROUND(SUM(CASE WHEN is_delayed = TRUE THEN 1 ELSE 0 END)::FLOAT
              / NULLIF(COUNT(*), 0), 4)                     AS delay_rate,
        ROUND(AVG(promised_minutes), 1)                     AS avg_promised_minutes,
        ROUND(AVG(actual_minutes), 1)                       AS avg_actual_minutes,
        ROUND(AVG(delay_minutes), 1)                        AS avg_delay_minutes,
        MAX(delay_minutes)                                  AS max_delay_minutes,
        CASE
            WHEN SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END)::FLOAT
               / NULLIF(COUNT(*), 0) >= 0.40 THEN 'critical'
            WHEN SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END)::FLOAT
               / NULLIF(COUNT(*), 0) >= 0.25 THEN 'warning'
            ELSE 'healthy'
        END                                                 AS health_status,
        CURRENT_TIMESTAMP()                                 AS dbt_updated_at
    FROM deliveries
    GROUP BY delivery_date, city, delivery_partner
)
SELECT * FROM city_daily
ORDER BY delivery_date DESC, delay_rate DESC
