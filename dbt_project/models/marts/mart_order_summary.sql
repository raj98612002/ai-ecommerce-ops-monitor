WITH orders AS (
    SELECT * FROM {{ ref("stg_orders") }}
),
daily_city AS (
    SELECT
        order_date,
        city,
        COUNT(*)                                            AS total_orders,
        COUNT(DISTINCT customer_id)                         AS unique_customers,
        SUM(CASE WHEN is_cancelled = FALSE THEN 1 ELSE 0 END) AS active_orders,
        SUM(CASE WHEN is_cancelled = TRUE  THEN 1 ELSE 0 END) AS cancelled_orders,
        SUM(order_amount)                                   AS total_revenue,
        ROUND(AVG(order_amount), 2)                         AS avg_order_value,
        ROUND(SUM(CASE WHEN is_cancelled THEN 1 ELSE 0 END)::FLOAT
              / NULLIF(COUNT(*), 0), 4)                     AS cancellation_rate,
        CURRENT_TIMESTAMP()                                 AS dbt_updated_at
    FROM orders
    GROUP BY order_date, city
)
SELECT * FROM daily_city
ORDER BY order_date DESC, total_revenue DESC
