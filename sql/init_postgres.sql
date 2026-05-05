CREATE TABLE IF NOT EXISTS raw_orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    city TEXT,
    order_ts TIMESTAMP,
    order_amount NUMERIC,
    order_status TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_payments (
    payment_id TEXT PRIMARY KEY,
    order_id TEXT,
    payment_ts TIMESTAMP,
    payment_method TEXT,
    payment_status TEXT,
    amount NUMERIC,
    failure_reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_deliveries (
    delivery_id TEXT PRIMARY KEY,
    order_id TEXT,
    city TEXT,
    delivery_partner TEXT,
    promised_minutes INT,
    actual_minutes INT,
    delivery_status TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_refunds (
    refund_id TEXT PRIMARY KEY,
    order_id TEXT,
    refund_ts TIMESTAMP,
    refund_amount NUMERIC,
    refund_reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_complaints (
    complaint_id TEXT PRIMARY KEY,
    order_id TEXT,
    complaint_ts TIMESTAMP,
    complaint_text TEXT,
    ai_category TEXT,
    ai_severity TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_logs (
    log_id SERIAL PRIMARY KEY,
    component TEXT,
    status TEXT,
    message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ai_pipeline_diagnosis (
    diagnosis_id SERIAL PRIMARY KEY,
    status TEXT,
    issue_summary TEXT,
    root_cause TEXT,
    suggested_fix TEXT,
    severity TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
