CREATE TABLE IF NOT EXISTS spark_metrics (
    id SERIAL PRIMARY KEY,
    region TEXT,
    total_cas BIGINT,
    total_deces BIGINT,
    avg_temp FLOAT,
    avg_rain FLOAT,
    avg_humidity FLOAT,
    avg_risk FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS spark_predictions (
    id SERIAL PRIMARY KEY,
    region TEXT,
    prediction FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS spark_alerts (
    id SERIAL PRIMARY KEY,
    region TEXT,
    severity TEXT,
    risk_score FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_spark_metrics_region
ON spark_metrics(region);

CREATE INDEX idx_spark_alerts_severity
ON spark_alerts(severity);