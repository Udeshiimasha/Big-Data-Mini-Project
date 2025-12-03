-- Initialize PostgreSQL schema for Smart City Traffic & Congestion System

-- Create table for windowed aggregates (5-minute windows)
CREATE TABLE IF NOT EXISTS traffic_window_agg (
  id SERIAL PRIMARY KEY,
  sensor_id VARCHAR(50) NOT NULL,
  window_start TIMESTAMP NOT NULL,
  window_end TIMESTAMP NOT NULL,
  sum_vehicle_count INT,
  avg_speed FLOAT,
  congestion_index FLOAT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for faster queries on date ranges
CREATE INDEX IF NOT EXISTS idx_traffic_window_start ON traffic_window_agg(window_start);
CREATE INDEX IF NOT EXISTS idx_traffic_window_sensor ON traffic_window_agg(sensor_id, window_start);

-- Create table for critical traffic alerts
CREATE TABLE IF NOT EXISTS traffic_alerts (
  id SERIAL PRIMARY KEY,
  sensor_id VARCHAR(50) NOT NULL,
  timestamp TIMESTAMP NOT NULL,
  vehicle_count INT,
  avg_speed FLOAT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for alert queries
CREATE INDEX IF NOT EXISTS idx_traffic_alerts_timestamp ON traffic_alerts(timestamp);
CREATE INDEX IF NOT EXISTS idx_traffic_alerts_sensor ON traffic_alerts(sensor_id);

-- Create table for daily peak hour reports
CREATE TABLE IF NOT EXISTS daily_peak (
  id SERIAL PRIMARY KEY,
  sensor_id VARCHAR(50) NOT NULL,
  peak_hour INT NOT NULL CHECK (peak_hour >= 0 AND peak_hour <= 23),
  total_vehicles BIGINT,
  date DATE NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(sensor_id, date)
);

-- Create index for daily peak queries
CREATE INDEX IF NOT EXISTS idx_daily_peak_date ON daily_peak(date);
CREATE INDEX IF NOT EXISTS idx_daily_peak_sensor ON daily_peak(sensor_id, date);

-- Create Airflow database (if not exists)
-- Note: Airflow will create its own tables, but we ensure the database exists
-- This is typically handled by Airflow init, but included for completeness

