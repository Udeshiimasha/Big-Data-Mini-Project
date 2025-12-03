# airflow/dags/nightly_peak_hour.py
"""
Airflow DAG: run nightly at 02:00 local time to compute Peak Traffic Hour per junction.

Assumes window aggregates or raw events are in Postgres table `traffic_window_agg`.

Produces CSV report: /opt/airflow/reports/peak_traffic_YYYYMMDD.csv
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import psycopg2
import csv
import os

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

REPORT_DIR = "/opt/airflow/reports"

def compute_peak_hour(**context):
    """
    Compute peak hour for the previous day based on window aggregates.
    """
    execution_date = context.get('execution_date') or datetime.utcnow()
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="postgres",
        dbname="trafficdb",
        user="postgres",
        password="postgres",
        port=5432
    )
    cur = conn.cursor()
    
    # Select aggregates for previous day
    prev_day = (execution_date - timedelta(days=1)).date() if isinstance(execution_date, datetime) else (datetime.utcnow().date() - timedelta(days=1))
    start_ts = datetime.combine(prev_day, datetime.min.time())
    end_ts = datetime.combine(prev_day, datetime.max.time())

    query = """
    SELECT sensor_id, window_start, sum_vehicle_count
    FROM traffic_window_agg
    WHERE window_start >= %s AND window_start <= %s
    ORDER BY sensor_id, window_start;
    """
    cur.execute(query, (start_ts, end_ts))
    rows = cur.fetchall()
    
    if not rows:
        print(f"No data found for {prev_day}. Skipping report generation.")
        cur.close()
        conn.close()
        return
    
    # Aggregate per sensor per hour
    sensor_hour = {}
    for sensor_id, window_start, sum_vehicle_count in rows:
        if window_start:
            hour = window_start.hour
            key = (sensor_id, hour)
            sensor_hour[key] = sensor_hour.get(key, 0) + (sum_vehicle_count or 0)

    # Compute peak hour per sensor
    peak_results = []
    sensors = set(k[0] for k in sensor_hour.keys())
    
    if not sensors:
        print(f"No sensor data found for {prev_day}.")
        cur.close()
        conn.close()
        return
    
    for sensor in sensors:
        best_hour = max(
            ((hour, sensor_hour.get((sensor, hour), 0)) for hour in range(0, 24)),
            key=lambda x: x[1]
        )
        peak_results.append((sensor, best_hour[0], best_hour[1]))

    # Write CSV
    os.makedirs(REPORT_DIR, exist_ok=True)
    out_file = os.path.join(REPORT_DIR, f"peak_traffic_{prev_day.strftime('%Y%m%d')}.csv")
    
    with open(out_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["sensor_id", "peak_hour", "vehicle_count"])
        for row in peak_results:
            writer.writerow(row)
    
    print(f"Report written to {out_file}")
    print(f"Peak hours computed for {len(peak_results)} sensors")

    # Optionally, insert into 'daily_peak' table for reference
    for sensor, hour, count in peak_results:
        cur.execute("""
            INSERT INTO daily_peak (sensor_id, peak_hour, total_vehicles, date)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (sensor_id, date) DO UPDATE SET
                peak_hour = EXCLUDED.peak_hour,
                total_vehicles = EXCLUDED.total_vehicles;
        """, (sensor, hour, count, prev_day))
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"Daily peak data inserted/updated for {prev_day}")

with DAG(
    dag_id="nightly_peak_hour",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",  # daily at 02:00 UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    description="Nightly job to compute peak traffic hour per junction",
    tags=["traffic", "analytics", "nightly"],
) as dag:
    
    task_compute = PythonOperator(
        task_id="compute_peak_hour",
        python_callable=compute_peak_hour,
        provide_context=True,
    )

    task_compute

