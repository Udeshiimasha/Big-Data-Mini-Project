# Quick Start Guide - Smart City Traffic Pipeline

## Prerequisites Check

Before starting, ensure you have:
- ✅ Docker Desktop installed and running
- ✅ Python 3.8+ installed
- ✅ Apache Spark 3.3.0+ installed (or use Docker)
- ✅ Java 8+ installed (for Spark)

## Step-by-Step Execution

### Step 1: Start All Infrastructure Services

Open a terminal in the project root directory and run:

```bash
docker-compose up -d
```

**Wait 2-3 minutes** for all services to start. Check status:

```bash
docker-compose ps
```

All services should show "Up" status. If any service fails, check logs:

```bash
docker-compose logs [service-name]
```

### Step 2: Create Kafka Topics

Open a new terminal and create the required Kafka topics:

```bash
# Create traffic-events topic
docker exec kafka kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic traffic-events \
  --partitions 3 \
  --replication-factor 1

# Create critical-traffic topic (for alerts)
docker exec kafka kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic critical-traffic \
  --partitions 1 \
  --replication-factor 1

# Verify topics were created
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092
```

You should see both `traffic-events` and `critical-traffic` in the list.

### Step 3: Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Start the Sensor Producer

In a new terminal, start generating traffic data:

```bash
python producers/sensor_producer.py
```

You should see output like:
```
Starting sensor producers to topic: traffic-events
Sensors: JUNCTION_A, JUNCTION_B, JUNCTION_C, JUNCTION_D
[12:34:56] JUNCTION_A: vehicles=15, speed=45.2 km/h
[12:34:56] JUNCTION_B: vehicles=8, speed=52.1 km/h
...
```

**Keep this running** - it will continuously generate data. You'll occasionally see `[CRITICAL]` messages when traffic congestion is simulated.

### Step 5: Start Spark Streaming Job

Open **another new terminal** and run the Spark streaming job:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.5.0 \
  spark/streaming_traffic.py
```

**Note:** Adjust the Spark version if you have a different version:
- For Spark 3.4.0: `spark-sql-kafka-0-10_2.12:3.4.0`
- For Spark 3.5.0: `spark-sql-kafka-0-10_2.12:3.5.0`

You should see:
```
Spark Streaming started. Waiting for data...
Alerts query started
Aggregates query started
```

The Spark job will:
- Read messages from Kafka
- Process them in 5-minute windows
- Write aggregates to PostgreSQL
- Write alerts when speed < 10 km/h

**Keep this running** - it processes data in real-time.

### Step 6: Access Airflow Web UI

Open your browser and go to:

```
http://localhost:8080
```

Login credentials:
- **Username:** `admin`
- **Password:** `admin`

You should see the `nightly_peak_hour` DAG. You can:
- **Trigger it manually** by clicking the play button (▶️)
- **Wait for scheduled run** at 02:00 UTC daily

### Step 7: Verify Data Flow

#### Check Kafka Messages

In a new terminal:

```bash
# View messages in traffic-events topic
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic traffic-events \
  --from-beginning \
  --max-messages 10

# View alerts in critical-traffic topic
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic critical-traffic \
  --from-beginning \
  --max-messages 5
```

#### Check PostgreSQL Data

```bash
# Connect to database
docker exec -it postgres psql -U postgres -d trafficdb

# Check window aggregates (should have data after 5+ minutes)
SELECT * FROM traffic_window_agg ORDER BY window_start DESC LIMIT 10;

# Check alerts (should have data when critical events occur)
SELECT * FROM traffic_alerts ORDER BY timestamp DESC LIMIT 10;

# Exit PostgreSQL
\q
```

### Step 8: Generate Nightly Report

#### Option A: Trigger Manually via Airflow UI

1. Go to http://localhost:8080
2. Find `nightly_peak_hour` DAG
3. Click the play button (▶️) to trigger it
4. Click on the DAG → Graph View to see execution
5. Wait for task to complete (green checkmark)

#### Option B: Wait for Scheduled Run

The DAG runs automatically at 02:00 UTC daily.

#### Check Generated Report

After the DAG completes, check for CSV file:

```bash
# List reports
ls -la airflow/reports/

# View report content
cat airflow/reports/peak_traffic_YYYYMMDD.csv
```

Replace `YYYYMMDD` with the actual date.

### Step 9: Generate Visualization

After a report is generated, create visualizations:

```bash
python visualization/plot_traffic_report.py airflow/reports/peak_traffic_YYYYMMDD.csv
```

This will generate:
- `peak_traffic_YYYYMMDD_visualization.png` - Bar charts per junction
- `peak_traffic_YYYYMMDD_comparison.png` - Line chart comparison

## Expected Timeline

- **0-2 minutes**: Infrastructure starts
- **2-5 minutes**: Producer generates data, Spark processes it
- **5+ minutes**: First window aggregates appear in PostgreSQL
- **When critical events occur**: Alerts appear in `traffic_alerts` table
- **After DAG runs**: CSV report appears in `airflow/reports/`

## Troubleshooting

### Kafka Connection Errors

**Problem:** Producer can't connect to Kafka

**Solution:**
```bash
# Check if Kafka is running
docker-compose ps kafka

# Check Kafka logs
docker-compose logs kafka

# Verify Kafka is accessible
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Spark Job Fails

**Problem:** Spark can't connect to Kafka or PostgreSQL

**Solution:**
- Verify Kafka is running: `docker-compose ps`
- Check Spark package versions match your Spark installation
- Verify PostgreSQL is healthy: `docker-compose ps postgres`
- Check Spark logs for specific error messages

### No Data in PostgreSQL

**Problem:** Tables are empty after running Spark

**Solution:**
- Ensure Spark job is running and connected to Kafka
- Verify producer is sending data
- Check Spark logs for errors
- Wait at least 5 minutes for first window aggregates
- Verify JDBC connection settings in `spark/streaming_traffic.py`

### Airflow DAG Not Visible

**Problem:** DAG doesn't appear in Airflow UI

**Solution:**
```bash
# Check Airflow scheduler logs
docker-compose logs airflow-scheduler

# Verify DAG file syntax
python -m py_compile airflow/dags/nightly_peak_hour.py

# Restart Airflow scheduler
docker-compose restart airflow-scheduler
```

### Port Already in Use

**Problem:** Port 8080, 9092, or 5432 already in use

**Solution:**
- Stop conflicting services
- Or modify ports in `docker-compose.yml`

## Stopping the Project

To stop all services:

```bash
# Stop all services
docker-compose down

# Stop and remove all data (WARNING: deletes databases)
docker-compose down -v
```

## Next Steps

1. **Monitor Real-Time**: Watch Spark logs and PostgreSQL data
2. **Generate Reports**: Trigger Airflow DAG manually or wait for schedule
3. **Analyze Results**: Review CSV reports and visualizations
4. **Experiment**: Modify producer frequency, window sizes, or alert thresholds

## Quick Reference

| Service | Port | Access |
|---------|------|--------|
| Kafka | 9092, 9093 | `localhost:9093` (external) |
| PostgreSQL | 5432 | `localhost:5432` |
| Airflow UI | 8080 | `http://localhost:8080` |
| Zookeeper | 2181 | Internal only |

| Credentials | Value |
|-------------|-------|
| Airflow Username | `admin` |
| Airflow Password | `admin` |
| PostgreSQL User | `postgres` |
| PostgreSQL Password | `postgres` |
| PostgreSQL Database | `trafficdb` |

## Need Help?

- Check `README.md` for detailed documentation
- Review `PROJECT_REPORT.md` for architecture details
- Check Docker logs: `docker-compose logs [service-name]`

