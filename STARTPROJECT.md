# Smart City Traffic & Congestion System

An end-to-end Lambda/Kappa architecture data pipeline for real-time traffic monitoring and congestion analysis in Colombo, Sri Lanka.

## Project Overview

This system simulates traffic sensors at four major junctions, ingests real-time data via Apache Kafka, processes streams using Apache Spark Structured Streaming, stores results in PostgreSQL, and orchestrates nightly batch jobs using Apache Airflow to generate peak traffic hour reports.

## Architecture

```
┌─────────────────┐
│ Sensor Producers│ (Python scripts simulating 4 junctions)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Kafka Topic    │ (traffic-events)
│  (Ingestion)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Spark Streaming │ (5-minute windows, congestion index, alerts)
└────────┬────────┘
         │
    ┌────┴────┐
    ▼         ▼
┌────────┐ ┌──────────┐
│Postgres│ │Kafka Topic│ (critical-traffic alerts)
│Storage │ └──────────┘
└────┬───┘
     │
     ▼
┌─────────────────┐
│  Airflow DAG    │ (Nightly peak hour computation)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  CSV Reports    │ (Peak traffic hour analysis)
└─────────────────┘
```

## Prerequisites

- Docker and Docker Compose installed
- Python 3.8+ (for running producers and visualization locally)
- Apache Spark 3.3.0+ (for running Spark streaming job)
- Java 8+ (required for Spark)

## Project Structure

```
Code/
├── producers/
│   └── sensor_producer.py          # Kafka producer simulating 4 junction sensors
├── spark/
│   └── streaming_traffic.py        # Spark Structured Streaming job
├── airflow/
│   ├── dags/
│   │   └── nightly_peak_hour.py    # Airflow DAG for nightly aggregation
│   └── reports/                    # Output directory for CSV reports
├── sql/
│   └── init_schema.sql             # PostgreSQL table creation scripts
├── visualization/
│   └── plot_traffic_report.py      # Matplotlib visualization script
├── docker-compose.yml               # Complete stack configuration
├── requirements.txt                 # Python dependencies
├── README.md                        # This file
└── PROJECT_REPORT.md                # Detailed project report
```

## Quick Start

### 1. Start Infrastructure Services

Start all services using Docker Compose:

```bash
docker-compose up -d
```

This will start:
- Zookeeper (port 2181)
- Kafka (ports 9092, 9093)
- PostgreSQL (port 5432)
- Airflow Webserver (port 8080)
- Airflow Scheduler

Wait for all services to be healthy (check with `docker-compose ps`).

### 2. Initialize Database Schema

The database schema is automatically initialized via the `init_schema.sql` script when PostgreSQL starts. If you need to manually create tables:

```bash
docker exec -i postgres psql -U postgres -d trafficdb < sql/init_schema.sql
```

### 3. Create Kafka Topics

Create the required Kafka topics:

```bash
# Create traffic-events topic
docker exec kafka kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic traffic-events \
  --partitions 3 \
  --replication-factor 1

# Create critical-traffic topic
docker exec kafka kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic critical-traffic \
  --partitions 1 \
  --replication-factor 1
```

### 4. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 5. Start Data Producer

Run the sensor producer to generate traffic data:

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

### 6. Run Spark Streaming Job

In a separate terminal, submit the Spark streaming job:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.5.0 \
  spark/streaming_traffic.py
```

**Note:** Adjust the Spark package version to match your Spark installation. For Spark 3.4.0, use `spark-sql-kafka-0-10_2.12:3.4.0`.

The Spark job will:
- Read from `traffic-events` topic
- Compute 5-minute tumbling windows
- Write alerts to `critical-traffic` topic and PostgreSQL when avg_speed < 10 km/h
- Write windowed aggregates to PostgreSQL

### 7. Access Airflow Web UI

Open your browser and navigate to:
```
http://localhost:8080
```

Login credentials:
- Username: `admin`
- Password: `admin`

The `nightly_peak_hour` DAG should be visible. You can trigger it manually or wait for the scheduled run at 02:00 UTC.

### 8. Generate Visualization

After Airflow generates a CSV report, visualize it:

```bash
python visualization/plot_traffic_report.py airflow/reports/peak_traffic_YYYYMMDD.csv
```

Replace `YYYYMMDD` with the actual date.

## Data Flow

### Real-Time Processing

1. **Producers** generate JSON messages every second per sensor:
   ```json
   {
     "sensor_id": "JUNCTION_A",
     "timestamp": "2024-01-15T12:34:56.789Z",
     "vehicle_count": 15,
     "avg_speed": 45.2
   }
   ```

2. **Kafka** receives messages in the `traffic-events` topic.

3. **Spark Streaming** processes messages:
   - Groups by sensor_id and 5-minute windows
   - Computes congestion index: `(50 - avg_speed) * (sum_vehicle_count / 100)`
   - Filters alerts when `avg_speed < 10 km/h`
   - Writes aggregates to `traffic_window_agg` table
   - Writes alerts to `traffic_alerts` table and `critical-traffic` topic

### Batch Processing (Nightly)

1. **Airflow DAG** runs at 02:00 UTC daily
2. Reads previous day's window aggregates from PostgreSQL
3. Computes peak traffic hour per junction
4. Generates CSV report: `peak_traffic_YYYYMMDD.csv`
5. Inserts results into `daily_peak` table

## Database Schema

### Tables

- **traffic_window_agg**: 5-minute windowed aggregates
  - sensor_id, window_start, window_end, sum_vehicle_count, avg_speed, congestion_index

- **traffic_alerts**: Critical traffic events (avg_speed < 10 km/h)
  - sensor_id, timestamp, vehicle_count, avg_speed

- **daily_peak**: Daily peak hour per junction
  - sensor_id, peak_hour (0-23), total_vehicles, date

## Monitoring & Verification

### Check Kafka Topics

```bash
# List topics
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# Consume messages from traffic-events
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic traffic-events \
  --from-beginning

# Consume alerts
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic critical-traffic \
  --from-beginning
```

### Query PostgreSQL

```bash
# Connect to database
docker exec -it postgres psql -U postgres -d trafficdb

# Check window aggregates
SELECT * FROM traffic_window_agg ORDER BY window_start DESC LIMIT 10;

# Check alerts
SELECT * FROM traffic_alerts ORDER BY timestamp DESC LIMIT 10;

# Check daily peaks
SELECT * FROM daily_peak ORDER BY date DESC;
```

### Check Airflow Logs

```bash
# View DAG logs via web UI or:
docker logs airflow-scheduler
docker logs airflow-webserver
```

## Configuration

### Kafka Configuration

- **Bootstrap servers**: `localhost:9093` (external) or `kafka:9092` (internal)
- **Topics**: `traffic-events`, `critical-traffic`

### Spark Configuration

Edit `spark/streaming_traffic.py` to adjust:
- Window duration (default: 5 minutes)
- Watermark delay (default: 2 minutes)
- Congestion index formula
- Alert threshold (default: avg_speed < 10)

### Airflow Configuration

Edit `airflow/dags/nightly_peak_hour.py` to adjust:
- Schedule interval (default: daily at 02:00 UTC)
- Report output directory
- Aggregation logic

## Troubleshooting

### Kafka Connection Issues

- Ensure Kafka is running: `docker-compose ps`
- Check if ports 9092/9093 are available
- Verify producer uses `localhost:9093` (external listener)

### Spark Job Fails

- Verify Spark version matches package versions
- Check PostgreSQL connection (ensure postgres service is healthy)
- Verify checkpoint directories are writable

### Airflow DAG Not Appearing

- Check DAGs directory is mounted correctly
- Verify Python syntax: `docker logs airflow-scheduler`
- Ensure Airflow has initialized: `docker logs airflow-init`

### No Data in PostgreSQL

- Verify Spark streaming job is running
- Check Spark logs for errors
- Verify JDBC connection settings in `streaming_traffic.py`

## Expected Outputs

### Real-Time Alerts

When critical traffic is detected (avg_speed < 10 km/h), you should see:
- Messages in `critical-traffic` Kafka topic
- Rows in `traffic_alerts` PostgreSQL table

### Window Aggregates

Every 5 minutes, Spark writes aggregated data to `traffic_window_agg` table with:
- Total vehicle count per sensor per window
- Average speed
- Computed congestion index

### Nightly Reports

CSV files in `airflow/reports/` directory:
```
sensor_id,peak_hour,vehicle_count
JUNCTION_A,17,1250
JUNCTION_B,18,980
JUNCTION_C,17,1100
JUNCTION_D,19,890
```

## Cleanup

Stop all services:

```bash
docker-compose down
```

Remove volumes (WARNING: deletes all data):

```bash
docker-compose down -v
```

## Technology Stack Justification

- **Apache Kafka**: Durable, partitioned message queue for high-throughput IoT data ingestion
- **Apache Spark Structured Streaming**: High-level streaming API with windowing, event-time handling, and Kafka integration
- **PostgreSQL**: Relational database for structured storage, easy querying, and CSV export
- **Apache Airflow**: Workflow orchestration for scheduled batch jobs and ETL pipelines
- **Docker Compose**: Simplified local development and testing environment

## Event Time vs Processing Time

- **Event Time**: Timestamp from sensor message (`timestamp` field) - used for windowing
- **Processing Time**: When Spark receives the message - affects alert latency
- **Watermark**: 2-minute tolerance for late-arriving events

See `PROJECT_REPORT.md` for detailed explanation.

## Ethics & Privacy

This system collects aggregate traffic data (vehicle counts, speeds) without personal identifiers. For production deployment:
- Implement data retention policies
- Enforce access controls
- Encrypt data in transit and at rest
- Maintain audit logs
- Comply with local privacy regulations

See `PROJECT_REPORT.md` for detailed ethics discussion.

## License

This project is created for educational purposes as part of the Applied Big Data Engineering course.

## Authors

Group/Individual Project - Smart City Traffic & Congestion System

## References

- Apache Kafka Documentation: https://kafka.apache.org/documentation/
- Apache Spark Structured Streaming Guide: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- Apache Airflow Documentation: https://airflow.apache.org/docs/
- PostgreSQL Documentation: https://www.postgresql.org/docs/