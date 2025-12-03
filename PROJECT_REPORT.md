# Project Report: Smart City Traffic & Congestion System

**Course:** Applied Big Data Engineering  
**Scenario:** Smart City Traffic & Congestion System (Colombo)  
**Architecture:** Lambda/Kappa Pipeline  
**Date:** 2024

---

## 1. Introduction

This project implements an end-to-end streaming data pipeline to monitor traffic congestion at four key junctions in Colombo, Sri Lanka. The system demonstrates real-time ingestion, streaming analytics, immediate alerting, and nightly orchestration for historical reporting. The implementation fulfills the assessment objectives of designing a Lambda/Kappa architecture pipeline utilizing Apache Kafka for ingestion, Apache Spark Structured Streaming for stream processing, Apache Airflow for orchestration, and PostgreSQL for storage.

The city of Colombo requires continuous monitoring at four major intersections to optimize traffic flow and allocate resources effectively. Sensors broadcast per-second messages containing junction identifiers, timestamps, vehicle counts, and average speeds. The system must process this data in real-time to detect congestion, generate immediate alerts for critical situations, and produce daily analytical reports to guide traffic management decisions.

## 2. Scenario & Objectives

The Smart City Traffic & Congestion System addresses the following requirements:

1. **Real-Time Monitoring**: Continuously ingest sensor data from four junctions (JUNCTION_A, JUNCTION_B, JUNCTION_C, JUNCTION_D) producing JSON messages every second.

2. **Stream Processing**: Compute 5-minute tumbling window aggregates per junction, calculating total vehicle counts, average speeds, and a derived congestion index metric.

3. **Immediate Alerting**: Trigger alerts when average speed drops below 10 km/h, indicating critical congestion requiring immediate traffic police intervention.

4. **Historical Analysis**: Store windowed aggregates for batch processing and generate nightly reports identifying peak traffic hours per junction to optimize resource allocation.

5. **Reporting**: Produce CSV reports and visualizations showing Traffic Volume vs. Time of Day to support data-driven decision-making.

## 3. Architecture Overview

The pipeline follows a Lambda/Kappa architecture pattern, combining real-time stream processing with batch analytics:

### 3.1 Components

**Data Sources (Producers)**
- Python scripts simulate four junction sensors, generating JSON messages with sensor_id, timestamp, vehicle_count, and avg_speed fields. The producer occasionally injects critical traffic events (low avg_speed) to test alert mechanisms.

**Ingestion Layer (Apache Kafka)**
- Kafka provides a durable, partitioned message queue for sensor events. The `traffic-events` topic receives raw sensor data, while the `critical-traffic` topic stores immediate alerts. Kafka's partitioning enables horizontal scaling and fault tolerance.

**Stream Processing (Apache Spark Structured Streaming)**
- Spark reads from Kafka, parses JSON messages, converts timestamps to event-time columns, and applies 5-minute tumbling window aggregations. The job computes a congestion index, filters critical events, and writes results to both Kafka (alerts) and PostgreSQL (aggregates and alerts).

**Storage Layer (PostgreSQL)**
- Three tables store different data types:
  - `traffic_window_agg`: 5-minute windowed aggregates for batch analysis
  - `traffic_alerts`: Critical traffic events requiring immediate attention
  - `daily_peak`: Daily peak hour summaries per junction

**Orchestration (Apache Airflow)**
- A nightly DAG scheduled at 02:00 UTC reads previous day's window aggregates, computes peak traffic hours per junction, generates CSV reports, and updates the daily_peak table.

### 3.2 Data Flow

1. **Real-Time Path**: Sensors → Kafka → Spark Streaming → (Alerts to Kafka/Postgres, Aggregates to Postgres)
2. **Batch Path**: Postgres (window aggregates) → Airflow DAG → CSV Reports → Visualization

## 4. Technology Stack Justification

### 4.1 Apache Kafka

**Selection Rationale**: Kafka was chosen for its durability, high throughput, and partitioning capabilities. IoT sensor data requires reliable ingestion that can handle bursts and network interruptions. Kafka's log-based architecture ensures no data loss, while partitions enable parallel processing and scalability.

**Key Features Utilized**:
- Topics and partitions for organizing sensor data
- Producer API for sensor simulators
- Consumer API for Spark integration
- Durability guarantees for critical traffic data

**Alternatives Considered**: RabbitMQ, Apache Pulsar. Kafka was selected for its superior throughput, built-in partitioning, and seamless Spark integration.

### 4.2 Apache Spark Structured Streaming

**Selection Rationale**: Spark Structured Streaming provides high-level abstractions for stream processing with built-in support for event-time windowing, watermarks, and Kafka integration. It simplifies complex aggregations compared to lower-level frameworks like Storm.

**Key Features Utilized**:
- Kafka source connector for reading streams
- Event-time windowing (5-minute tumbling windows)
- Watermarks for late event handling
- foreachBatch for JDBC writes to PostgreSQL
- Kafka sink for alert distribution

**Alternatives Considered**: Apache Storm, Apache Flink. Spark was chosen for its SQL-like API, better documentation, and easier integration with existing Spark ecosystems.

### 4.3 PostgreSQL

**Selection Rationale**: PostgreSQL provides a robust relational database with excellent JDBC support, ACID guarantees, and straightforward CSV export capabilities. For this project's scale, PostgreSQL offers simplicity while maintaining performance.

**Key Features Utilized**:
- Relational schema for structured data
- JDBC connectivity from Spark
- Indexed queries for efficient daily aggregation
- CSV export for reporting

**Alternatives Considered**: Cassandra, HDFS/Parquet. PostgreSQL was selected for its simplicity, SQL query capabilities, and sufficient performance for this use case. For production at scale, Parquet on HDFS/S3 would be preferred.

### 4.4 Apache Airflow

**Selection Rationale**: Airflow provides declarative workflow definition, scheduling, monitoring, and retry mechanisms. It excels at orchestrating batch jobs that depend on data availability and time-based triggers.

**Key Features Utilized**:
- PythonOperator for custom aggregation logic
- Daily scheduling (cron-based)
- Dependency management and retries
- Web UI for monitoring and manual triggers

**Alternatives Considered**: Luigi, Prefect. Airflow was chosen for its maturity, extensive community support, and built-in scheduling capabilities.

### 4.5 Docker Compose

**Selection Rationale**: Docker Compose simplifies local development by containerizing all services, ensuring consistent environments, and enabling easy service orchestration.

## 5. Implementation Details

### 5.1 Producer Implementation

The sensor producer (`producers/sensor_producer.py`) simulates four junctions, generating realistic traffic data:
- Normal traffic: vehicle_count 0-20, avg_speed 15-60 km/h
- Critical events: vehicle_count 0-40, avg_speed 2-8 km/h (1 in 200 probability)
- ISO 8601 timestamps with UTC timezone
- Clean, self-documenting console output

### 5.2 Spark Streaming Job

The Spark job (`spark/streaming_traffic.py`) implements:

**Event Parsing**: Converts Kafka byte arrays to JSON strings, parses into structured DataFrame with schema validation.

**Event-Time Conversion**: Transforms ISO timestamp strings to Spark `TimestampType` for accurate windowing.

**Windowing**: Groups by sensor_id and 5-minute tumbling windows using `window(event_time, "5 minutes")`. Watermark set to 2 minutes tolerates late arrivals while bounding state.

**Congestion Index**: Computed as `GREATEST(0, (50 - avg_speed) * (sum_vehicle_count / 100.0))`. Higher values indicate worse congestion.

**Alert Detection**: Filters records where `avg_speed < 10` km/h, writes to both Kafka topic (`critical-traffic`) and PostgreSQL table (`traffic_alerts`) for redundancy.

**Aggregate Storage**: Uses `foreachBatch` to write windowed aggregates to PostgreSQL `traffic_window_agg` table, enabling efficient batch queries.

### 5.3 Airflow DAG

The nightly DAG (`airflow/dags/nightly_peak_hour.py`):

**Scheduling**: Runs daily at 02:00 UTC using cron expression `0 2 * * *`.

**Data Retrieval**: Queries `traffic_window_agg` for previous day's data, filtering by date range.

**Aggregation**: Groups window aggregates by sensor and hour, summing vehicle counts to identify peak hours.

**Report Generation**: Creates CSV file with columns: sensor_id, peak_hour, vehicle_count. File named `peak_traffic_YYYYMMDD.csv`.

**Database Update**: Inserts/updates `daily_peak` table with peak hour results, using ON CONFLICT for idempotency.

### 5.4 Database Schema

Three tables support the pipeline:

**traffic_window_agg**: Stores 5-minute window aggregates with indexes on window_start and sensor_id for efficient date-range queries.

**traffic_alerts**: Stores critical events with indexes on timestamp and sensor_id for alert monitoring.

**daily_peak**: Stores daily summaries with unique constraint on (sensor_id, date) to prevent duplicates.

## 6. Event Time vs Processing Time

Handling time semantics correctly is critical for accurate analytics:

### 6.1 Event Time

**Definition**: The timestamp embedded in the sensor message (`timestamp` field), representing when the observation occurred in the real world.

**Usage**: Used as the primary time dimension for windowing. This ensures that aggregates align with actual occurrence times, not processing delays. For example, a message arriving late due to network delay still falls into its correct 5-minute window based on event_time.

**Implementation**: Spark converts ISO timestamp strings to `TimestampType` and uses `window(event_time, "5 minutes")` for grouping.

### 6.2 Processing Time

**Definition**: The wall-clock time when Spark receives and processes the message.

**Usage**: Determines alert latency. Alerts are triggered based on event-time values (avg_speed < 10), but delivery speed depends on processing time. Near real-time responsiveness is achieved by processing events as they arrive.

**Trade-offs**: Using processing time for windowing would be simpler but incorrect for late-arriving data. Event time ensures correctness but requires watermarking to bound state.

### 6.3 Watermarks

**Purpose**: Balance correctness and resource usage by accepting late events within a tolerance window, then purging state.

**Implementation**: `.withWatermark("event_time", "2 minutes")` tells Spark to accept events up to 2 minutes late, then drop older state. This prevents unbounded memory growth while accommodating transient network delays.

**Example**: If current processing time is 12:10:00 and watermark is 2 minutes, Spark will process events with event_time >= 12:08:00. Events older than 12:08:00 are considered too late and dropped.

### 6.4 Practical Implications

- **Window Aggregates**: Use event_time for accurate time-aligned results
- **Alerts**: Based on event_time values but delivered with processing-time latency
- **Late Events**: Handled gracefully via watermarks, preventing state explosion
- **Clock Skew**: Event-time windowing mitigates sensor clock synchronization issues

## 7. Ethics & Privacy Considerations

While this scenario collects aggregate traffic data without personal identifiers, several privacy and ethical considerations must be addressed:

### 7.1 Privacy Risks

**Location Inference**: Junction sensor IDs combined with timestamps could reveal movement patterns if correlated with other data sources. While individual vehicles aren't identified, aggregate patterns might expose sensitive information about traffic flows, business districts, or event locations.

**Surveillance Concerns**: Continuous monitoring of public spaces raises questions about surveillance and citizen privacy. Even aggregate data can be used to infer behavior patterns, peak activity times, and potentially sensitive information about commercial or residential areas.

**Data Linkage**: If this system is integrated with other datasets (e.g., license plate recognition, mobile phone location data), the combination could enable re-identification or tracking of individuals.

**Retention Risks**: Long-term storage of traffic data creates a historical record that could be misused for surveillance, profiling, or discrimination.

### 7.2 Data Governance Measures

**Data Minimization**: The system collects only necessary fields (sensor_id, timestamp, vehicle_count, avg_speed). No personal identifiers, license plates, or vehicle-specific data are stored. This principle should be maintained even as the system evolves.

**Retention Policies**: Implement automated data retention:
- Raw second-level events: Retain for 7 days, then purge
- Window aggregates: Retain for 90 days for trend analysis
- Daily peaks: Retain for 1 year for annual comparisons
- Alerts: Retain for 30 days for incident review

**Access Control**: Enforce strict access controls:
- Kafka topics: Authenticate producers and consumers, use ACLs to restrict topic access
- PostgreSQL: Role-based access control (RBAC), limit read/write permissions to authorized personnel only
- Airflow: Secure web UI with authentication, restrict DAG execution permissions

**Encryption**: Protect data in transit and at rest:
- Kafka: Enable TLS/SSL for producer and consumer connections
- PostgreSQL: Encrypt connections using SSL, consider encryption at rest for sensitive tables
- Network: Use VPN or private networks for inter-service communication

**Audit Logging**: Maintain comprehensive audit trails:
- Log all database queries, especially data exports
- Track Airflow DAG executions and manual triggers
- Monitor Kafka topic access and message consumption
- Alert on unusual access patterns or bulk data exports

**Anonymization**: If future expansions include vehicle identifiers:
- Hash or anonymize identifiers before storage
- Store original identifiers separately with strict access controls
- Use differential privacy techniques for aggregate statistics
- Publish privacy notices explaining data collection and use

**Legal Compliance**: Ensure compliance with local regulations:
- Sri Lanka's Data Protection Act (when applicable)
- Right to information laws
- Surveillance and privacy regulations
- Obtain necessary approvals for public space monitoring

**Transparency**: Publish clear privacy policies:
- Explain what data is collected and why
- Describe how data is used and shared
- Provide mechanisms for citizen inquiries or complaints
- Regular privacy impact assessments

**Ethical Oversight**: Establish governance framework:
- Data ethics committee to review system expansions
- Regular audits of data access and usage
- Public consultation for significant changes
- Clear policies on data sharing with third parties

### 7.3 Mitigation Summary

The current implementation minimizes privacy risks by collecting only aggregate, non-identifying data. However, as the system scales or integrates with other datasets, the above governance measures become essential. The architecture should be designed with privacy-by-design principles, ensuring that future enhancements don't compromise citizen privacy.

## 8. Testing & Demonstration

### 8.1 Test Plan

**Infrastructure Setup**:
1. Start Docker Compose services, verify all containers are healthy
2. Create Kafka topics using kafka-topics.sh
3. Initialize PostgreSQL schema (automatic via init script)
4. Verify Airflow web UI is accessible

**Producer Testing**:
1. Run sensor producer, verify messages appear in Kafka topic
2. Check for occasional critical events (low avg_speed)
3. Verify message format and timestamp accuracy

**Stream Processing Testing**:
1. Submit Spark job, verify it connects to Kafka
2. Monitor Spark logs for window aggregation output
3. Query PostgreSQL to verify aggregates are written
4. Trigger critical event, verify alert appears in `traffic_alerts` table and `critical-traffic` topic

**Batch Processing Testing**:
1. Manually trigger Airflow DAG or wait for scheduled run
2. Verify CSV report is generated in `airflow/reports/`
3. Check `daily_peak` table for inserted records
4. Validate peak hour calculations are correct

**End-to-End Testing**:
1. Run producer for 30+ minutes to generate sufficient data
2. Verify Spark creates multiple 5-minute windows
3. Trigger Airflow DAG, verify report contains all four junctions
4. Generate visualization, verify charts display correctly

### 8.2 Expected Outputs

**Real-Time Alerts**: When avg_speed < 10 km/h:
- JSON message in `critical-traffic` Kafka topic
- Row in `traffic_alerts` PostgreSQL table with sensor_id, timestamp, vehicle_count, avg_speed

**Window Aggregates**: Every 5 minutes:
- Row in `traffic_window_agg` with sensor_id, window_start, window_end, sum_vehicle_count, avg_speed, congestion_index

**Nightly Reports**: Daily CSV file:
```
sensor_id,peak_hour,vehicle_count
JUNCTION_A,17,1250
JUNCTION_B,18,980
JUNCTION_C,17,1100
JUNCTION_D,19,890
```

**Visualization**: PNG files showing:
- Bar charts per junction with peak hour highlighted
- Line chart comparing traffic volumes across junctions

## 9. Limitations & Future Extensions

### 9.1 Current Limitations

**Scale**: Designed for four junctions; production would require horizontal scaling of Kafka, Spark, and storage.

**Fault Tolerance**: Basic retry logic; production needs comprehensive error handling, dead letter queues, and monitoring.

**Monitoring**: Limited observability; production requires Prometheus/Grafana for metrics, alerting, and dashboards.

**Security**: Development setup with basic authentication; production needs TLS, encryption, and comprehensive access controls.

**Data Quality**: No validation or schema evolution handling; production needs data quality checks and schema registry.

### 9.2 Future Extensions

**Real-Time Dashboard**: Integrate Grafana or custom web dashboard showing live traffic conditions, congestion maps, and alert notifications.

**Machine Learning**: Add predictive models to forecast congestion based on historical patterns, weather, and events.

**Dynamic Traffic Control**: Integrate with traffic light systems to adjust signal timing based on real-time congestion data.

**Multi-City Support**: Extend to multiple cities with centralized monitoring and comparative analytics.

**Advanced Analytics**: Implement anomaly detection, trend analysis, and correlation with external factors (weather, events, holidays).

**Data Lake Integration**: Store raw data in Parquet format on HDFS/S3 for long-term analytics and data science workloads.

**API Layer**: Expose REST APIs for third-party integrations and mobile applications.

## 10. Conclusion

This project successfully demonstrates an end-to-end streaming data pipeline implementing Lambda/Kappa architecture principles. The system ingests real-time sensor data via Kafka, processes streams using Spark Structured Streaming with event-time windowing, stores results in PostgreSQL, and orchestrates nightly batch jobs using Airflow to generate actionable reports.

Key achievements include:
- Correct handling of event-time vs processing-time semantics
- Real-time alerting for critical traffic conditions
- Historical analysis for peak hour identification
- Comprehensive documentation and ethical considerations
- Reproducible Docker-based deployment

The implementation satisfies all assessment requirements: Kafka ingestion with topics and partitions, Spark Structured Streaming with windowing, Airflow orchestration for batch jobs, and PostgreSQL storage with CSV report generation. The system demonstrates practical understanding of streaming architectures, time semantics, and data governance principles essential for production big data systems.

While designed for educational purposes, the architecture and implementation patterns are directly applicable to production traffic management systems, with appropriate scaling, security, and monitoring enhancements.

---

**Word Count**: ~1,500 words

