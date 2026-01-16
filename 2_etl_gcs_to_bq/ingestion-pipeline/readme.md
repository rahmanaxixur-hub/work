1. Executive Summary

This document describes the architecture for a reliable, scalable, and observable batch data ingestion pipeline that transfers large Parquet datasets from AWS S3 into Google BigQuery native tables using GCP managed services and Apache Airflow (Cloud Composer 2).

The solution ensures:

High reliability and fault tolerance

Idempotent reprocessing

Schema evolution handling

Data quality validation

End-to-end observability

Cost-efficient large file ingestion (500MB+ Parquet)

2. Business Objective

Enable the Data Platform Team to:

Ingest daily batch data from external AWS sources

Maintain consistent and trusted analytical datasets in BigQuery

Support downstream analytics, reporting, and ML workloads

Guarantee operational robustness and auditability

3. Non-Functional Requirements
Requirement	Description
Reliability	Automatic retries, failure isolation, safe re-runs
Scalability	Handle large Parquet files and growing volume
Idempotency	Re-running the same day must not duplicate data
Observability	File-level and record-level monitoring
Schema Evolution	Detect and safely apply backward-compatible changes
Cost Efficiency	Minimize cross-cloud egress and BigQuery query costs
Security	Least-privilege IAM, encrypted transport, audit logs
4. High-Level Architecture

Flow:

AWS S3
→ GCP Storage Transfer Service (STS)
→ GCS Landing Zone (dt=yyyy-mm-dd)
→ BigQuery External Table
→ BigQuery Load Job
→ Staging (Silver) Table
→ MERGE / UPSERT
→ Main (Gold) Table
→ Airflow (Cloud Composer 2) Orchestration

5. Component Design
5.1 Storage Transfer Service (STS)

Purpose:

Managed, resumable, incremental transfer from S3 to GCS

Handles retries, checksum validation, parallelism

Key Features:

Daily scheduled job

Prefix-based partitioning (dt folders)

Transfer logs for auditability

5.2 GCS Landing Zone (Bronze)

Structure:

gs://raw-bucket/dataset_name/dt=2026-01-16/*.parquet


Responsibilities:

Immutable raw storage

Retention policy (e.g., 30–90 days)

Source-of-truth for replay

5.3 BigQuery External Table

Purpose:

Lightweight schema and readability validation

No data movement or storage duplication

Checks:

File accessibility

Column presence

Data type compatibility

Partition completeness

5.4 BigQuery Staging Table (Silver)

Purpose:

Physical load target

Partitioned by ingestion date

Clustered by business keys

Characteristics:

Append-only

Load jobs with schema auto-detect + evolution

File-level error capture

5.5 Main Table (Gold)

Purpose:

Curated analytical layer

Deduplicated

Business-ready

Method:

MERGE using primary/business key

Supports late arriving data

Supports updates and deletes (SCD-1 or SCD-2)

6. Airflow (Cloud Composer 2) Orchestration
DAG Stages

Trigger STS Transfer

Validate GCS arrival

External Table Schema Check

Data Quality Checks

Load to Staging

Merge to Main

Audit Logging

Metrics & Alerts

7. Data Quality & Validation

Checks include:

Row count reconciliation

Null constraint checks

Primary key uniqueness

Partition completeness

Schema drift detection

Corrupt file detection

Failures block downstream tasks.

8. Idempotency & Reprocessing

Strategy:

Partition overwrite in staging

Deterministic MERGE in main table

File-level audit table:

Column	Description
file_name	GCS object
checksum	Integrity validation
load_status	SUCCESS / FAILED
row_count	Loaded rows
load_time	Timestamp

This enables:

Safe re-runs

Partial recovery

Exactly-once semantics

9. Error Handling & Retry
Layer	Handling
STS	Automatic retries, resume
GCS	Object existence verification
BQ Load	Retry on transient errors
Merge	Transactional atomicity
Airflow	Task retries, SLA alerts
10. Security & IAM

S3 → GCS: Encrypted transfer

STS service account: Read-only S3, Write-only GCS

BigQuery: Least-privilege dataset roles

Airflow: Workload Identity

Audit logs enabled for:

Storage access

BigQuery jobs

Schema changes

11. Performance & Cost Optimization

Parquet columnar format

Batch load jobs (not streaming)

Partition pruning

Clustered merge keys

Slot-based scheduling window

STS parallel transfer tuning

12. Monitoring & Alerting

Metrics:

Files transferred

Rows ingested

Load duration

Data freshness

Error rate

Schema drift events

Tools:

Cloud Monitoring

Airflow SLA miss alerts

BigQuery job statistics

13. Limitations

Batch latency (not real-time)

Schema breaking changes require manual intervention

Cross-cloud egress cost from S3

14. Future Enhancements

Auto schema evolution with approval workflow

CDC support (Debezium / DMS)

Iceberg / BigLake unified lakehouse

Data contract enforcement

Row-level lineage (Dataplex / OpenLineage)

15. Appendix
A. Example BigQuery MERGE
MERGE gold_table T
USING staging_table S
ON T.id = S.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT ROW;

B. DAG Logical Structure
transfer_task
 >> validate_files
 >> external_schema_check
 >> dq_checks
 >> load_staging
 >> merge_gold
 >> audit_log
 >> notify