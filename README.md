# Hacker News ETL Pipeline

**End-to-end data engineering pipeline — Apache Airflow · LocalStack S3 · Docker · Python 3.11**

> Extracts the top 200 Hacker News stories daily from the Firebase API, transforms and partitions them into a Hive-style S3 data lake, and renders a custom HTML news reader — fully orchestrated with Apache Airflow running in Docker.

---

## Architecture

```
Hacker News Firebase API
        │
        ▼
Apache Airflow DAG  (daily @ 06:00 UTC)
  extract_story_ids → extract_stories → transform → load_to_s3 → glue_crawler → manifest
        │
        ▼
S3 Data Lake  (Hive-partitioned NDJSON)
  s3://hn-data-lake/raw/stories/year=YYYY/month=MM/day=DD/
        │
        ▼
  today_hn.json → hn_reader_live.html
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9 |
| Extraction | Python · requests · ThreadPoolExecutor (20 workers) |
| Storage | LocalStack S3 · Hive-style partitioning |
| Cataloging | AWS Glue Crawler (LocalStack) |
| Infrastructure | Docker · docker-compose |
| Output | Custom HTML news reader |
| Language | Python 3.11 |
| Testing | pytest |

---

## Pipeline — 7 Tasks

```
start
└─ extract_story_ids     GET /topstories.json → top 200 IDs
└─ extract_stories       Parallel fetch · 20 workers · ~10s for 200 stories
└─ transform_stories     Flatten · enrich · normalize · filter to type=story
└─ load_to_s3            NDJSON → partitioned S3 data lake
└─ trigger_glue_crawler  Schema catalog update
└─ write_run_manifest    Audit JSON → s3://hn-data-lake/manifests/
└─ end
```

---

## Quick Start

```bash
git clone https://github.com/rosalinatorres888/hn-etl-pipeline
cd hn-etl-pipeline

# Start all services
docker compose up -d

# Copy DAG into containers (required on macOS due to volume mount behavior)
docker cp dags/hn_etl_dag.py hn_etl-airflow-scheduler-1:/opt/airflow/dags/hn_etl_dag.py
docker cp dags/hn_etl_dag.py hn_etl-airflow-webserver-1:/opt/airflow/dags/hn_etl_dag.py

# Create the S3 bucket in LocalStack
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  aws --endpoint-url=http://localhost:4566 s3 mb s3://hn-data-lake --region us-east-1

# Open Airflow UI
open http://localhost:8085   # credentials: airflow / airflow

# Trigger the pipeline
docker exec hn_etl-airflow-webserver-1 airflow dags trigger hn_etl_pipeline
```

---

## Generating the HTML Reader

After the pipeline completes, pull today's data from S3 and render the reader:

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  aws --endpoint-url=http://localhost:4566 \
  s3 cp s3://hn-data-lake/raw/stories/year=YYYY/month=MM/day=DD/stories.json \
  ~/Downloads/today_hn.json

python3 scripts/refresh_reader.py
open ~/Downloads/hn_reader_live.html
```

---

## Key Engineering Decisions

**Parallel extraction** — `ThreadPoolExecutor(max_workers=20)` reduces 200 sequential API calls (~200s) to ~10s.

**Hive-style S3 partitioning** — `year=/month=/day=` structure enables partition pruning for efficient querying at scale.

**NDJSON format** — ideal for streaming reads, schema evolution, and Glue crawler inference at the raw layer.

**Run manifests** — every execution writes an audit JSON to `s3://hn-data-lake/manifests/` for lineage tracking.

**LocalStack** — full AWS S3/Glue simulation locally with zero cloud cost during development.

---

## Repository Structure

```
hn-etl-pipeline/
├── dags/
│   └── hn_etl_dag.py        # 7-task Airflow DAG
├── scripts/
│   ├── refresh_reader.py    # Pulls S3 data → renders HTML reader
│   └── glue_setup.py        # Glue catalog setup
├── athena/                  # Sample Athena queries
├── tests/                   # pytest test suite
├── docker-compose.yml       # Airflow + LocalStack + Postgres
└── README.md
```

---

## Sample Query (Athena / LocalStack)

```sql
SELECT title, domain, by AS author, score, descendants AS comments
FROM hn_stories
WHERE year = '2026' AND month = '05' AND day = '19'
ORDER BY score DESC
LIMIT 10;
```

---

## Author

**Rosalina Torres**
MS Data Analytics Engineering · Northeastern University
[linkedin.com/in/rosalina-torres](https://linkedin.com/in/rosalina-torres) · [github.com/rosalinatorres888](https://github.com/rosalinatorres888)
