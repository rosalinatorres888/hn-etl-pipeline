# Hacker News Data Pipeline
### End-to-End ETL on AWS · Apache Airflow · S3 Data Lake · Glue · Athena

> Extracting 200 top stories daily from the Hacker News Firebase API, loading partitioned JSON into an S3 data lake, cataloging schema with AWS Glue, and enabling SQL analytics via Amazon Athena — fully orchestrated with Apache Airflow.

## Architecture

\`\`\`
HN Firebase API
       |
       v
Apache Airflow DAG (Daily 06:00 UTC)
extract_ids → extract_stories → transform → load_to_s3 → glue_crawler → manifest
       |
       v
S3 Data Lake (Hive-partitioned NDJSON)
s3://hn-data-lake/raw/stories/year=YYYY/month=MM/day=DD/
       |
       v
AWS Glue Crawler → Glue Data Catalog → Amazon Athena
\`\`\`

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9 |
| Extraction | Python · requests · ThreadPoolExecutor (20 workers) |
| Storage | Amazon S3 · Hive-style partitioning |
| Cataloging | AWS Glue Crawler · Glue Data Catalog |
| Querying | Amazon Athena · Standard SQL |
| Infrastructure | Docker · LocalStack |
| Language | Python 3.11 |
| Testing | pytest |

## Quick Start

\`\`\`bash
git clone https://github.com/rosalinatorres888/hn-etl-pipeline
cd hn-etl-pipeline
docker compose up -d
open http://localhost:8085   # Airflow UI · admin / admin
\`\`\`

## Pipeline — 7 Tasks

\`\`\`
start
  └─ extract_story_ids      # GET /topstories.json → 200 IDs
       └─ extract_stories   # Parallel fetch · 20 workers
            └─ transform    # Flatten · enrich · normalize
                 └─ load_to_s3      # NDJSON → S3 partitioned
                      └─ glue_crawler    # Schema catalog update
                           └─ manifest   # Audit log
                                └─ end
\`\`\`

## Key Engineering Decisions

**Parallel extraction** — ThreadPoolExecutor(max_workers=20) reduces 200 sequential API calls (~200s) to ~10s.

**Hive-style S3 partitioning** — enables Athena partition pruning, reducing query cost on large datasets.

**NDJSON format** — ideal for streaming reads, schema evolution, and Glue crawler inference at the raw layer.

**Run manifests** — every pipeline execution writes audit JSON to s3://hn-data-lake/manifests/ for lineage tracking.

## Sample Athena Query

\`\`\`sql
SELECT title, domain, by AS author, score, descendants AS comments
FROM hn_data_lake.stories
WHERE year = '2026' AND month = '05' AND day = '06'
ORDER BY score DESC
LIMIT 10;
\`\`\`

## Skills Demonstrated

Apache Airflow · AWS S3 · AWS Glue · Amazon Athena · Data Lake Design · ETL Pipeline Architecture · Parallel API Extraction · Schema Evolution · Docker · LocalStack · Python 3.11 · SQL Analytics

## Author

**Rosalina Torres**
MS Data Analytics Engineering · Northeastern University
[linkedin.com/in/rosalina-torres](https://linkedin.com/in/rosalina-torres) · [github.com/rosalinatorres888](https://github.com/rosalinatorres888)
