from __future__ import annotations
import json, logging, os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "rosalina",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

S3_BUCKET     = os.getenv("AIRFLOW_VAR_HN_S3_BUCKET", "hn-data-lake")
S3_PREFIX     = os.getenv("AIRFLOW_VAR_HN_S3_PREFIX", "raw/stories")
GLUE_CRAWLER  = os.getenv("AIRFLOW_VAR_HN_GLUE_CRAWLER", "hn-stories-crawler")
AWS_REGION    = os.getenv("AIRFLOW_VAR_HN_AWS_REGION", "us-east-1")
TOP_N_STORIES = int(os.getenv("AIRFLOW_VAR_HN_TOP_N", "200"))

log = logging.getLogger(__name__)

def extract_story_ids(**context):
    import requests
    resp = requests.get("https://hacker-news.firebaseio.com/v0/topstories.json", timeout=30)
    resp.raise_for_status()
    ids = resp.json()[:TOP_N_STORIES]
    log.info("Fetched %d story IDs", len(ids))
    context["task_instance"].xcom_push(key="story_ids", value=ids)
    return ids

def extract_stories(**context):
    import requests
    from concurrent.futures import ThreadPoolExecutor, as_completed
    ids = context["task_instance"].xcom_pull(key="story_ids", task_ids="extract_story_ids")
    def fetch_item(item_id):
        try:
            r = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json", timeout=10)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            log.warning("Failed to fetch %s: %s", item_id, exc)
            return None
    stories = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        for future in as_completed({executor.submit(fetch_item, sid): sid for sid in ids}):
            result = future.result()
            if result:
                stories.append(result)
    log.info("Extracted %d stories", len(stories))
    context["task_instance"].xcom_push(key="stories", value=stories)
    return stories

def _extract_domain(url):
    try:
        from urllib.parse import urlparse
        parts = urlparse(url or "").netloc.split(".")
        return ".".join(parts[-2:]) if len(parts) >= 2 else ""
    except Exception:
        return ""

def transform_stories(**context):
    from datetime import timezone
    stories = context["task_instance"].xcom_pull(key="stories", task_ids="extract_stories")
    run_date = context["ds"]
    run_ts = datetime.now(timezone.utc).isoformat()
    transformed = []
    for s in stories:
        if not s or s.get("type") != "story":
            continue
        transformed.append({
            "id": s.get("id"), "title": (s.get("title") or "").strip(),
            "url": s.get("url", ""), "domain": _extract_domain(s.get("url", "")),
            "by": s.get("by", ""), "score": s.get("score", 0),
            "descendants": s.get("descendants", 0),
            "kids_count": len(s.get("kids") or []),
            "kids": s.get("kids") or [],
            "text": (s.get("text") or "").strip(),
            "time": s.get("time"), "type": s.get("type", "story"),
            "run_date": run_date, "run_ts": run_ts,
        })
    log.info("Transformed %d stories", len(transformed))
    context["task_instance"].xcom_push(key="transformed", value=transformed)
    return transformed

def load_to_s3(**context):
    import boto3
    stories = context["task_instance"].xcom_pull(key="transformed", task_ids="transform_stories")
    run_date = context["ds"]
    year, month, day = run_date.split("-")
    s3_key = f"{S3_PREFIX}/year={year}/month={month}/day={day}/stories.json"
    ndjson = "\n".join(json.dumps(s) for s in stories)
    s3 = boto3.client("s3", region_name=AWS_REGION, endpoint_url=os.getenv("AWS_ENDPOINT_URL"))
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=ndjson.encode("utf-8"), ContentType="application/json")
    manifest = {"run_date": run_date, "s3_path": f"s3://{S3_BUCKET}/{s3_key}", "record_count": len(stories)}
    log.info("Loaded %d stories", len(stories))
    context["task_instance"].xcom_push(key="manifest", value=manifest)

def trigger_glue_crawler(**context):
    log.info("Glue crawler — skipped in local/dev mode")

def write_run_manifest(**context):
    manifest = context["task_instance"].xcom_pull(key="manifest", task_ids="load_to_s3")
    s3 = __import__("boto3").client("s3", region_name=AWS_REGION, endpoint_url=os.getenv("AWS_ENDPOINT_URL"))
    s3.put_object(Bucket=S3_BUCKET, Key=f"manifests/hn_etl_manifest_{context['ds']}.json",
                  Body=json.dumps(manifest, indent=2).encode("utf-8"), ContentType="application/json")
    log.info("Manifest written")

with DAG(
    dag_id="hn_etl_pipeline",
    description="Hacker News ETL",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "hacker-news"],
) as dag:
    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")
    t1 = PythonOperator(task_id="extract_story_ids",    python_callable=extract_story_ids)
    t2 = PythonOperator(task_id="extract_stories",      python_callable=extract_stories)
    t3 = PythonOperator(task_id="transform_stories",    python_callable=transform_stories)
    t4 = PythonOperator(task_id="load_to_s3",           python_callable=load_to_s3)
    t5 = PythonOperator(task_id="trigger_glue_crawler", python_callable=trigger_glue_crawler)
    t6 = PythonOperator(task_id="write_run_manifest",   python_callable=write_run_manifest)
    start >> t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> end