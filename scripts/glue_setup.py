"""
glue_setup.py
=============
Creates the AWS Glue Data Catalog database, S3 target table,
and crawler for the Hacker News data lake.

Run once to bootstrap infrastructure:
    python glue_setup.py --bucket hn-data-lake --region us-east-1
"""

import argparse
import boto3
import json
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def create_glue_database(glue_client, db_name: str):
    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": db_name,
                "Description": "Hacker News data lake — raw stories partitioned by date",
            }
        )
        log.info("Created Glue database: %s", db_name)
    except glue_client.exceptions.AlreadyExistsException:
        log.info("Glue database already exists: %s", db_name)


def create_iam_role(iam_client, role_name: str, bucket: str) -> str:
    """Create IAM role for Glue crawler with S3 read access."""

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "glue.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }]
    }

    s3_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:ListBucket"],
                "Resource": [
                    f"arn:aws:s3:::{bucket}",
                    f"arn:aws:s3:::{bucket}/*",
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "glue:*",
                ],
                "Resource": "*"
            }
        ]
    }

    try:
        resp = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Glue crawler role for HN data lake",
        )
        role_arn = resp["Role"]["Arn"]

        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName="hn-glue-s3-access",
            PolicyDocument=json.dumps(s3_policy),
        )
        log.info("Created IAM role: %s → %s", role_name, role_arn)
        return role_arn

    except iam_client.exceptions.EntityAlreadyExistsException:
        role_arn = iam_client.get_role(RoleName=role_name)["Role"]["Arn"]
        log.info("IAM role already exists: %s → %s", role_name, role_arn)
        return role_arn


def create_glue_crawler(
    glue_client,
    crawler_name: str,
    db_name: str,
    bucket: str,
    prefix: str,
    role_arn: str,
):
    try:
        glue_client.create_crawler(
            Name=crawler_name,
            Role=role_arn,
            DatabaseName=db_name,
            Description="Crawls HN stories partitioned JSON in S3",
            Targets={
                "S3Targets": [{
                    "Path": f"s3://{bucket}/{prefix}/",
                    "Exclusions": ["manifests/**"],
                }]
            },
            Schedule="cron(30 6 * * ? *)",  # 06:30 UTC daily (after ETL loads)
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "LOG",
            },
            RecrawlPolicy={"RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY"},
            Configuration=json.dumps({
                "Version": 1.0,
                "CrawlerOutput": {
                    "Partitions": {"AddOrUpdateBehavior": "InheritFromTable"},
                    "Tables": {"AddOrUpdateBehavior": "MergeNewColumns"},
                },
                "Grouping": {"TableGroupingPolicy": "CombineCompatibleSchemas"},
            }),
        )
        log.info("Created Glue crawler: %s", crawler_name)
    except glue_client.exceptions.AlreadyExistsException:
        log.info("Glue crawler already exists: %s", crawler_name)


def create_s3_bucket(s3_client, bucket: str, region: str):
    try:
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket)
        else:
            s3_client.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": region},
            )

        # Block all public access
        s3_client.put_public_access_block(
            Bucket=bucket,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            },
        )

        # Enable versioning for data lineage
        s3_client.put_bucket_versioning(
            Bucket=bucket,
            VersioningConfiguration={"Status": "Enabled"},
        )

        log.info("Created S3 bucket: s3://%s", bucket)

    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        log.info("S3 bucket already exists: s3://%s", bucket)


def main():
    parser = argparse.ArgumentParser(description="Bootstrap HN ETL AWS infrastructure")
    parser.add_argument("--bucket",       default="hn-data-lake")
    parser.add_argument("--prefix",       default="raw/stories")
    parser.add_argument("--region",       default="us-east-1")
    parser.add_argument("--db-name",      default="hn_data_lake")
    parser.add_argument("--crawler-name", default="hn-stories-crawler")
    parser.add_argument("--role-name",    default="hn-glue-crawler-role")
    args = parser.parse_args()

    session    = boto3.Session(region_name=args.region)
    s3_client  = session.client("s3")
    iam_client = session.client("iam")
    glue_client = session.client("glue")

    log.info("=== HN ETL Infrastructure Bootstrap ===")
    log.info("Region: %s | Bucket: %s", args.region, args.bucket)

    create_s3_bucket(s3_client, args.bucket, args.region)
    role_arn = create_iam_role(iam_client, args.role_name, args.bucket)
    create_glue_database(glue_client, args.db_name)
    create_glue_crawler(
        glue_client,
        args.crawler_name,
        args.db_name,
        args.bucket,
        args.prefix,
        role_arn,
    )

    log.info("=== Bootstrap complete ===")
    log.info("Next: Set Airflow variables HN_S3_BUCKET=%s, HN_GLUE_CRAWLER=%s", args.bucket, args.crawler_name)


if __name__ == "__main__":
    main()
