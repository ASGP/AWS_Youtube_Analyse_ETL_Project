import json
import boto3
import awswrangler as wr
import pandas as pd
from urllib.parse import unquote_plus
from datetime import datetime
import logging
import io

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# S3 client
s3 = boto3.client("s3")

# Constants
BUCKET = "asgp-yt-etl"
RAW_PREFIX = "raw/"
PROCESSED_PREFIX = "processed/"


def load_category_mapping(country: str) -> dict:
    """
    Load category mapping JSON (e.g., US_category_id.json)
    Returns: {category_id: category_name}
    """
    json_key = f"{RAW_PREFIX}{country}_category_id.json"

    try:
        obj = s3.get_object(Bucket=BUCKET, Key=json_key)
        data = json.loads(obj["Body"].read())

        mapping = {
            str(item["id"]): item["snippet"]["title"]
            for item in data.get("items", [])
        }

        logger.info(f"Loaded {len(mapping)} category mappings for {country}")
        return mapping

    except Exception as e:
        logger.warning(f"Category JSON not found for {country}: {e}")
        return {}


def lambda_handler(event, context):
    logger.info("YouTube ETL Lambda started")

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        filename = key.split("/")[-1]

        logger.info(f"Processing file: s3://{bucket}/{key}")

        
        if not filename.lower().endswith(".csv"):
            logger.info(f"Skipping non-CSV file: {filename}")
            continue

        # detect country
        country = filename[:2].upper()
        logger.info(f"Detected country: {country}")

        # use multiple encoding casue utf-8 does not not work
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            raw_bytes = obj["Body"].read()

            encodings_to_try = [
                "utf-8",
                "cp949",
                "euc-kr",
                "latin-1",
                "cp1252",
                "iso-8859-1",
            ]

            df = None
            for enc in encodings_to_try:
                try:
                    df = pd.read_csv(io.BytesIO(raw_bytes), encoding=enc)
                    logger.info(f"Loaded CSV using encoding {enc}")
                    break
                except Exception:
                    continue

            if df is None:
                df = pd.read_csv(
                    io.BytesIO(raw_bytes),
                    encoding="latin-1",
                    errors="replace",
                )
                logger.warning("Used fallback encoding latin-1 with replacement")

        except Exception as e:
            logger.error(f"Failed to load CSV {filename}: {e}")
            continue

        if df.empty:
            logger.warning(f"CSV {filename} is empty")
            continue

        # Standardize columns
        df.columns = [c.strip().replace(" ", "_").lower() for c in df.columns]

        if "category_id" in df.columns:
            df["category_id"] = df["category_id"].astype(str)

        # Load and apply category 
        mapping = load_category_mapping(country)
        df["category_name"] = df["category_id"].map(mapping).fillna("Unknown")

        # Remove duplicate rows
        df = df.drop_duplicates()

        # Drop rows missing essential fields
        required_cols = ["video_id", "title", "channel_title"]
        existing_required = [c for c in required_cols if c in df.columns]
        if existing_required:
            df = df.dropna(subset=existing_required)

        # Fill numeric columns with 0
        numeric_cols = ["views", "likes", "dislikes", "comment_count"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Fill text columns
        text_cols = ["description", "tags"]
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].fillna("")

        # category_id is not missing
        if "category_id" in df.columns:
            df["category_id"] = df["category_id"].fillna("unknown")

        # split
        now = datetime.utcnow()
        df["country"] = country
        df["year"] = now.year
        df["month"] = now.month

        # parquet
        output_path = f"s3://{BUCKET}/{PROCESSED_PREFIX}"

        try:
            wr.s3.to_parquet(
                df=df,
                path=output_path,
                dataset=True,
                partition_cols=["country", "year", "month"],
                compression="snappy",
                mode="append",
            )
            logger.info(f"Parquet written to {output_path}")

        except Exception as e:
            logger.error(f"Failed writing parquet for {filename}: {e}")

    logger.info("Lambda ETL finished successfully")
    return {"status": "success"}
