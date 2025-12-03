import json
import boto3
import awswrangler as wr
import pandas as pd
from urllib.parse import unquote_plus
from datetime import datetime
import logging
import io

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

BUCKET = "asgp-yt-etl"           
RAW_PREFIX = "raw/"               
PROCESSED_PREFIX = "processed/"   

def load_category_mapping(country: str) -> dict:
    json_key = f"{RAW_PREFIX}{country}_category_id.json"
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=json_key)
        data = json.loads(obj['Body'].read())
        mapping = {str(item["id"]): item["snippet"]["title"] for item in data.get("items", [])}
        logger.info(f"Loaded {len(mapping)} category mappings for {country}")
        return mapping
    except Exception as e:
        logger.warning(f"Category JSON not found for {country}: {e}")
        return {}

def lambda_handler(event, context):
    logger.info("YouTube ETL Lambda started...")
    #extract countires from csv
    for record in event.get('Records', []):
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        filename = key.split("/")[-1]
        logger.info(f"Processing file: s3://{bucket}/{key}")

        if not filename.lower().endswith(".csv"):
            logger.info(f"Skipping non-CSV file: {filename}")
            continue

        country = filename[:2].upper()
        logger.info(f"Detected country: {country}")

        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            raw_bytes = obj['Body'].read()
            
            encodings_to_try = ['utf-8', 'cp949', 'euc-kr', 'latin-1', 'cp1252', 'iso-8859-1'] # multi encode tried casue utf-8 did not work
            df = None
            
            for encoding in encodings_to_try:
                try:
                    df = pd.read_csv(io.BytesIO(raw_bytes), encoding=encoding)
                    logger.info(f"✔ Loaded {len(df)} rows using {encoding} encoding")
                    break
                except (UnicodeDecodeError, LookupError):
                    continue
            
            #  use latin-1
            if df is None:
                df = pd.read_csv(io.BytesIO(raw_bytes), encoding='latin-1', errors='replace')
                logger.warning(f"⚠ Used latin-1 with error replacement for {filename}")
                
        except Exception as e:
            logger.error(f"Error reading CSV {filename}: {e}")
            continue

        if df.empty:
            logger.warning(f"No data in CSV {filename}, skipping write.")
            continue

        df.columns = [c.strip().replace(" ", "_").lower() for c in df.columns]

        if "category_id" in df.columns:
            df["category_id"] = df["category_id"].astype(str)

        
        mapping = load_category_mapping(country)
        df["category_name"] = df["category_id"].map(mapping).fillna("Unknown")

        # split
        df["country"] = country
        df["year"] = datetime.utcnow().year
        df["month"] = datetime.utcnow().month

        # Parquet 
        output_path = f"s3://{BUCKET}/{PROCESSED_PREFIX}"
        try:
            wr.s3.to_parquet(
                df=df,
                path=output_path,
                dataset=True,
                partition_cols=["country", "year", "month"],
                compression="snappy",
                mode="append"
            )
            logger.info(f"CSV processed and written to {output_path}")
        except Exception as e:
            logger.error(f"Error writing Parquet for {filename}: {e}")

    logger.info("ETL Lambda finished successfully.")
    return {"status": "success"}
