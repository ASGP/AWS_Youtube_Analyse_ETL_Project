#  YouTube Trending Data ETL Pipeline on AWS
## A cloud-native data engineering project using S3, Lambda, Glue, Athena & Parquet

### Project Overview

This project implements a serverless ETL pipeline on AWS to process, transform, and analyze YouTube Trending Video Data across countries.
It demonstrates real-world cloud data engineering concepts such as:

- S3-based data lake design
- Event-driven ETL using AWS Lambda
- Schema discovery with AWS Glue
- Querying large datasets using Amazon Athena
- Building visual analytics (Grafana / Athena dashboards)
- IAM security, permissions, and serverless architecture

### Technologies Used

#### AWS Services
- Amazon S3 – Data lake storage
- AWS Lambda – ETL transformation to Parquet
- AWS Glue – Crawlers + Data Catalog
- Amazon Athena – Query engine
- Amazon Managed Grafana – Dashboards
- CloudWatch – Logs + monitoring
- IAM – Roles & permissions

#### Languages & Libraries
- Python 3.10
- AWS Wrangler (awswrangler)
- Pandas
- Boto3
- SQL (Athena)

### ETL Workflow Summary

- Raw YouTube CSV + JSON files uploaded to S3 → `raw/`
- S3 Event Trigger invokes AWS Lambda
- Lambda:
  - Detects country from filename
  - Reads corresponding CSV + JSON category mapping file
  - Cleans and normalizes data (dates, tags, missing values)
  - Adds metadata fields (country, load timestamp)
  - Converts data to optimized Parquet format
  - Writes outputs to in S3
  - Logs ETL details to CloudWatch
- AWS Glue Crawler updates Athena tables
- Athena queries the processed Parquet data
