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

### Architecture
<img width="1067" height="694" alt="image" src="https://github.com/user-attachments/assets/29c0eeed-aa91-49a3-b995-9fc99776fe2d" />

### Flow 
*User Upload → S3 Raw → Lambda → S3 Processed → Glue Crawler → Glue Catalog → Athena*

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


# Queries and Results
1) #### Average Data by country
   <img width="1305" height="443" alt="image" src="https://github.com/user-attachments/assets/c2ad6369-2375-445e-9ac6-df8ee9c1536b" />

3) #### Highest Like/View Ratio 

| video_id | title | channel_title | views | likes | like_rate | country |
|----------|-------|---------------|-------|-------|-----------|---------|
| ODjB1sL_D2E | REGALO ESTA MOTO CROSS POR NAVIDAD ESPECIAL 5M !!! Makiman | Makiman131 | 335259 | 131443 | 0.392064046 | MX |
| TyYV_h2bXM0 | OnePlus 6 Top Features and GIVEAWAY ðŸ”¥- OnePlus 6 Avengers Edition Giveaway!!ðŸ”¥ | Technical Guruji | 840727 | 321088 | 0.381917079 | IN |
| c94AB4PGucY | PLANETE FOOT | LA COUPE DE KURZAWA... ! | Pauleta | 121091 | 45970 | 0.379631847 | FR |
| F81wlI9iEpw | Cantando Comentarios | JuegaGerman | 476851 | 178958 | 0.375291234 | MX |
| uKmx6OnbuN0 | Oneplus 5T Lava Red Unboxing and Giveaway ðŸ”¥ðŸ”¥ðŸ”¥ Valentine's Gift!! | Technical Guruji | 335058 | 122284 | 0.364963678 | IN |
| NafpiJjOI-A | TOP 10 EL HUMANO ES RETRASADO Y NO TIENE CURA PARTE 11 - 8cho | 8cho | 1002395 | 342529 | 0.341710603 | MX |
| TyYV_h2bXM0 | OnePlus 6 Top Features and GIVEAWAY ðŸ”¥- OnePlus 6 Avengers Edition Giveaway!!ðŸ”¥ | Technical Guruji | 1049339 | 355742 | 0.339015323 | IN |
| IlkBLwl4aBM | Shawn Mendes In My Blood (Audio) | Shawn Mendes | 606265 | 204588 | 0.337456393 | MX |
| IlkBLwl4aBM | Shawn Mendes In My Blood (Audio) | Shawn Mendes | 606265 | 204538 | 0.337373921 | DE |
| 1hHvH8xQDKA | OnePlus 5T Star Wars Limited Edition Unboxing and First Look *GIVEAWAY* | Technical Guruji | 446587 | 148132 | 0.331697967 | IN |
| N4vnpOznw4Q | CÃƒÂ³mo hacer un documental especial | ELVISA | ElvisaYomastercard | 107137 | 35357 | 0.330016708 | MX |
| cKbl19xeSIQ | Niall Horan, Maren Morris - Seeing Blind (Acoustic) | NiallHoranVEVO | 104776 | 34550 | 0.329751088 | MX |
| Fp1MOsZKySQ | ROAST YOURSELF CHALLENGE - KILLADAMENTE | Killadamente | 288918 | 94382 | 0.326674004 | MX |
| c94AB4PGucY | PLANETE FOOT | LA COUPE DE KURZAWA... ! | Pauleta | 194343 | 62687 | 0.322558569 | FR |

3) #### Most trending catergories
   <img width="997" height="516" alt="image" src="https://github.com/user-attachments/assets/df45821f-8903-42ef-b6de-dd5f8f40451e" />

4) #### Catergory wise engagements
   <img width="1247" height="541" alt="image" src="https://github.com/user-attachments/assets/594f460f-63f0-4abd-b066-14eb5ead1109" />
   
5) #### Channels with highest views
   <img width="1288" height="530" alt="image" src="https://github.com/user-attachments/assets/d1039eed-349e-48ea-ab70-e83b4cfaf2e5" />

   
6) #### Trending category by country
   <img width="1181" height="415" alt="image" src="https://github.com/user-attachments/assets/af5cccd9-afd7-4741-b241-80d4a6b683e2" />

7) #### Channel and respective category with highest engagement with channel
   <img width="1283" height="365" alt="image" src="https://github.com/user-attachments/assets/24a0ec92-73cb-4356-b6e9-5ae96f5e4db7" />
   
8) #### New Trending videos
   <img width="1240" height="506" alt="image" src="https://github.com/user-attachments/assets/14e8e189-a96f-4caf-9cfc-b9ffa622b8e6" />
   
9) #### Low view but high engagement(like)
    <img width="957" height="515" alt="image" src="https://github.com/user-attachments/assets/193e6de4-114d-48c6-848b-e7bdb2369a2b" />


