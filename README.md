# NYC TLC High Volume FHV Data Pipeline

End-to-end data pipeline for processing Uber and Lyft trip records from the NYC Taxi and Limousine Commission dataset (2021–2026), built on AWS.

## Stack

- **Ingestion**: AWS Lambda (serverless, streaming multipart upload)
- **Storage**: AWS S3 (partitioned by year/month)
- **ETL**: Python / Pandas
- **Processing**: Apache Spark 3.5 (PySpark)
- **Visualization**: Streamlit + Plotly
- **CI/CD**: GitHub Actions (self-hosted runner)

## Architecture
```
NYC TLC CDN -> Lambda -> S3 raw/
                              |
                         Pandas ETL -> S3 processed/
                                              |
                                       PySpark -> S3 output/
                                                       |
                                               Streamlit Dashboard
```

## Dataset

Source: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)  
File pattern: `fhvhv_tripdata_YYYY-MM.parquet`  
CDN: `https://d37ci6vzurychx.cloudfront.net/trip-data/`  
Coverage: January 2021 to March 2026 (High Volume FHV only)

Key columns: `hvfhs_license_num` (HV0003=Uber, HV0005=Lyft), `pickup_datetime`, `PULocationID`, `DOLocationID`, `base_passenger_fare`, `tips`, `trip_miles`

## Setup

### Requirements

- Python 3.12
- Java 11 (required by Spark)
- AWS CLI configured
- Docker (for dashboard)

### Install dependencies
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Environment variables

Create a `.env` file in the project root:
```
BUCKET=your-s3-bucket-name
LAMBDA_ROLE_ARN=arn:aws:iam::ACCOUNT_ID:role/ROLE_NAME
AWS_REGION=us-west-1
```

## Running the Pipeline
```bash
# Activate environment
source venv/bin/activate
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Run interactive orchestrator (prompts for year and month)
python3 src/run_pipeline.py

# Run Spark on already-processed data
python3 src/03_spark_pipeline.py --year 2024 --month 1

# Start dashboard
docker compose -f dashboard/docker-compose.yml up -d
# Open: http://localhost:8501
```

## Pipeline Stages

| Stage | Script | Description |
|-------|--------|-------------|
| Ingestion | `lambda_ingesta.py` | Downloads Parquet from TLC CDN via streaming multipart upload to S3. Logs metadata to `metadata/ingesta_log.json`. Falls back to direct download if Lambda invocation fails. |
| ETL | `02_etl.py` | Cleans nulls and duplicates, maps license numbers to platform names, computes derived columns, joins with taxi zone lookup. Processes in 1M-row chunks to stay under 400MB RAM. |
| Spark | `03_spark_pipeline.py` | Broadcast joins zone lookup, computes four aggregation tables: platform by year, demand by hour, borough stats, top zones by month using RANK() window function. |
| Dashboard | `04_dashboard.py` | Reads Spark output from S3, renders KPIs and five Plotly charts across three tabs. |

## Key Results (January 2024)

- Uber: 14.4M trips, $366M revenue, $25.36 avg fare
- Lyft: 5.2M trips, $125M revenue, $24.10 avg fare
- Top pickup zone: JFK Airport (374K trips)

## Infrastructure

- EC2: t3.medium, Ubuntu 24, us-west-1
- S3 bucket: partitioned as `raw/year=YYYY/month=MM/`
- IAM: least-privilege role scoped to the S3 bucket
- Dashboard: Docker container, port 8501, auto-deployed via GitHub Actions on push to `dashboard/`



Christian Rubi — Data Academy Xideral — 2025
