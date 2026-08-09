# PySpark: Transforming Data From Hive Distributed Warehouse To Kafka Events Format

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54) ![JSON](https://img.shields.io/badge/json-000000?style=for-the-badge&logo=json&logoColor=white) ![YAML](https://img.shields.io/badge/yaml-CB171E?style=for-the-badge&logo=yaml&logoColor=white) ![AWS Glue](https://img.shields.io/badge/AWS%20Glue%20Spark-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=FF9900) ![GitHub](https://img.shields.io/badge/github-%23121011.svg?style=for-the-badge&logo=github&logoColor=white) ![Git](https://img.shields.io/badge/git-%23F05033.svg?style=for-the-badge&logo=git&logoColor=white) ![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)

Welcome to the YouTube Analytics Data Pipeline project repository. This project covers an end-to-end serverless data pipeline on AWS that ingests YouTube Trending API data and Kaggle reference datasets, processes them using a Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold), executes data quality validation checks, and exposes analytics-ready tables for query execution via Amazon Athena. Designed as a portfolio project, it highlights production-grade CI/CD and data engineering best practices.
## 📌 Project Overview
this project focuses on:
- **End-to-End Medallion Architecture** - Ingesting raw JSON/CSV data (Bronze), cleaning and transforming into partitioned Parquet format (Silver), and aggregating analytics data models (Gold).
- **Serverless Orchestration** - State-machine coordination using AWS Step Functions to sequence Lambda ingestions, Glue PySpark jobs, and Data Quality checks.
- **Automated Quality Control** - Custom Lambda data validation enforcing schema constraints, null checks, and record counts prior to Gold layer promotion.
- **CI/CD Deployment** - Fully automated deployment pipelines via GitHub Actions that lint code, build deployment artifacts, update AWS Lambda functions, upload Glue ETL scripts, and update Step Function state machine definitions.

## 📑 Project Background
### A. Problem Statement
The Marketing and Content Strategy teams require daily insights into trending YouTube videos across various geographical regions to identify high-performing content categories, viral engagement metrics, and tag performance.
The challenges:
- **Unstructured & Raw Ingestion:** Data processing required manual script execution, leading to inconsistent daily updates and lack of automated error handling.
- **Manual Pipeline Execution:** Moreover, the increase of data demand from the newly developed downstream systems penalized the performance of the current data pipelines. Thus, making the legacy architecture un-scalable.
- **Lack of Governance & Quality Checks:** Malformed API responses and missing schema fields occasionally polluted downstream analytics dashboards, eroding trust in reporting metrics.

### B. Proposed System Architecture
<img width="1024" height="565" alt="image" src="https://github.com/user-attachments/assets/83d557c5-76e3-4352-9c44-bd1bef466eab" />
<br><br>

### C. Desired Output
1. **Bronze Layer (Raw Storage):** AWS Lambda fetches YouTube Trending API payload and stores raw JSON in s3://<bucket>/bronze/youtube_api/. Historical region reference datasets are loaded into s3://<bucket>/bronze/kaggle/.
2. **Silver Layer (Cleaned & Standardized):** An AWS Lambda function transforms incoming Bronze JSON objects into optimized Parquet format. AWS Glue PySpark jobs clean, cast data types, flatten nested structures, and partition datasets by region and category.
3. **Gold Layer (Analytics Ready):** AWS Glue PySpark aggregates key business metrics (engagement rates, view counts, category rankings) and writes business-ready Parquet tables.
4. **Data Quality Validation:** A dedicated Python Data Quality Lambda runs automated validation checks against the Silver/Gold tables. If a check fails, the pipeline halts and emits an alert via Amazon SNS.
5. **Analytics & Querying:** Amazon Athena serves as the serverless SQL query engine over the AWS Glue Data Catalog tables.

# Project Requirements and Resources

### Stack
1. **Python 3.11** — Primary language for AWS Lambda functions and Glue PySpark ETL scripts.
2. **AWS Lambda** — Serverless compute for API extraction, JSON-to-Parquet conversion, and Data Quality logic.
3. **AWS Glue (PySpark)** — Distributed data processing for Silver/Gold transformations and Glue Data Catalog indexing.
4. **AWS Step Functions** — JSON based State-machine orchestrator for end-to-end pipeline execution and error handling.
5. **Amazon S3** — Object storage hosting Bronze, Silver, and Gold data layers.
6. **Amazon Athena** — Serverless SQL engine for querying Silver and Gold datasets.
7. **Amazon SNS** — Simple Notification Service for pipeline success/failure notifications.
8. **GitHub Actions** — CI/CD automation runner for code validation, packaging, and AWS deployment.

### [Project Milestone & Guidelines](https://app.notion.com/p/Youtube-End-To-End-Analytics-Pipeline-Using-AWS-Services-3ae7d7cd41a680688600ed29239819b7?source=copy_link)
A comprehensive checkpoint in a project timeline that marks a major event, phase completion, or key deliverables.
<br><br>

### Project File Structure 

```bash
YoutubeAnalyticsPipeline/
├── .github/
│   └── workflows/
│       └── deploy.yml                       # Automated GitHub Actions CI/CD workflow
├── data_quality/
│   └── lambda_function.py                   # Data Quality validation Lambda function
├── glue_jobs/
│   ├── bronze_to_silver_statistics.py       # Glue PySpark job: Bronze to Silver cleaning
│   └── silver_to_gold_analytics.py          # Glue PySpark job: Silver to Gold aggregation
├── lambda_function/
│   ├── s3_bronzeJSON_to_silverparquet/
│   │   └── lambda_function.py               # Lambda function converting raw S3 JSON to Parquet
│   └── youtube_API_ingestion/
│       └── lambda_function.py               # Lambda function extracting raw YouTube API data
├── step_functions/
│   └── pipeline_orchestrations.json         # ASL definition for AWS Step Function state machine
├── .gitignore                               # Git exclusion definitions
├── pyproject.toml                           # Python project dependency configuration
├── README.md                                # Project documentation
└── uv.lock                                  # Dependency lock file (uv package manager)
```
<br>

### Version Control & Branching Strategy
This repository adopts a standard feature-branch workflow to emulate production engineering environments. Code changes undergo syntax validation and local testing before merging into development and master branches.

```txt
          master (Production Deployment via CI/CD)
                            ^
                            |
                       development
                            ^
                            |
          feature-changes (Local Dev & Testing)
```
<br>

### Automated CI/CD (GitHub Actions)
Deployments are fully automated using GitHub Actions. Upon pushing commits or merging pull requests to master, the workflow executes static code analysis, validates JSON state machine syntax, packages Python Lambda functions into .zip archives, syncs Glue scripts to S3, and updates AWS resource configurations via the AWS CLI.
```yaml
name: CI/CD Pipeline - YoutubeAnalyticsPipeline

on:
  push:
    branches:
      - master
  pull_request:
    branches:
      - master

permissions:
  contents: read

jobs:
  validate:
    name: Code Validation & Syntax Checks
    runs-on: ubuntu-latest
    steps:
      - name: Checkout Code
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: pip install uv

      - name: Validate Python Syntax
        run: |
          python -m py_compile data_quality/lambda_function.py
          python -m py_compile glue_jobs/*.py
          python -m py_compile lambda_function/s3_bronzeJSON_to_silverparquet/lambda_function.py
          python -m py_compile lambda_function/youtube_API_ingestion/lambda_function.py

      - name: Validate Step Function JSON Syntax
        run: |
          python -c "import json; json.load(open('step_functions/pipeline_orchestrations.json'))"

  deploy:
    name: Deploy Scripts to AWS Services
    needs: validate
    if: github.ref == 'refs/heads/master'
    runs-on: ubuntu-latest
    steps:
      - name: Checkout Code
        uses: actions/checkout@v4

      - name: Configure AWS Credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: ${{ vars.AWS_REGION }}

      - name: Deploy Glue Scripts to S3
        run: |
          aws s3 sync glue_jobs/ s3://${{ vars.GLUE_S3_BUCKET }}/scripts/glue_jobs/ \
            --exclude ".*" \
            --exclude "__MACOSX/*"

      - name: Deploy Data Quality Lambda
        run: |
          cd data_quality
          zip -j dq_lambda.zip lambda_function.py
          aws lambda update-function-code \
            --function-name ${{ vars.LAMBDA_DQ_NAME }} \
            --zip-file fileb://dq_lambda.zip
          cd ..

      - name: Deploy YouTube API Ingestion Lambda
        run: |
          cd lambda_function/youtube_API_ingestion
          zip -r ingestion_lambda.zip . -x "*.git*"
          aws lambda update-function-code \
            --function-name ${{ vars.LAMBDA_INGESTION_NAME }} \
            --zip-file fileb://ingestion_lambda.zip
          cd ../..

      - name: Deploy JSON to Parquet Lambda
        run: |
          cd lambda_function/s3_bronzeJSON_to_silverparquet
          zip -r parquet_lambda.zip . -x "*.git*"
          aws lambda update-function-code \
            --function-name ${{ vars.LAMBDA_PARQUET_NAME }} \
            --zip-file fileb://parquet_lambda.zip
          cd ../..

      - name: Deploy Step Functions Definition
        run: |
          aws stepfunctions update-state-machine \
            --state-machine-arn ${{ vars.STEP_FUNCTION_ARN }} \
            --definition file://step_functions/pipeline_orchestrations.json
```
<br>

### Credentials & IAM Security
To adhere to security best practices, sensitive parameters (such as AWS Access Keys, Account IDs, and API Tokens) are never committed to version control.
- **Local Development:** Environment variables are defined in a local .env file that is explicitly ignored by Git (.gitignore).
- **CI/CD Deployment:** Credentials and configuration parameters are injected at runtime via GitHub Actions Repository Secrets
  
# Data Formats & Transformation Schemas

### A. Raw Ingestion Format (Bronze JSON)
Raw API data fetched from YouTube Data API v3 lands in S3 as nested JSON objects:
```JSON
{
  "kind": "youtube#videoListResponse",
  "etag": "c361a145-d2fc-434e-a608-9688caa6d22e",
  "items": [
    {
      "kind": "youtube#video",
      "id": "ks334_sample_id",
      "snippet": {
        "publishedAt": "2026-08-01T12:00:00Z",
        "channelId": "UC_x5XG1OV2P6uZZ5FSM9Ttw",
        "title": "Trending Tech News & Innovations",
        "categoryId": "28",
        "tags": ["tech", "ai", "data engineering"]
      },
      "statistics": {
        "viewCount": "1250000",
        "likeCount": "85000",
        "commentCount": "3200"
      }
    }
  ]
}
```


### B. Analytical Output Format (Gold Layer / Athena)
The Silver & Gold processing jobs convert raw nested JSON structures into partitioned Parquet tables stored in S3 and cataloged in AWS Glue Data Catalog, enabling performant SQL queries via Amazon Athena:
```SQL
SELECT 
    category_id,
    SUM(CAST(view_count AS BIGINT)) AS total_views,
    SUM(CAST(like_count AS BIGINT)) AS total_likes,
    ROUND(SUM(CAST(like_count AS FLOAT)) / SUM(CAST(view_count AS FLOAT)) * 100, 2) AS engagement_rate_pct
FROM "youtube_analytics_gold"."fact_daily_video_stats"
WHERE region = 'US'
GROUP BY category_id
ORDER BY total_views DESC;
```

<br><br><br><br>
_License: Distributed under the MIT License. See [LICENSE](https://opensource.org/license/mit) for more information._

_Follow me on LinkedIn: https://www.linkedin.com/in/mohdnuriqhwan/_
