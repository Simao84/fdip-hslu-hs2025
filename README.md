# Flight Delay Intelligence Platform (FDIP)
## HSLU Data Warehouse and Data Lake — HS 2025
**Author:** Simao Garcia  
**AWS Account:** 604415812723 | **Region:** us-east-1

## Overview
Automated AWS data lake and data warehouse analysing 7.4 million 
US domestic flight records (Feb 2025 – Feb 2026) to answer 
7 business intelligence questions about flight delays.

## Architecture
- **Bronze:** s3://bronze-604415812723/ — raw JSON and CSV
- **Silver:** s3://silver-604415812723/ — clean Parquet
- **Gold:** s3://gold-604415812723/ — analytics Parquet
- **Query engine:** Amazon Athena + AWS Glue Data Catalog
- **Visualisation:** Tableau

## Pipeline Schedule
| Time (Zurich) | Service | Action |
|---|---|---|
| 00:00 | AWS Lambda | API ingestion to bronze |
| 00:30 | AWS Glue | bronze-to-silver ETL |
| 01:00 | AWS Glue | silver-to-gold ETL |
| 01:15 | Glue Crawler | update fdip_catalog |

## Repository Structure
- `lambda/` — AWS Lambda ingestion functions
- `glue/` — AWS Glue PySpark ETL jobs
- `athena/` — Athena SQL view definitions
- `notebooks/` — Jupyter notebook with full pipeline

## Data Sources
1. AviationStack API — live flight data (100 records/day)
2. NOAA METAR API — weather observations (20 airports)
3. BTS On-Time Performance CSV — 13 months historical data

## Key Findings
- Early morning flights (6am) carry near-zero delay risk
- Late evening flights (after 7pm) average delays in hours
- Carrier operations cause 65% of all delay minutes
- Hawaiian Airlines is the best carrier at 7.9 min average
- ACK→ORD (SkyWest) is the most delayed route at 93 min average

## Project Report
[Download Full Report (PDF)](docs/FDIP_Report_2026.pdf)

