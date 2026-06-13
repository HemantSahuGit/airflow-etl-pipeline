# airflow-etl-pipeline

## DAGs Overview

This project contains two main ETL DAGs:

1. **YouTube Data API Ingestion** (`youtube_bronze_layer_ingestion`) — Ingests trending video data from YouTube API to PostgreSQL
2. **PostgreSQL to Snowflake Sync** (`postgres_to_snowflake_incremental_sync`) — Incremental data sync with watermark tracking

---

## Setup Instructions

### 1. Start Docker Compose

```bash
docker compose down  # If already running
docker compose up -d
```

This will:
- Start Airflow services (scheduler, worker, API server)
- Start PostgreSQL databases (Airflow metadata + project data)
- Start Redis (message broker)
- Install all required packages (YouTube API client, Snowflake connector)

### 2. Access Airflow Web UI

Open: `http://localhost:8080`

Login with credentials:
- Username: `airflow`
- Password: `airflow`

---

## DAG 1: YouTube Data Ingestion

### Setup

1. **Create Airflow Connection** (`postgres_local`):
   - Go to `Admin` → `Connections`
   - Click `+` and enter:
     - **Connection Id**: `postgres_local`
     - **Connection Type**: `Postgres`
     - **Host**: `postgres-data`
     - **Port**: `5432`
     - **Schema**: `project_db`
     - **Login**: `project`
     - **Password**: `project`

2. **Create Airflow Variables**:
   - Go to `Admin` → `Variables`
   - Create these variables:

| Variable Name | Value | Notes |
|---|---|---|
| `YOUTUBE_API_KEY` | your-api-key | Get from [Google Cloud Console](https://console.cloud.google.com/) |
| `YOUTUBE_REGIONS` | `US,GB,CA,DE,FR,IN,JP,KR,MX,RU` | Comma-separated region codes |
| `POSTGRES_CONN_ID` | `postgres_local` | Must match connection ID |

3. **Verify DAG**:
   - Go to DAGs view
   - Look for `youtube_bronze_layer_ingestion`
   - Ready to trigger!

---

## DAG 2: PostgreSQL to Snowflake Incremental Sync

### Understanding Watermarks

A **watermark** is a timestamp that tracks the last successful sync. The DAG:
1. Creates a `sync_watermark` table in Snowflake
2. On each run, checks the last watermark timestamp for each table
3. Fetches only NEW records (created after the watermark)
4. Syncs them to Snowflake
5. Updates the watermark with the current sync time

This ensures **incremental, efficient data transfer** without duplicates.

### Setup

1. **Create Snowflake Connection** (`snowflake_default`):
   - Go to `Admin` → `Connections`
   - Click `+` and enter:
     - **Connection Id**: `snowflake_default`
     - **Connection Type**: `Snowflake`
     - **Account**: `xy12345` (your Snowflake account ID)
     - **Warehouse**: `compute_wh` (or your warehouse name)
     - **Database**: `analytics` (or your database)
     - **Schema**: `bronze` (or your schema)
     - **Login**: `your_snowflake_username`
     - **Password**: `your_snowflake_password`
     - **Role**: `transformer` (or your role)

2. **Create Airflow Variables**:
   - Go to `Admin` → `Variables`
   - Create these variables:

| Variable Name | Value | Notes |
|---|---|---|
| `SNOWFLAKE_CONN_ID` | `snowflake_default` | Must match connection ID |
| `SNOWFLAKE_DB` | `analytics` | Your Snowflake database |
| `SNOWFLAKE_SCHEMA` | `bronze` | Schema for watermark table |
| `SYNC_TABLES` | `youtube_bronze_trending_videos,youtube_bronze_categories` | Tables to sync from PostgreSQL |
| `POSTGRES_CONN_ID` | `postgres_local` | Connection to PostgreSQL |

3. **Verify DAG**:
   - Go to DAGs view
   - Look for `postgres_to_snowflake_incremental_sync`
   - Ready to trigger!

### What the DAG Does

1. **Creates watermark table** in Snowflake (if not exists)
2. **Gets table list** from SYNC_TABLES variable
3. **Fetches last watermark** for each table
4. **Syncs incremental data**:
   - Queries PostgreSQL for records since last watermark
   - Inserts into Snowflake
5. **Updates watermark** with current sync timestamp
6. **Logs sync summary**

### Example Watermark Table

```
| id | table_name | last_sync_time | current_sync_time | records_synced | status |
|----|-----------|----------------|-------------------|----------------|--------|
| 1  | youtube_bronze_trending_videos | 2026-06-13 10:00:00 | 2026-06-13 12:00:00 | 250 | SUCCESS |
| 2  | youtube_bronze_categories | 2026-06-13 10:00:00 | 2026-06-13 12:00:00 | 15 | SUCCESS |
```

---

## Project Structure

```
.
├── dags/
│   ├── Step01_youtube_api_ingestion_data.py      # YouTube ingestion DAG
│   └── Step02_copy_files_pg_to_snow.py           # Postgres to Snowflake DAG
├── docker-compose.yaml                          # Airflow + Postgres + Redis setup
├── .env                                         # Environment variables
├── .env.example                                 # Configuration template
└── config/
    └── airflow.cfg                              # Airflow configuration
```

---

## Troubleshooting

### DAG not visible
- Ensure DAG file is in `dags/` folder
- Restart scheduler: `docker compose restart airflow-scheduler airflow-dag-processor`

### Connection error (Postgres)
- Verify connection ID is `postgres_local`
- Host should be `postgres-data` (not `localhost`)
- Credentials: user `project`, password `project`

### Connection error (Snowflake)
- Verify account ID format (e.g., `xy12345`)
- Check username and password
- Ensure warehouse and database exist
- Verify role has necessary permissions

### Missing packages
- Check Docker logs: `docker compose logs airflow-apiserver`
- Restart containers: `docker compose down && docker compose up -d`

### Watermark not updating
- Check if table is in SYNC_TABLES variable
- Verify Snowflake connection works
- Check Airflow logs for SQL errors

---

## Restarting After Configuration

After updating `.env` or adding connections/variables:

```bash
docker compose down
docker compose up -d
docker compose restart airflow-scheduler airflow-dag-processor
```
