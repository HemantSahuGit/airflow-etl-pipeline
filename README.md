# airflow-etl-pipeline

## YouTube Data API Ingestion DAG

This Airflow DAG ingests trending video data from YouTube API and stores it in PostgreSQL.

---

## Setup Instructions (UI Approach)

### 1. Start Docker Compose

```bash
docker compose up -d
```

This will:
- Start Airflow services (scheduler, worker, API server)
- Start PostgreSQL databases (Airflow metadata + project data)
- Start Redis (message broker)
- Install `google-api-python-client` package (from `.env`)

### 2. Access Airflow Web UI

Open: `http://localhost:8080`

Login with credentials:
- Username: `airflow`
- Password: `airflow`

### 3. Create Airflow Connection

1. Go to `Admin` → `Connections`
2. Click `+` (Create New Connection)
3. Enter the following:
   - **Connection Id**: `postgres_local`
   - **Connection Type**: `Postgres`
   - **Host**: `postgres-data`
   - **Port**: `5432`
   - **Schema**: `project_db`
   - **Login**: `project`
   - **Password**: `project`
4. Click **Save**

### 4. Create Airflow Variables

1. Go to `Admin` → `Variables`
2. Create each variable by clicking `+`:

| Variable Name | Value | Notes |
|---|---|---|
| `YOUTUBE_API_KEY` | your-api-key | Get from [Google Cloud Console](https://console.cloud.google.com/) |
| `YOUTUBE_REGIONS` | `US,GB,CA,DE,FR,IN,JP,KR,MX,RU` | Comma-separated region codes |
| `POSTGRES_CONN_ID` | `postgres_local` | Must match the connection ID above |

### 5. Verify the DAG

1. Go to the **DAGs** view
2. Look for `youtube_bronze_layer_ingestion`
3. The DAG should be visible and ready to trigger

### 6. Test the DAG (Optional)

1. Click on the DAG name
2. Click **Trigger DAG** (play button)
3. Monitor the execution in the logs

---

## Reference: .env Configuration (Optional)

If you want to use environment variables instead of the UI, see `.env.example` for all available options.

To use `.env` approach:
1. Copy `.env.example` contents to `.env`
2. Uncomment and fill in the variables
3. Restart Docker Compose:
   ```bash
   docker compose down
   docker compose up -d
   ```

---

## Project Structure

```
.
├── dags/
│   └── Step01_youtube_api_ingestion_data.py   # YouTube ingestion DAG
├── docker-compose.yaml                        # Airflow + Postgres + Redis setup
├── .env                                       # Environment variables (keep minimal)
├── .env.example                               # Configuration template
└── config/
    └── airflow.cfg                            # Airflow configuration
```

---

## Troubleshooting

### DAG not visible
- Ensure `dags/Step01_youtube_api_ingestion_data.py` file exists
- Restart the scheduler: `docker compose restart airflow-scheduler`

### Connection error
- Verify connection ID is `postgres_local`
- Verify host is `postgres-data` (not `localhost`)
- Check credentials: user `project`, password `project`

### Missing google-api-python-client
- Check Docker logs: `docker compose logs airflow-apiserver`
- Restart containers: `docker compose down && docker compose up -d`
