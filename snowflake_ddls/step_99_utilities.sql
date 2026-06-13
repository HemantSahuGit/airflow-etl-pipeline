-- =============================================================
-- Utility queries
-- =============================================================
-- Check if stream has pending data
-- SELECT SYSTEM$STREAM_HAS_DATA('AIRFLOW_ETL.BRONZE.YOUTUBE_TRENDING_VIDEOS_STREAM');

-- Preview stream contents (does not consume)
-- SELECT * FROM AIRFLOW_ETL.BRONZE.YOUTUBE_TRENDING_VIDEOS_STREAM LIMIT 10;

-- Check task execution history
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'TASK_MERGE_YOUTUBE_TRENDING_VIDEOS')) ORDER BY SCHEDULED_TIME DESC;

-- View ETL logs
-- SELECT * FROM AIRFLOW_ETL.SILVER.ETL_PROCESS_LOG ORDER BY started_at DESC;