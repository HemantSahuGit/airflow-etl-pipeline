-- =============================================================
-- Task: Auto-trigger merge when stream has new data (every 5 min)
-- =============================================================
CREATE OR REPLACE TASK AIRFLOW_ETL.SILVER.TASK_MERGE_YOUTUBE_TRENDING_VIDEOS
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    COMMENT = 'Scheduled task that checks the bronze stream every 5 minutes and triggers the merge procedure when new data is available.'
    WHEN SYSTEM$STREAM_HAS_DATA('AIRFLOW_ETL.BRONZE.YOUTUBE_TRENDING_VIDEOS_STREAM')
AS
    CALL AIRFLOW_ETL.SILVER.SP_MERGE_YOUTUBE_TRENDING_VIDEOS();

-- Enable the task (uncomment to activate)
-- ALTER TASK AIRFLOW_ETL.SILVER.TASK_MERGE_YOUTUBE_TRENDING_VIDEOS RESUME;


-- =============================================================
-- Task: Auto-trigger categories merge when stream has new data
-- =============================================================
CREATE OR REPLACE TASK AIRFLOW_ETL.SILVER.TASK_MERGE_YOUTUBE_CATEGORIES
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    COMMENT = 'Scheduled task that checks the bronze categories stream every 5 minutes and triggers the merge procedure when new data is available.'
    WHEN SYSTEM$STREAM_HAS_DATA('AIRFLOW_ETL.BRONZE.YOUTUBE_CATEGORIES_STREAM')
AS
    CALL AIRFLOW_ETL.SILVER.SP_MERGE_YOUTUBE_CATEGORIES();

-- Enable the task (uncomment to activate)
-- ALTER TASK AIRFLOW_ETL.SILVER.TASK_MERGE_YOUTUBE_CATEGORIES RESUME;