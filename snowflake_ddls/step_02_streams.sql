-- =============================================================
-- Stream: Append-only stream on bronze trending videos table
-- =============================================================
CREATE OR REPLACE STREAM AIRFLOW_ETL.BRONZE.YOUTUBE_TRENDING_VIDEOS_STREAM
    ON TABLE AIRFLOW_ETL.BRONZE.YOUTUBE_BRONZE_TRENDING_VIDEOS
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE
    COMMENT = 'Append-only stream capturing new inserts into the bronze trending videos table. Consumed by the silver merge procedure.';


-- =============================================================
-- Stream: Append-only stream on bronze categories table
-- =============================================================
CREATE OR REPLACE STREAM AIRFLOW_ETL.BRONZE.YOUTUBE_CATEGORIES_STREAM
    ON TABLE AIRFLOW_ETL.BRONZE.YOUTUBE_BRONZE_CATEGORIES
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE
    COMMENT = 'Append-only stream capturing new inserts into the bronze categories table. Consumed by the silver merge procedure.';