
create schema if not exists bronze;
create schema if not exists silver;
create schema if not exists gold;

-- =============================================================
-- DDL: Silver layer table for flattened YouTube trending videos
-- =============================================================
CREATE TABLE IF NOT EXISTS AIRFLOW_ETL.SILVER.YOUTUBE_TRENDING_VIDEOS (
    ingestion_id         STRING    COMMENT 'Unique identifier for each data ingestion batch from the Airflow pipeline',
    region               STRING    COMMENT 'ISO 3166-1 alpha-2 country code representing the region where the video is trending',
    song_id              STRING    COMMENT 'YouTube video ID uniquely identifying the trending video',
    song_etag            STRING    COMMENT 'Entity tag for cache validation of the video resource',
    song_kind            STRING    COMMENT 'YouTube API resource type identifier (e.g., youtube#video)',
    song_tags            STRING    COMMENT 'Comma-separated tags assigned to the video by the uploader',
    title                STRING    COMMENT 'Title of the YouTube video as displayed on the platform',
    channel_id           STRING    COMMENT 'Unique YouTube channel ID that uploaded the video',
    category_id          STRING    COMMENT 'YouTube video category ID (maps to categories like Music, Entertainment, etc.)',
    thumbnail_details    STRING    COMMENT 'JSON containing thumbnail image URLs at various resolutions (default, medium, high)',
    song_desc            STRING    COMMENT 'Full description text of the video provided by the uploader',
    song_published_dt    TIMESTAMP COMMENT 'Date and time when the video was originally published on YouTube',
    channel_title        STRING    COMMENT 'Display name of the YouTube channel that owns the video',
    song_default_language STRING   COMMENT 'Default audio language of the video in BCP-47 format (e.g., en, hi, es)',
    song_like_count      INTEGER   COMMENT 'Total number of likes on the video at the time of ingestion',
    song_view_count      INTEGER   COMMENT 'Total number of views on the video at the time of ingestion',
    song_comment_count   INTEGER   COMMENT 'Total number of comments on the video at the time of ingestion',
    song_favourite_count INTEGER   COMMENT 'Number of times users have marked this video as a favourite',
    song_duration        STRING    COMMENT 'Video duration in ISO 8601 format (e.g., PT4M13S = 4 minutes 13 seconds)',
    song_dimension       STRING    COMMENT 'Video dimension format: 2d for standard or 3d for stereoscopic',
    song_definition      STRING    COMMENT 'Video quality definition: hd (high definition) or sd (standard definition)',
    search_etag          STRING    COMMENT 'Entity tag of the top-level YouTube API search response for cache validation',
    response_type        STRING    COMMENT 'YouTube API response resource type (e.g., youtube#videoListResponse)',
    ingestion_time       TIMESTAMP COMMENT 'Timestamp when the record was ingested by the Airflow pipeline'
)
COMMENT = 'Silver layer table containing flattened and typed YouTube trending video data. Sourced from bronze layer raw JSON responses via LATERAL FLATTEN on the items array. Each row represents one trending video per region per ingestion batch.';

-- =============================================================
-- DDL: Silver layer table for YouTube categories
-- =============================================================
CREATE TABLE IF NOT EXISTS AIRFLOW_ETL.SILVER.YOUTUBE_CATEGORIES (
    ingestion_id        STRING    COMMENT 'Unique identifier for each data ingestion batch from the Airflow pipeline',
    region              STRING    COMMENT 'ISO 3166-1 alpha-2 country code representing the region for the category list',
    category_id         INTEGER   COMMENT 'YouTube category ID (numeric identifier for video categories)',
    category_etag       STRING    COMMENT 'Entity tag for cache validation of the category resource',
    category_kind       STRING    COMMENT 'YouTube API resource type identifier (e.g., youtube#videoCategory)',
    category_title      STRING    COMMENT 'Display name of the category (e.g., Music, Entertainment, Sports)',
    category_channel_id STRING    COMMENT 'Channel ID associated with the category',
    category_assignable BOOLEAN   COMMENT 'Whether videos can be assigned to this category by uploaders',
    search_etag         STRING    COMMENT 'Entity tag of the top-level YouTube API response for cache validation',
    response_type       STRING    COMMENT 'YouTube API response resource type (e.g., youtube#videoCategoryListResponse)',
    ingestion_time      TIMESTAMP COMMENT 'Timestamp when the record was ingested by the Airflow pipeline'
)
COMMENT = 'Silver layer table containing flattened YouTube video categories. Sourced from bronze layer raw JSON responses via LATERAL FLATTEN on the items array. Each row represents one category per region per ingestion batch.';

-- =============================================================
-- DDL: ETL log table for tracking procedure executions
-- =============================================================
CREATE TABLE IF NOT EXISTS AIRFLOW_ETL.SILVER.ETL_PROCESS_LOG (
    log_id          NUMBER AUTOINCREMENT COMMENT 'Auto-generated unique log entry identifier',
    procedure_name  STRING  COMMENT 'Name of the stored procedure that was executed',
    status          STRING  COMMENT 'Execution status: SUCCESS, FAILED, or RUNNING',
    rows_inserted   INTEGER COMMENT 'Number of rows inserted during the merge',
    rows_updated    INTEGER COMMENT 'Number of rows updated during the merge',
    error_message   STRING  COMMENT 'Error details if the procedure failed',
    started_at      TIMESTAMP COMMENT 'Timestamp when the procedure execution began',
    completed_at    TIMESTAMP COMMENT 'Timestamp when the procedure execution completed'
)
COMMENT = 'Audit log table capturing execution details for all silver layer ETL procedures.';