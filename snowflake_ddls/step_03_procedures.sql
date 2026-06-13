-- =============================================================
-- Stored Procedure: Merge from stream to silver
-- =============================================================
CREATE OR REPLACE PROCEDURE AIRFLOW_ETL.SILVER.SP_MERGE_YOUTUBE_TRENDING_VIDEOS()
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Consumes new rows from the bronze stream and merges flattened YouTube trending video data into the silver layer. Matches on song_id + region + ingestion_id. Logs execution results to ETL_PROCESS_LOG.'
EXECUTE AS CALLER
AS
BEGIN
    LET v_start TIMESTAMP := CURRENT_TIMESTAMP();
    LET v_rows_inserted INTEGER := 0;

    -- Log start
    INSERT INTO AIRFLOW_ETL.SILVER.ETL_PROCESS_LOG
        (procedure_name, status, rows_inserted, rows_updated, error_message, started_at, completed_at)
    VALUES
        ('SP_MERGE_YOUTUBE_TRENDING_VIDEOS', 'RUNNING', 0, 0, NULL, :v_start, NULL);

    BEGIN
        -- Check if stream has data before processing
        IF (NOT SYSTEM$STREAM_HAS_DATA('AIRFLOW_ETL.BRONZE.YOUTUBE_TRENDING_VIDEOS_STREAM')) THEN
            UPDATE AIRFLOW_ETL.SILVER.ETL_PROCESS_LOG
            SET status = 'SUCCESS',
                rows_inserted = 0,
                error_message = 'No new data in stream',
                completed_at = CURRENT_TIMESTAMP()
            WHERE procedure_name = 'SP_MERGE_YOUTUBE_TRENDING_VIDEOS'
              AND status = 'RUNNING'
              AND started_at = :v_start;

            RETURN 'SUCCESS: No new data in stream. Skipped.';
        END IF;

        MERGE INTO AIRFLOW_ETL.SILVER.YOUTUBE_TRENDING_VIDEOS AS tgt
        USING (
            SELECT
                "ingestion_id" AS ingestion_id,
                "region" AS region,
                f.value:id::string AS song_id,
                f.value:etag::string AS song_etag,
                f.value:kind::string AS song_kind,
                f.value:snippet:tags::string AS song_tags,
                f.value:snippet:title::string AS title,
                f.value:snippet:channelId::string AS channel_id,
                f.value:snippet:categoryId::string AS category_id,
                f.value:snippet:thumbnails::string AS thumbnail_details,
                f.value:snippet:description::string AS song_desc,
                f.value:snippet:publishedAt::timestamp AS song_published_dt,
                f.value:snippet:channelTitle::string AS channel_title,
                f.value:snippet:defaultAudioLanguage::string AS song_default_language,
                f.value:statistics:likeCount::integer AS song_like_count,
                f.value:statistics:viewCount::integer AS song_view_count,
                f.value:statistics:commentCount::integer AS song_comment_count,
                f.value:statistics:favoriteCount::integer AS song_favourite_count,
                f.value:contentDetails:duration::string AS song_duration,
                f.value:contentDetails:dimension::string AS song_dimension,
                f.value:contentDetails:definition::string AS song_definition,
                parse_json("raw_json"::variant):etag::string AS search_etag,
                parse_json("raw_json"::variant):kind::string AS response_type,
                parse_json("raw_json"::variant):_pipeline_metadata:ingestion_timestamp::timestamp AS ingestion_time
            FROM AIRFLOW_ETL.BRONZE.YOUTUBE_TRENDING_VIDEOS_STREAM,
            LATERAL FLATTEN(parse_json("raw_json"::variant):items) AS f
        ) AS src
        ON  tgt.song_id = src.song_id
        AND tgt.region = src.region
        AND tgt.ingestion_id = src.ingestion_id
        WHEN MATCHED THEN UPDATE SET
            tgt.song_etag            = src.song_etag,
            tgt.song_kind            = src.song_kind,
            tgt.song_tags            = src.song_tags,
            tgt.title                = src.title,
            tgt.channel_id           = src.channel_id,
            tgt.category_id          = src.category_id,
            tgt.thumbnail_details    = src.thumbnail_details,
            tgt.song_desc            = src.song_desc,
            tgt.song_published_dt    = src.song_published_dt,
            tgt.channel_title        = src.channel_title,
            tgt.song_default_language = src.song_default_language,
            tgt.song_like_count      = src.song_like_count,
            tgt.song_view_count      = src.song_view_count,
            tgt.song_comment_count   = src.song_comment_count,
            tgt.song_favourite_count = src.song_favourite_count,
            tgt.song_duration        = src.song_duration,
            tgt.song_dimension       = src.song_dimension,
            tgt.song_definition      = src.song_definition,
            tgt.search_etag          = src.search_etag,
            tgt.response_type        = src.response_type,
            tgt.ingestion_time       = src.ingestion_time
        WHEN NOT MATCHED THEN INSERT (
            ingestion_id, region, song_id, song_etag, song_kind, song_tags, title,
            channel_id, category_id, thumbnail_details, song_desc, song_published_dt,
            channel_title, song_default_language, song_like_count, song_view_count,
            song_comment_count, song_favourite_count, song_duration, song_dimension,
            song_definition, search_etag, response_type, ingestion_time
        ) VALUES (
            src.ingestion_id, src.region, src.song_id, src.song_etag, src.song_kind,
            src.song_tags, src.title, src.channel_id, src.category_id, src.thumbnail_details,
            src.song_desc, src.song_published_dt, src.channel_title, src.song_default_language,
            src.song_like_count, src.song_view_count, src.song_comment_count,
            src.song_favourite_count, src.song_duration, src.song_dimension,
            src.song_definition, src.search_etag, src.response_type, src.ingestion_time
        );

        v_rows_inserted := SQLROWCOUNT;

        -- Update log with success
        UPDATE AIRFLOW_ETL.SILVER.ETL_PROCESS_LOG
        SET status = 'SUCCESS',
            rows_inserted = :v_rows_inserted,
            completed_at = CURRENT_TIMESTAMP()
        WHERE procedure_name = 'SP_MERGE_YOUTUBE_TRENDING_VIDEOS'
          AND status = 'RUNNING'
          AND started_at = :v_start;

        RETURN 'SUCCESS: Merged ' || :v_rows_inserted || ' rows from stream.';

    EXCEPTION
        WHEN OTHER THEN
            UPDATE AIRFLOW_ETL.SILVER.ETL_PROCESS_LOG
            SET status = 'FAILED',
                error_message = SQLERRM,
                completed_at = CURRENT_TIMESTAMP()
            WHERE procedure_name = 'SP_MERGE_YOUTUBE_TRENDING_VIDEOS'
              AND status = 'RUNNING'
              AND started_at = :v_start;

            RETURN 'FAILED: ' || SQLERRM;
    END;
END;


-- =============================================================
-- Stored Procedure: Merge YouTube categories from stream to silver
-- =============================================================
CREATE OR REPLACE PROCEDURE AIRFLOW_ETL.SILVER.SP_MERGE_YOUTUBE_CATEGORIES()
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Consumes new rows from the bronze categories stream and merges flattened data into the silver layer. Matches on category_id + region + ingestion_id. Logs execution results to ETL_PROCESS_LOG.'
EXECUTE AS CALLER
AS
BEGIN
    LET v_start TIMESTAMP := CURRENT_TIMESTAMP();
    LET v_rows_inserted INTEGER := 0;

    -- Log start
    INSERT INTO AIRFLOW_ETL.SILVER.ETL_PROCESS_LOG
        (procedure_name, status, rows_inserted, rows_updated, error_message, started_at, completed_at)
    VALUES
        ('SP_MERGE_YOUTUBE_CATEGORIES', 'RUNNING', 0, 0, NULL, :v_start, NULL);

    BEGIN
        -- Check if stream has data before processing
        IF (NOT SYSTEM$STREAM_HAS_DATA('AIRFLOW_ETL.BRONZE.YOUTUBE_CATEGORIES_STREAM')) THEN
            UPDATE AIRFLOW_ETL.SILVER.ETL_PROCESS_LOG
            SET status = 'SUCCESS',
                rows_inserted = 0,
                error_message = 'No new data in stream',
                completed_at = CURRENT_TIMESTAMP()
            WHERE procedure_name = 'SP_MERGE_YOUTUBE_CATEGORIES'
              AND status = 'RUNNING'
              AND started_at = :v_start;

            RETURN 'SUCCESS: No new data in stream. Skipped.';
        END IF;

        MERGE INTO AIRFLOW_ETL.SILVER.YOUTUBE_CATEGORIES AS tgt
        USING (
            SELECT
                "ingestion_id" AS ingestion_id,
                "region" AS region,
                f.value:id::integer AS category_id,
                f.value:etag::string AS category_etag,
                f.value:kind::string AS category_kind,
                f.value:snippet:title::string AS category_title,
                f.value:snippet:channelId::string AS category_channel_id,
                f.value:snippet:assignable::boolean AS category_assignable,
                parse_json("raw_json"::variant):etag::string AS search_etag,
                parse_json("raw_json"::variant):kind::string AS response_type,
                parse_json("raw_json"::variant):_pipeline_metadata:ingestion_timestamp::timestamp AS ingestion_time
            FROM AIRFLOW_ETL.BRONZE.YOUTUBE_CATEGORIES_STREAM,
            LATERAL FLATTEN(parse_json("raw_json"::variant):items) AS f
        ) AS src
        ON  tgt.category_id = src.category_id
        AND tgt.region = src.region
        AND tgt.ingestion_id = src.ingestion_id
        WHEN MATCHED THEN UPDATE SET
            tgt.category_etag       = src.category_etag,
            tgt.category_kind       = src.category_kind,
            tgt.category_title      = src.category_title,
            tgt.category_channel_id = src.category_channel_id,
            tgt.category_assignable = src.category_assignable,
            tgt.search_etag         = src.search_etag,
            tgt.response_type       = src.response_type,
            tgt.ingestion_time      = src.ingestion_time
        WHEN NOT MATCHED THEN INSERT (
            ingestion_id, region, category_id, category_etag, category_kind,
            category_title, category_channel_id, category_assignable,
            search_etag, response_type, ingestion_time
        ) VALUES (
            src.ingestion_id, src.region, src.category_id, src.category_etag, src.category_kind,
            src.category_title, src.category_channel_id, src.category_assignable,
            src.search_etag, src.response_type, src.ingestion_time
        );

        v_rows_inserted := SQLROWCOUNT;

        -- Update log with success
        UPDATE AIRFLOW_ETL.SILVER.ETL_PROCESS_LOG
        SET status = 'SUCCESS',
            rows_inserted = :v_rows_inserted,
            completed_at = CURRENT_TIMESTAMP()
        WHERE procedure_name = 'SP_MERGE_YOUTUBE_CATEGORIES'
          AND status = 'RUNNING'
          AND started_at = :v_start;

        RETURN 'SUCCESS: Merged ' || :v_rows_inserted || ' rows from stream.';

    EXCEPTION
        WHEN OTHER THEN
            UPDATE AIRFLOW_ETL.SILVER.ETL_PROCESS_LOG
            SET status = 'FAILED',
                error_message = SQLERRM,
                completed_at = CURRENT_TIMESTAMP()
            WHERE procedure_name = 'SP_MERGE_YOUTUBE_CATEGORIES'
              AND status = 'RUNNING'
              AND started_at = :v_start;

            RETURN 'FAILED: ' || SQLERRM;
    END;
END;