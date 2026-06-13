from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import googleapiclient.discovery
import json
import logging

logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'data-engineer',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ============================================================================
# DAG Definition using @dag decorator
# ============================================================================
@dag(
    dag_id='youtube_bronze_layer_ingestion',
    default_args=default_args,
    schedule='0 */6 * * *',  # Run every 6 hours (cron format)
    catchup=False,
    start_date=datetime(2024, 1, 1),
    doc_md="""
    ## YouTube Data API Ingestion (Bronze Layer - Staging)
    
    Pulls trending videos from the YouTube Data API for each configured region
    and writes raw JSON responses to PostgreSQL staging tables.
    
    Bronze layer allows duplicates. Deduplication happens in Silver layer.
    
    Configuration via Airflow Variables:
    - YOUTUBE_API_KEY: Google API key with YouTube Data API v3 enabled
    - YOUTUBE_REGIONS: Comma-separated region codes (default: US,GB,CA,...)
    - POSTGRES_CONN_ID: PostgreSQL connection ID
    """,
    tags=['youtube', 'bronze-layer', 'api-ingestion'],
)
def youtube_bronze_layer_pipeline():
    """
    Main pipeline function that orchestrates all tasks
    """
    
    # ========================================================================
    # TASK 1: Setup - Create Bronze Layer Tables (Staging - No Constraints)
    # ========================================================================
    @task(task_id='create_bronze_tables')
    def create_bronze_tables():
        """
        Create raw data tables in PostgreSQL for Bronze layer (Staging).
        
        These tables allow duplicates for flexibility in data ingestion.
        Deduplication will happen in the Silver layer.
        No PRIMARY KEY or UNIQUE constraints.
        """
        try:
            postgres_conn_id = Variable.get('POSTGRES_CONN_ID', 'postgres_local')
            postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
            
            conn = postgres_hook.get_conn()
            cursor = conn.cursor()
            
            logger.info("Creating Bronze layer staging tables (no constraints)...")
            
            # ────────────────────────────────────────────────────────────────
            # Table for raw trending videos data (NO PRIMARY KEY, NO UNIQUE)
            # ────────────────────────────────────────────────────────────────
            create_raw_videos_table = """
            DROP TABLE IF EXISTS youtube_bronze_trending_videos CASCADE;
            
            CREATE TABLE youtube_bronze_trending_videos (
                ingestion_id VARCHAR(50) NOT NULL,
                region VARCHAR(10) NOT NULL,
                ingestion_timestamp TIMESTAMP NOT NULL,
                raw_json JSONB NOT NULL,
                video_count INT,
                source VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX idx_region_timestamp 
            ON youtube_bronze_trending_videos(region, ingestion_timestamp DESC);
            CREATE INDEX idx_ingestion_id 
            ON youtube_bronze_trending_videos(ingestion_id);
            CREATE INDEX idx_created_at 
            ON youtube_bronze_trending_videos(created_at DESC);
            """
            
            cursor.execute(create_raw_videos_table)
            conn.commit()
            
            logger.info("✓ Raw videos staging table created (allows duplicates)")
            
            # ────────────────────────────────────────────────────────────────
            # Table for raw category reference data (NO PRIMARY KEY, NO UNIQUE)
            # ────────────────────────────────────────────────────────────────
            create_categories_table = """
            DROP TABLE IF EXISTS youtube_bronze_categories CASCADE;
            
            CREATE TABLE youtube_bronze_categories (
                ingestion_id VARCHAR(50) NOT NULL,
                region VARCHAR(10) NOT NULL,
                ingestion_timestamp TIMESTAMP NOT NULL,
                raw_json JSONB NOT NULL,
                source VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX idx_categories_region_timestamp 
            ON youtube_bronze_categories(region, ingestion_timestamp DESC);
            CREATE INDEX idx_categories_ingestion_id 
            ON youtube_bronze_categories(ingestion_id);
            CREATE INDEX idx_categories_created_at 
            ON youtube_bronze_categories(created_at DESC);
            """
            
            cursor.execute(create_categories_table)
            conn.commit()
            
            logger.info("✓ Categories staging table created (allows duplicates)")
            
            # ────────────────────────────────────────────────────────────────
            # Audit/metadata table
            # ────────────────────────────────────────────────────────────────
            create_audit_table = """
            DROP TABLE IF EXISTS youtube_ingestion_audit CASCADE;
            
            CREATE TABLE youtube_ingestion_audit (
                id SERIAL PRIMARY KEY,
                ingestion_id VARCHAR(50) NOT NULL,
                started_at TIMESTAMP NOT NULL,
                completed_at TIMESTAMP,
                total_regions INT,
                success_count INT,
                failed_count INT,
                status VARCHAR(50),
                error_details JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX idx_audit_ingestion_id 
            ON youtube_ingestion_audit(ingestion_id);
            CREATE INDEX idx_audit_status 
            ON youtube_ingestion_audit(status);
            CREATE INDEX idx_audit_created_at 
            ON youtube_ingestion_audit(created_at DESC);
            """
            
            cursor.execute(create_audit_table)
            conn.commit()
            
            logger.info("✓ Audit table created/verified")
            cursor.close()
            conn.close()
            
            return {'status': 'success', 'tables_created': True}
        
        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            raise

    # ========================================================================
    # TASK 2: Fetch Trending Videos from YouTube API
    # ========================================================================
    @task(task_id='fetch_trending_videos')
    def fetch_trending_videos():
        """
        Fetch trending videos from YouTube Data API for each region
        Similar to Lambda's fetch_trending_videos function
        """
        try:
            youtube_api_key = Variable.get('YOUTUBE_API_KEY')
            regions_str = Variable.get('YOUTUBE_REGIONS', 'US,GB,CA,DE,FR,IN,JP,KR,MX,RU')
            regions = [r.strip().upper() for r in regions_str.split(',')]
            
            now = datetime.utcnow()
            ingestion_id = now.strftime("%Y%m%d_%H%M%S")
            
            logger.info(f"Starting ingestion {ingestion_id} for regions: {regions}")
            
            youtube = googleapiclient.discovery.build(
                'youtube',
                'v3',
                developerKey=youtube_api_key
            )
            
            regional_data = {}
            
            for region in regions:
                logger.info(f"Fetching trending videos for region: {region}")
                
                try:
                    request = youtube.videos().list(
                        part='snippet,statistics,contentDetails',
                        chart='mostPopular',
                        regionCode=region,
                        maxResults=50
                    )
                    
                    response = request.execute()
                    video_count = len(response.get('items', []))
                    
                    # Add pipeline metadata (same as Lambda)
                    response['_pipeline_metadata'] = {
                        'ingestion_id': ingestion_id,
                        'region': region,
                        'ingestion_timestamp': now.isoformat(),
                        'video_count': video_count,
                        'source': 'youtube_data_api_v3'
                    }
                    
                    regional_data[region] = {
                        'data': response,
                        'video_count': video_count,
                        'status': 'success'
                    }
                    
                    logger.info(f"  ✓ Fetched {video_count} videos from {region}")
                
                except Exception as e:
                    logger.error(f"  ✗ Error fetching {region}: {str(e)}")
                    regional_data[region] = {
                        'status': 'failed',
                        'error': str(e)
                    }
            
            success_count = sum(1 for v in regional_data.values() if v['status'] == 'success')
            logger.info(f"Trending videos fetched: {success_count}/{len(regions)} regions successful")
            
            return {
                'regional_data': regional_data,
                'ingestion_id': ingestion_id,
                'ingestion_timestamp': now.isoformat(),
                'regions_processed': len(regions),
                'regions_successful': success_count
            }
        
        except Exception as e:
            logger.error(f"Error in fetch_trending_videos: {str(e)}")
            raise

    # ========================================================================
    # TASK 3: Fetch Category Reference Data
    # ========================================================================
    @task(task_id='fetch_categories')
    def fetch_categories(trending_result):
        """
        Fetch video category mappings for each region
        Similar to Lambda's fetch_video_categories function
        """
        try:
            youtube_api_key = Variable.get('YOUTUBE_API_KEY')
            regions_str = Variable.get('YOUTUBE_REGIONS', 'US,GB,CA,DE,FR,IN,JP,KR,MX,RU')
            regions = [r.strip().upper() for r in regions_str.split(',')]
            
            ingestion_id = trending_result['ingestion_id']
            ingestion_timestamp = trending_result['ingestion_timestamp']
            
            logger.info(f"Fetching category reference data for {len(regions)} regions")
            
            youtube = googleapiclient.discovery.build(
                'youtube',
                'v3',
                developerKey=youtube_api_key
            )
            
            category_data = {}
            
            for region in regions:
                logger.info(f"Fetching categories for region: {region}")
                
                try:
                    request = youtube.videoCategories().list(
                        part='snippet',
                        regionCode=region,
                        hl='en'
                    )
                    
                    response = request.execute()
                    
                    # Add pipeline metadata
                    response['_pipeline_metadata'] = {
                        'ingestion_id': ingestion_id,
                        'region': region,
                        'ingestion_timestamp': ingestion_timestamp,
                        'source': 'youtube_data_api_v3'
                    }
                    
                    category_data[region] = {
                        'data': response,
                        'status': 'success'
                    }
                    
                    logger.info(f"  ✓ Fetched categories for {region}")
                
                except Exception as e:
                    logger.error(f"  ✗ Error fetching categories for {region}: {str(e)}")
                    category_data[region] = {
                        'status': 'failed',
                        'error': str(e)
                    }
            
            success_count = sum(1 for v in category_data.values() if v['status'] == 'success')
            logger.info(f"Categories fetched: {success_count}/{len(regions)} regions successful")
            
            return {
                'category_data': category_data,
                'regions_processed': len(regions),
                'regions_successful': success_count
            }
        
        except Exception as e:
            logger.error(f"Error in fetch_categories: {str(e)}")
            raise

    # ========================================================================
    # TASK 4: Load Trending Videos to PostgreSQL (Simple Insert - No Conflict)
    # ========================================================================
    @task(task_id='load_trending_to_postgres')
    def load_trending_to_postgres(trending_result):
        """
        Insert raw trending videos data into PostgreSQL Bronze layer (Staging).
        
        Simple INSERT without ON CONFLICT - allows duplicates.
        Deduplication happens in Silver layer.
        """
        try:
            postgres_conn_id = Variable.get('POSTGRES_CONN_ID', 'postgres_local')
            postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
            
            regional_data = trending_result['regional_data']
            
            conn = postgres_hook.get_conn()
            cursor = conn.cursor()
            
            # ✅ SIMPLE INSERT - NO ON CONFLICT
            insert_sql = """
            INSERT INTO youtube_bronze_trending_videos 
            (ingestion_id, region, ingestion_timestamp, raw_json, video_count, source)
            VALUES (%s, %s, %s, %s, %s, %s);
            """
            
            records_loaded = 0
            
            for region, data in regional_data.items():
                if data['status'] == 'success':
                    metadata = data['data'].get('_pipeline_metadata', {})
                    
                    cursor.execute(insert_sql, (
                        metadata.get('ingestion_id'),
                        metadata.get('region'),
                        metadata.get('ingestion_timestamp'),
                        json.dumps(data['data']),  # Store entire response as JSONB
                        metadata.get('video_count'),
                        metadata.get('source')
                    ))
                    
                    records_loaded += 1
                    logger.info(f"✓ Loaded trending data for {region}")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"✓ Successfully loaded {records_loaded} trending video datasets to staging")
            
            return {
                'records_loaded': records_loaded,
                'total_regions': len(regional_data)
            }
        
        except Exception as e:
            logger.error(f"Error loading trending data to PostgreSQL: {str(e)}")
            raise

    # ========================================================================
    # TASK 5: Load Categories to PostgreSQL (Simple Insert - No Conflict)
    # ========================================================================
    @task(task_id='load_categories_to_postgres')
    def load_categories_to_postgres(category_result):
        """
        Insert raw category reference data into PostgreSQL Bronze layer (Staging).
        
        Simple INSERT without ON CONFLICT - allows duplicates.
        Deduplication happens in Silver layer.
        """
        try:
            postgres_conn_id = Variable.get('POSTGRES_CONN_ID', 'postgres_local')
            postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
            
            category_data = category_result['category_data']
            
            conn = postgres_hook.get_conn()
            cursor = conn.cursor()
            
            # ✅ SIMPLE INSERT - NO ON CONFLICT
            insert_sql = """
            INSERT INTO youtube_bronze_categories 
            (ingestion_id, region, ingestion_timestamp, raw_json, source)
            VALUES (%s, %s, %s, %s, %s);
            """
            
            records_loaded = 0
            
            for region, data in category_data.items():
                if data['status'] == 'success':
                    metadata = data['data'].get('_pipeline_metadata', {})
                    
                    cursor.execute(insert_sql, (
                        metadata.get('ingestion_id'),
                        metadata.get('region'),
                        metadata.get('ingestion_timestamp'),
                        json.dumps(data['data']),
                        metadata.get('source')
                    ))
                    
                    records_loaded += 1
                    logger.info(f"✓ Loaded categories for {region}")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"✓ Successfully loaded {records_loaded} category datasets to staging")
            
            return {
                'records_loaded': records_loaded,
                'total_regions': len(category_data)
            }
        
        except Exception as e:
            logger.error(f"Error loading categories to PostgreSQL: {str(e)}")
            raise

    # ========================================================================
    # TASK 6: Audit & Summary
    # ========================================================================
    @task(task_id='audit_and_summary')
    def audit_and_summary(trending_result, trending_load, category_load):
        """
        Create audit entry and log summary of the ingestion
        """
        try:
            postgres_conn_id = Variable.get('POSTGRES_CONN_ID', 'postgres_local')
            postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
            
            ingestion_id = trending_result['ingestion_id']
            ingestion_timestamp = trending_result['ingestion_timestamp']
            total_regions = trending_result['regions_processed']
            success_regions = trending_result['regions_successful']
            failed_regions = total_regions - success_regions
            
            conn = postgres_hook.get_conn()
            cursor = conn.cursor()
            
            audit_sql = """
            INSERT INTO youtube_ingestion_audit 
            (ingestion_id, started_at, completed_at, total_regions, 
             success_count, failed_count, status, error_details)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
            
            cursor.execute(audit_sql, (
                ingestion_id,
                ingestion_timestamp,
                datetime.utcnow().isoformat(),
                total_regions,
                success_regions,
                failed_regions,
                'PARTIAL_SUCCESS' if failed_regions > 0 else 'SUCCESS',
                json.dumps({
                    'trending_loaded': trending_load.get('records_loaded', 0),
                    'categories_loaded': category_load.get('records_loaded', 0),
                    'failed_regions': failed_regions
                })
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            summary = (
                f"\n{'='*70}\n"
                f"✓ YOUTUBE BRONZE LAYER INGESTION COMPLETE\n"
                f"{'='*70}\n"
                f"  Ingestion ID:     {ingestion_id}\n"
                f"  Timestamp:        {ingestion_timestamp}\n"
                f"  Total Regions:    {total_regions}\n"
                f"  Success:          {success_regions}/{total_regions}\n"
                f"  Failed:           {failed_regions}\n"
                f"  Trending Loaded:  {trending_load.get('records_loaded', 0)}\n"
                f"  Categories Loaded: {category_load.get('records_loaded', 0)}\n"
                f"  Status:           {'SUCCESS' if failed_regions == 0 else 'PARTIAL_SUCCESS'}\n"
                f"{'='*70}"
            )
            
            logger.info(summary)
            
            return {
                'status': 'success',
                'ingestion_id': ingestion_id,
                'summary': summary
            }
        
        except Exception as e:
            logger.error(f"Error in audit: {str(e)}")
            raise

    # ========================================================================
    # Task Dependencies & Orchestration
    # ========================================================================
    
    # Create tables first
    tables = create_bronze_tables()
    
    # Fetch data from YouTube API
    trending = fetch_trending_videos()
    categories = fetch_categories(trending)
    
    # Load data to PostgreSQL
    trending_load = load_trending_to_postgres(trending)
    category_load = load_categories_to_postgres(categories)
    
    # Final audit
    audit = audit_and_summary(trending, trending_load, category_load)
    
    # Ensure tables are created before fetching
    tables >> [trending, categories]


# ============================================================================
# Instantiate the DAG
# ============================================================================
youtube_dag = youtube_bronze_layer_pipeline()