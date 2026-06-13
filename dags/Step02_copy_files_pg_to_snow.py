from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from datetime import datetime, timedelta
import json
import logging
import pandas as pd

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
    dag_id='postgres_to_snowflake_incremental_sync',
    default_args=default_args,
    schedule='0 */12 * * *',  # Run every 12 hours
    catchup=False,
    start_date=datetime(2024, 1, 1),
    doc_md="""
    ## PostgreSQL to Snowflake Incremental Sync (with Watermarks)
    
    Copies data from PostgreSQL project database to Snowflake using incremental sync.
    Tracks watermarks (last sync timestamp) for each table in Snowflake.
    On each run, only new/updated records are synced.
    
    Configuration via Airflow Variables:
    - POSTGRES_CONN_ID: PostgreSQL connection ID
    - SNOWFLAKE_CONN_ID: Snowflake connection ID
    - SNOWFLAKE_DB: Snowflake database name
    - SNOWFLAKE_SCHEMA: Snowflake schema name
    - SYNC_TABLES: Comma-separated table names to sync (e.g., youtube_bronze_trending_videos,youtube_bronze_categories)
    """,
    tags=['postgres', 'snowflake', 'incremental-sync', 'watermark'],
)
def postgres_to_snowflake_pipeline():
    """
    Main pipeline function for incremental data sync
    """
    
    # ========================================================================
    # TASK 1: Create Watermark Table in Snowflake
    # ========================================================================
    @task(task_id='create_watermark_table')
    def create_watermark_table():
        """
        Create watermark table in Snowflake to track last sync timestamps
        """
        try:
            snowflake_conn_id = Variable.get('SNOWFLAKE_CONN_ID', 'snowflake_default')
            db = Variable.get('SNOWFLAKE_DB', 'analytics')
            schema = Variable.get('SNOWFLAKE_SCHEMA', 'bronze')
            
            snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
            
            logger.info(f"Creating watermark table in {db}.{schema}")
            
            # Create database and schema if not exists
            snowflake_hook.run(f"CREATE DATABASE IF NOT EXISTS {db}")
            snowflake_hook.run(f"CREATE SCHEMA IF NOT EXISTS {db}.{schema}")
            
            # Create watermark table
            create_watermark_sql = f"""
            CREATE TABLE IF NOT EXISTS {db}.{schema}.sync_watermark (
                id INT AUTOINCREMENT PRIMARY KEY,
                table_name VARCHAR(255) NOT NULL,
                last_sync_time TIMESTAMP,
                current_sync_time TIMESTAMP NOT NULL,
                records_synced INT DEFAULT 0,
                status VARCHAR(50) DEFAULT 'SUCCESS',
                error_message VARCHAR(1000),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                UNIQUE(table_name, current_sync_time)
            );
            """
            
            snowflake_hook.run(create_watermark_sql)
            logger.info("Watermark table created/verified")
            
            return {
                'status': 'success',
                'watermark_table_created': True,
                'database': db,
                'schema': schema
            }
        
        except Exception as e:
            logger.error(f"Error creating watermark table: {str(e)}")
            raise

    # ========================================================================
    # TASK 2: Get List of Tables to Sync
    # ========================================================================
    @task(task_id='get_sync_tables')
    def get_sync_tables():
        """
        Get list of tables to sync from Airflow Variables
        """
        try:
            tables_str = Variable.get('SYNC_TABLES', 'youtube_bronze_trending_videos,youtube_bronze_categories')
            tables = [t.strip() for t in tables_str.split(',') if t.strip()]
            
            logger.info(f"Tables to sync: {tables}")
            
            return {
                'tables': tables,
                'total_tables': len(tables)
            }
        
        except Exception as e:
            logger.error(f"Error getting sync tables: {str(e)}")
            raise

    # ========================================================================
    # TASK 3: Get Last Watermark for Each Table
    # ========================================================================
    @task(task_id='get_last_watermarks')
    def get_last_watermarks(watermark_info, table_info):
        """
        Fetch last sync timestamp for each table from Snowflake
        """
        try:
            snowflake_conn_id = Variable.get('SNOWFLAKE_CONN_ID', 'snowflake_default')
            db = watermark_info['database']
            schema = watermark_info['schema']
            tables = table_info.get('tables', []) if table_info else []
            
            snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
            
            watermarks = {}
            
            if not tables:
                logger.info("No tables configured for sync. Skipping watermark lookup.")
                return watermarks
            
            for table in tables:
                # Get last successful sync time for this table
                query = f"""
                SELECT MAX(current_sync_time) as last_sync_time
                FROM {db}.{schema}.sync_watermark
                WHERE table_name = '{table}' AND status = 'SUCCESS'
                """
                
                result = snowflake_hook.get_first(query)
                last_sync_time = result[0] if result and result[0] else None
                
                watermarks[table] = {
                    'table_name': table,
                    'last_sync_time': last_sync_time.isoformat() if last_sync_time else None,
                    'current_sync_time': datetime.utcnow().isoformat()
                }
                
                logger.info(f"Table: {table}, Last Sync: {watermarks[table]['last_sync_time']}")
            
            return watermarks
        
        except Exception as e:
            logger.error(f"Error getting watermarks: {str(e)}")
            raise

    # ========================================================================
    # TASK 4: Sync Data for Each Table
    # ========================================================================
    @task(task_id='sync_table_data')
    def sync_table_data(watermark_info, watermarks):
        """
        Sync data from PostgreSQL to Snowflake for each table
        using watermark timestamps for incremental load
        """
        try:
            postgres_conn_id = Variable.get('POSTGRES_CONN_ID', 'postgres_local')
            snowflake_conn_id = Variable.get('SNOWFLAKE_CONN_ID', 'snowflake_default')
            db = watermark_info['database']
            schema = watermark_info['schema']
            
            postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
            snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
            
            sync_results = {}
            watermarks = watermarks or {}
            
            for table_name, watermark_data in watermarks.items():
                logger.info(f"Starting sync for table: {table_name}")
                
                try:
                    last_sync_time = watermark_data['last_sync_time']
                    current_sync_time = watermark_data['current_sync_time']
                    
                    # Build query to fetch new/updated records
                    if last_sync_time:
                        # Incremental: fetch records after last sync
                        query = f"""
                        SELECT * FROM {table_name}
                        WHERE created_at > '{last_sync_time}'::TIMESTAMP
                        ORDER BY created_at
                        """
                        logger.info(f"Incremental sync from {last_sync_time}")
                    else:
                        # Full: fetch all records (first time)
                        query = f"SELECT * FROM {table_name} ORDER BY created_at"
                        logger.info("Full sync (first time)")
                    
                    # Fetch data from PostgreSQL
                    pg_conn = postgres_hook.get_conn()
                    cursor = pg_conn.cursor()
                    cursor.execute(query)
                    records = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description] if cursor.description else []
                    
                    if not records:
                        logger.info(f"No new records found for {table_name}")
                        sync_results[table_name] = {
                            'records_synced': 0,
                            'status': 'SUCCESS',
                            'error_message': None
                        }
                        continue
                    
                    # Convert to DataFrame
                    df = pd.DataFrame(records, columns=columns)
                    
                    # Create target table in Snowflake if not exists
                    target_table = f"{db}.{schema}.{table_name}"
                    
                    # Upload to Snowflake
                    snowflake_hook.run(f"CREATE SCHEMA IF NOT EXISTS {db}.{schema}")
                    
                    # Create the target table with broad VARCHAR columns to avoid truncation
                    column_names = [f'"{col}"' for col in df.columns]
                    col_names = ', '.join(column_names)
                    column_defs = ', '.join([f'"{col}" VARCHAR' for col in df.columns])
                    snowflake_hook.run(f"CREATE TABLE IF NOT EXISTS {target_table} ({column_defs})")
                    
                    def normalize_snowflake_param(value):
                        if value is None or (isinstance(value, float) and pd.isna(value)):
                            return None
                        if isinstance(value, bool):
                            return value
                        if isinstance(value, (int, float)) and not isinstance(value, bool):
                            return value
                        if isinstance(value, datetime):
                            return value.strftime('%Y-%m-%d %H:%M:%S')
                        if isinstance(value, pd.Timestamp):
                            if pd.isna(value):
                                return None
                            return value.to_pydatetime().strftime('%Y-%m-%d %H:%M:%S')
                        if hasattr(value, 'item') and not isinstance(value, str):
                            try:
                                value = value.item()
                            except Exception:
                                pass
                        if isinstance(value, bytes):
                            value = value.decode('utf-8', 'ignore')
                        return str(value)
                    
                    sf_conn = snowflake_hook.get_conn()
                    sf_cursor = sf_conn.cursor()
                    placeholder = ', '.join(['%s'] * len(df.columns))
                    insert_sql = f"INSERT INTO {target_table} ({col_names}) VALUES ({placeholder})"
                    
                    for _, row in df.iterrows():
                        row_values = [normalize_snowflake_param(val) for val in row.tolist()]
                        sf_cursor.execute(insert_sql, tuple(row_values))
                    sf_conn.commit()
                    sf_cursor.close()
                    sf_conn.close()
                    
                    records_synced = len(df)
                    logger.info(f"Successfully synced {records_synced} records from {table_name}")
                    
                    sync_results[table_name] = {
                        'records_synced': records_synced,
                        'status': 'SUCCESS',
                        'error_message': None
                    }
                
                except Exception as e:
                    logger.error(f"Error syncing {table_name}: {str(e)}")
                    sync_results[table_name] = {
                        'records_synced': 0,
                        'status': 'FAILED',
                        'error_message': str(e)
                    }
            
            return sync_results
        
        except Exception as e:
            logger.error(f"Error in sync_table_data: {str(e)}")
            raise

    # ========================================================================
    # TASK 5: Update Watermark Table
    # ========================================================================
    @task(task_id='update_watermarks')
    def update_watermarks(watermark_info, watermarks, sync_results):
        """
        Update watermark table in Snowflake with sync results
        """
        try:
            snowflake_conn_id = Variable.get('SNOWFLAKE_CONN_ID', 'snowflake_default')
            db = watermark_info['database']
            schema = watermark_info['schema']
            
            snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
            
            watermark_table = f"{db}.{schema}.sync_watermark"
            
            sync_results = sync_results or {}
            watermarks = watermarks or {}
            
            if not sync_results:
                logger.info("No sync results available to update watermarks")
                return {
                    'status': 'success',
                    'watermarks_updated': 0
                }
            
            for table_name, result in sync_results.items():
                watermark_data = watermarks.get(table_name, {})
                last_sync_time = watermark_data.get('last_sync_time')
                current_sync_time = watermark_data.get('current_sync_time', datetime.utcnow().isoformat())
                records_synced = result['records_synced']
                status = result['status']
                error_message = result['error_message']
                
                # Insert watermark record
                insert_watermark_sql = f"""
                INSERT INTO {watermark_table}
                (table_name, last_sync_time, current_sync_time, records_synced, status, error_message)
                VALUES (
                    '{table_name}',
                    {f"'{last_sync_time}'" if last_sync_time else 'NULL'},
                    '{current_sync_time}',
                    {records_synced},
                    '{status}',
                    {f"'{error_message.replace(chr(39), chr(39)*2)}'" if error_message else 'NULL'}
                )
                """
                
                snowflake_hook.run(insert_watermark_sql)
                logger.info(f"Updated watermark for {table_name}: {records_synced} records, status: {status}")
            
            # Fetch and log all watermarks
            query = f"""
            SELECT table_name, last_sync_time, current_sync_time, records_synced, status
            FROM {watermark_table}
            ORDER BY updated_at DESC
            LIMIT 10
            """
            
            results = snowflake_hook.get_pandas_df(query)
            logger.info(f"Watermark table:\n{results}")
            
            return {
                'status': 'success',
                'watermarks_updated': len(sync_results)
            }
        
        except Exception as e:
            logger.error(f"Error updating watermarks: {str(e)}")
            raise

    # ========================================================================
    # TASK 6: Sync Summary
    # ========================================================================
    @task(task_id='sync_summary')
    def sync_summary(sync_results):
        """
        Log summary of all sync operations
        """
        try:
            sync_results = sync_results or {}
            total_tables = len(sync_results)
            successful_tables = sum(1 for r in sync_results.values() if r['status'] == 'SUCCESS')
            total_records = sum(r['records_synced'] for r in sync_results.values())
            failed_tables = total_tables - successful_tables
            
            summary = (
                f"✓ Sync Complete\n"
                f"  Total Tables: {total_tables}\n"
                f"  Successful: {successful_tables}\n"
                f"  Failed: {failed_tables}\n"
                f"  Total Records Synced: {total_records}"
            )
            
            logger.info(summary)
            
            return {
                'status': 'success',
                'total_tables': total_tables,
                'successful_tables': successful_tables,
                'failed_tables': failed_tables,
                'total_records': total_records,
                'summary': summary
            }
        
        except Exception as e:
            logger.error(f"Error in sync_summary: {str(e)}")
            raise

    # ========================================================================
    # Task Dependencies & Orchestration
    # ========================================================================
    
    # Create watermark table first
    watermark_info = create_watermark_table()
    
    # Get tables to sync
    table_info = get_sync_tables()
    
    # Get watermarks for each table
    watermarks = get_last_watermarks(watermark_info, table_info)
    
    # Sync data
    sync_results = sync_table_data(watermark_info, watermarks)
    
    # Update watermarks
    watermark_update = update_watermarks(watermark_info, watermarks, sync_results)
    
    # Final summary
    summary = sync_summary(sync_results)
    
    # Dependencies
    watermark_info >> table_info >> watermarks >> sync_results >> [watermark_update, summary]


# ============================================================================
# Instantiate the DAG
# ============================================================================
postgres_to_snowflake_dag = postgres_to_snowflake_pipeline()
