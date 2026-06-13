from airflow.decorators import dag, task
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from datetime import datetime, timedelta
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
    dag_id='snowflake_youtube_silver_layer',
    default_args=default_args,
    schedule='0 */12 * * *',  # Run every 12 hours
    catchup=False,
    start_date=datetime(2024, 1, 1),
    doc_md="""
    ## Snowflake Silver Layer - YouTube Data Processing
    
    This DAG executes stored procedures in Snowflake to:
    1. Merge trending videos from Bronze to Silver layer
    2. Merge video categories from Bronze to Silver layer
    
    Configuration via Airflow:
    - SNOWFLAKE_CONN_ID: Snowflake connection ID (default: snowflake_prod)
    """,
    tags=['snowflake', 'silver-layer', 'youtube'],
)
def snowflake_youtube_silver_pipeline():
    """
    Main pipeline function that orchestrates Snowflake procedures
    """
    
    # ========================================================================
    # TASK 1: Execute SP_MERGE_YOUTUBE_TRENDING_VIDEOS
    # ========================================================================
    @task(task_id='merge_trending_videos')
    def merge_trending_videos():
        """
        Call Snowflake stored procedure to merge trending videos
        from Bronze layer to Silver layer
        """
        try:
            snowflake_conn_id = Variable.get(
                'SNOWFLAKE_CONN_ID', 
                'snowflake_default'
            )
            
            hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
            
            logger.info("Executing SP_MERGE_YOUTUBE_TRENDING_VIDEOS...")
            
            # Execute the procedure
            result = hook.run(
                sql="CALL AIRFLOW_ETL.SILVER.SP_MERGE_YOUTUBE_TRENDING_VIDEOS();",
                autocommit=True
            )
            
            logger.info("✓ SP_MERGE_YOUTUBE_TRENDING_VIDEOS executed successfully")
            
            return {
                'status': 'success',
                'procedure': 'SP_MERGE_YOUTUBE_TRENDING_VIDEOS',
                'result': result
            }
        
        except Exception as e:
            logger.error(f"Error executing SP_MERGE_YOUTUBE_TRENDING_VIDEOS: {str(e)}")
            raise

    # ========================================================================
    # TASK 2: Execute SP_MERGE_YOUTUBE_CATEGORIES
    # ========================================================================
    @task(task_id='merge_categories')
    def merge_categories():
        """
        Call Snowflake stored procedure to merge video categories
        from Bronze layer to Silver layer
        """
        try:
            snowflake_conn_id = Variable.get(
                'SNOWFLAKE_CONN_ID', 
                'snowflake_default'
            )
            
            hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
            
            logger.info("Executing SP_MERGE_YOUTUBE_CATEGORIES...")
            
            # Execute the procedure
            result = hook.run(
                sql="CALL AIRFLOW_ETL.SILVER.SP_MERGE_YOUTUBE_CATEGORIES();",
                autocommit=True
            )
            
            logger.info("✓ SP_MERGE_YOUTUBE_CATEGORIES executed successfully")
            
            return {
                'status': 'success',
                'procedure': 'SP_MERGE_YOUTUBE_CATEGORIES',
                'result': result
            }
        
        except Exception as e:
            logger.error(f"Error executing SP_MERGE_YOUTUBE_CATEGORIES: {str(e)}")
            raise

    # ========================================================================
    # TASK 3: Execution Summary
    # ========================================================================
    @task(task_id='execution_summary')
    def execution_summary(trending_result, categories_result):
        """
        Log summary of procedure executions
        """
        try:
            summary = {
                'trending_videos': trending_result.get('procedure'),
                'categories': categories_result.get('procedure'),
                'timestamp': datetime.utcnow().isoformat(),
                'status': 'COMPLETED'
            }
            
            logger.info("=" * 70)
            logger.info("✓ SNOWFLAKE SILVER LAYER EXECUTION SUMMARY")
            logger.info("=" * 70)
            logger.info(f"  Procedure 1: {summary['trending_videos']}")
            logger.info(f"  Procedure 2: {summary['categories']}")
            logger.info(f"  Timestamp:   {summary['timestamp']}")
            logger.info(f"  Status:      {summary['status']}")
            logger.info("=" * 70)
            
            return summary
        
        except Exception as e:
            logger.error(f"Error in execution summary: {str(e)}")
            raise

    # ========================================================================
    # Task Dependencies & Orchestration
    # ========================================================================
    
    # Execute both procedures in parallel
    trending = merge_trending_videos()
    categories = merge_categories()
    
    # After both complete, run summary
    summary = execution_summary(trending, categories)


# ============================================================================
# Instantiate the DAG
# ============================================================================
snowflake_dag = snowflake_youtube_silver_pipeline()


# ============================================================================
# Alternative: Using SnowflakeOperator (if you prefer)
# ============================================================================
# Uncomment below if you want to use SnowflakeOperator instead of hook

"""
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

@dag(
    dag_id='snowflake_youtube_silver_layer_operator',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    start_date=datetime(2024, 1, 1),
    tags=['snowflake', 'silver-layer'],
)
def snowflake_silver_pipeline_operator():
    
    merge_trending = SnowflakeOperator(
        task_id='merge_trending_videos',
        sql='CALL AIRFLOW_ETL.SILVER.SP_MERGE_YOUTUBE_TRENDING_VIDEOS();',
        snowflake_conn_id='snowflake_default',
        autocommit=True,
    )
    
    merge_categories = SnowflakeOperator(
        task_id='merge_categories',
        sql='CALL AIRFLOW_ETL.SILVER.SP_MERGE_YOUTUBE_CATEGORIES();',
        snowflake_conn_id='snowflake_prod',
        autocommit=True,
    )
    
    # Both execute in parallel, then chain
    [merge_trending, merge_categories]

snowflake_dag_operator = snowflake_silver_pipeline_operator()
"""