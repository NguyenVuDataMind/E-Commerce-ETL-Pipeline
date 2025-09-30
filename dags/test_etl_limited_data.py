"""
Test ETL DAG - Chỉ lấy dữ liệu 1-2 ngày cho máy yếu
Test pipeline với data limited để debugging và validation
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
import logging
import json
import sys
import os
import gc

# Add project root to path
sys.path.append('/opt/airflow/')

from src.extractors.misa_crm_extractor import MISACRMExtractor
from src.extractors.tiktok_shop_extractor import TikTokShopOrderExtractor
from src.transformers.misa_crm_transformer import MISACRMTransformer
from src.transformers.tiktok_shop_transformer import TikTokShopOrderTransformer
from src.loaders.misa_crm_loader import MISACRMLoader
from src.loaders.tiktok_shop_staging_loader import TikTokShopOrderLoader

logger = logging.getLogger(__name__)

# DAG Configuration
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'catchup': False,
}

dag = DAG(
    'test_etl_limited_data',
    default_args=default_args,
    description='Test ETL with limited data (1-2 days) - for weak machines',
    schedule_interval=None,  # Manual trigger only
    max_active_runs=1,
    tags=['test', 'etl', 'limited-data', 'debug']
)

# ========================
# TIKTOK SHOP TEST FUNCTIONS
# ========================

def test_tiktok_shop_extract_limited(**context):
    """
    Test TikTok Shop extraction - chỉ 2 ngày gần nhất để test
    """
    logger.info("🧪 Testing TikTok Shop extraction (limited data)...")
    
    try:
        extractor = TikTokShopOrderExtractor()
        
        # Chỉ test 2 ngày gần nhất
        logger.info("📅 Test range: Last 2 days")
        
        # Get limited orders from recent 2 days
        test_orders = extractor.extract_recent_orders(days_back=2)
        
        logger.info(f"✅ Test extracted {len(test_orders)} orders")
        
        # Push to XCom
        context['ti'].xcom_push(key='tiktok_test_data', value=test_orders)
        
        return f"Test extracted {len(test_orders)} orders"
        
    except Exception as e:
        logger.error(f"❌ TikTok Shop test extraction failed: {str(e)}")
        raise

def test_tiktok_shop_transform_limited(**context):
    """
    Test TikTok Shop transformation với limited data
    """
    logger.info("🧪 Testing TikTok Shop transformation (limited data)...")
    
    try:
        # Pull test data from XCom
        test_orders = context['ti'].xcom_pull(key='tiktok_test_data')
        
        if not test_orders:
            logger.info("📭 No test data to transform")
            return "No test data to transform"
        
        # Transform test data
        transformer = TikTokShopOrderTransformer()
        transformed_df = transformer.transform_orders_to_dataframe(test_orders)
        
        logger.info(f"✅ Test transformed {len(transformed_df)} records")
        
        # Convert to dict for XCom
        transformed_data = transformed_df.to_dict('records')
        context['ti'].xcom_push(key='tiktok_test_transformed', value=transformed_data)
        
        return f"Test transformed {len(transformed_df)} records"
        
    except Exception as e:
        logger.error(f"❌ TikTok Shop test transformation failed: {str(e)}")
        raise

def test_tiktok_shop_load_limited(**context):
    """
    Test TikTok Shop loading với limited data
    """
    logger.info("🧪 Testing TikTok Shop loading (limited data)...")
    
    try:
        # Pull transformed test data from XCom  
        transformed_data = context['ti'].xcom_pull(key='tiktok_test_transformed')
        
        if not transformed_data:
            logger.info("📭 No test transformed data to load")
            return "No test transformed data to load"
        
        # Convert back to DataFrame
        import pandas as pd
        df = pd.DataFrame(transformed_data)
        
        # Test load
        loader = TikTokShopOrderLoader()
        success = loader.load_orders(df, load_mode='append')
        
        logger.info(f"✅ Test loaded {len(df)} records (success: {success})")
        
        return f"Test loaded {len(df)} records"
        
    except Exception as e:
        logger.error(f"❌ TikTok Shop test loading failed: {str(e)}")
        raise

# ========================
# MISA CRM TEST FUNCTIONS
# ========================

def test_misa_crm_extract_limited(**context):
    """
    Test MISA CRM extraction với limited data
    """
    logger.info("🧪 Testing MISA CRM extraction (limited data)...")
    
    try:
        extractor = MISACRMExtractor()
        
        # Test endpoints với small page size 
        test_endpoints = ['customers', 'contacts']  # Chỉ 2 endpoints để test
        
        test_data = {}
        
        for endpoint in test_endpoints:
            logger.info(f"📥 Testing {endpoint} extraction...")
            
            # Extract với page limits 
            endpoint_data = extractor.extract_endpoint_data(
                endpoint, 
                page=0, 
                page_size=10  # Chỉ lấy 10 records để test
            )
            
            if endpoint_data and endpoint_data.get('data'):
                test_data[endpoint] = endpoint_data.get('data', [])
                logger.info(f"✅ Test extracted {len(test_data[endpoint])} {endpoint}")
            else:
                test_data[endpoint] = []
                logger.info(f"📭 No {endpoint} data found")
        
        # Push to XCom
        context['ti'].xcom_push(key='misa_test_data', value=json.dumps(test_data))
        
        total_records = sum(len(data) for data in test_data.values())
        return f"Test extracted {total_records} records"
        
    except Exception as e:
        logger.error(f"❌ MISA CRM test extraction failed: {str(e)}")
        raise

def test_misa_crm_transform_limited(**context):
    """
    Test MISA CRM transformation với limited data
    """
    logger.info("🧪 Testing MISA CRM transformation (limited data)...")
    
    try:
        # Pull test data from XCom
        test_data_json = context['ti'].xcom_pull(key='misa_test_data')
        
        if not test_data_json:
            logger.info("📭 No test data to transform")
            return "No test data to transform"
        
        test_data = json.loads(test_data_json)
        
        # Transform test data
        transformer = MISACRMTransformer()
        transformed_data = transformer.transform_all_endpoints(test_data, batch_id="test_run")
        
        # Convert DataFrames to dict for XCom
        serialized_data = {}
        total_records = 0
        
        for key, df in transformed_data.items():
            if df is not None and not df.empty:
                # Replace NaN values with None to make it JSON serializable
                df_clean = df.fillna(None)
                serialized_data[key] = df_clean.to_dict('records')
                total_records += len(df)
        
        context['ti'].xcom_push(key='misa_test_transformed', value=json.dumps(serialized_data))
        
        logger.info(f"✅ Test transformed {total_records} records")
        return f"Test transformed {total_records} records"
        
    except Exception as e:
        logger.error(f"❌ MISA CRM test transformation failed: {str(e)}")
        raise

def test_misa_crm_load_limited(**context):
    """
    Test MISA CRM loading với limited data
    """
    logger.info("🧪 Testing MISA CRM loading (limited data)...")
    
    try:
        # Pull transformed test data from XCom
        transformed_data_json = context['ti'].xcom_pull(key='misa_test_transformed')
        
        if not transformed_data_json:
            logger.info("📭 No test transformed data to load")
            return "No test transformed data to load"
        
        transformed_data = json.loads(transformed_data_json)
        
        # Test load
        loader = MISACRMLoader()
        total_loaded = 0
        
        for table_name, records in transformed_data.items():
            if records:
                import pandas as pd
                df = pd.DataFrame(records)
                success = loader.load_dataframe_to_staging(df, table_name, mode='MERGE')
                total_loaded += len(df)
                
                logger.info(f"✅ Test loaded {len(df)} {table_name} records")
        
        logger.info(f"✅ Total test loaded {total_loaded} records")
        return f"Test loaded {total_loaded} records"
        
    except Exception as e:
        logger.error(f"❌ MISA CRM test loading failed: {str(e)}")
        raise

# ========================
# DAG STRUCTURE  
# ========================

# Start task
start_test = DummyOperator(
    task_id='start_test',
    dag=dag
)

# TikTok Shop Test Pipeline
tiktok_test_extract = PythonOperator(
    task_id='tiktok_test_extract',
    python_callable=test_tiktok_shop_extract_limited,
    dag=dag,
    execution_timeout=timedelta(minutes=10)
)

tiktok_test_transform = PythonOperator(
    task_id='tiktok_test_transform',
    python_callable=test_tiktok_shop_transform_limited,
    dag=dag,
    execution_timeout=timedelta(minutes=5)
)

tiktok_test_load = PythonOperator(
    task_id='tiktok_test_load',
    python_callable=test_tiktok_shop_load_limited,
    dag=dag,
    execution_timeout=timedelta(minutes=5)
)

# MISA CRM Test Pipeline  
misa_test_extract = PythonOperator(
    task_id='misa_test_extract', 
    python_callable=test_misa_crm_extract_limited,
    dag=dag,
    execution_timeout=timedelta(minutes=10)
)

misa_test_transform = PythonOperator(
    task_id='misa_test_transform',
    python_callable=test_misa_crm_transform_limited,
    dag=dag,
    execution_timeout=timedelta(minutes=5)
)

misa_test_load = PythonOperator(
    task_id='misa_test_load',
    python_callable=test_misa_crm_load_limited,
    dag=dag,
    execution_timeout=timedelta(minutes=5)
)

# End task
end_test = DummyOperator(
    task_id='end_test',
    dag=dag
)

# ========================
# TASK DEPENDENCIES
# ========================

# Parallel test execution cho TikTok và MISA
start_test >> [tiktok_test_extract, misa_test_extract]

# TikTok Shop test pipeline
tiktok_test_extract >> tiktok_test_transform >> tiktok_test_load

# MISA CRM test pipeline  
misa_test_extract >> misa_test_transform >> misa_test_load

# Both test pipelines must complete
[tiktok_test_load, misa_test_load] >> end_test

