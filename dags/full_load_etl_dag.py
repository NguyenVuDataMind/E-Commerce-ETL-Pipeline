from datetime import datetime, timedelta
import json
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import pandas as pd

# Add project root to path
import sys

sys.path.append("/opt/airflow/")

from src.extractors.misa_crm_extractor import MISACRMExtractor
from src.transformers.misa_crm_transformer import MISACRMTransformer
from src.loaders.misa_crm_loader import MISACRMLoader
from src.extractors.tiktok_shop_extractor import TikTokShopOrderExtractor
from src.transformers.tiktok_shop_transformer import TikTokShopOrderTransformer
from src.loaders.tiktok_shop_staging_loader import TikTokShopOrderLoader
from src.extractors.shopee_orders_extractor import ShopeeOrderExtractor
from src.transformers.shopee_orders_transformer import ShopeeOrderTransformer
from src.loaders.shopee_orders_loader import ShopeeOrderLoader

# Default arguments
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}


def extract_misa_crm_data(**context):
    """Extracts all data from MISA CRM."""
    logger = logging.getLogger(__name__)
    logger.info("Starting MISA CRM extraction...")

    extractor = MISACRMExtractor()
    endpoints = ["customers", "contacts", "sale_orders", "stocks", "products"]
    raw_data = {}

    for endpoint in endpoints:
        logger.info(f"Extracting {endpoint}...")
        raw_data[endpoint] = extractor.extract_all_data_from_endpoint(endpoint)

    context["ti"].xcom_push(key="misa_crm_raw_data", value=json.dumps(raw_data))


def transform_misa_crm_data(**context):
    """Transforms MISA CRM data."""
    import gc  # Import gc ở đầu function
    logger = logging.getLogger(__name__)
    logger.info("Starting MISA CRM transformation...")

    try:
        raw_data_json = context["ti"].xcom_pull(key="misa_crm_raw_data")
        raw_data = json.loads(raw_data_json)

        # Heavy memory operation - transform data
        transformer = MISACRMTransformer()

        # Process with memory management
        logger.info("Starting data transformation with memory optimization...")
        
        # Tạo batch_id từ execution date
        execution_date = context["execution_date"]
        batch_id = f"misa_crm_full_load_{execution_date.strftime('%Y%m%d_%H%M%S')}"
        
        transformed_data = transformer.transform_all_endpoints(raw_data, batch_id)

        # Clear raw data from memory immediately after transformation
        del raw_data
        del transformer

        gc.collect()
        logger.info("Post-transformation memory cleanup completed")

        # Serialize dict of DataFrames to JSON for XCom
        transformed_data_json = {
            k: v.to_json(orient="split") for k, v in transformed_data.items()
        }
        context["ti"].xcom_push(
            key="misa_crm_transformed_data", value=json.dumps(transformed_data_json)
        )

        # Final cleanup
        del transformed_data
        del transformed_data_json
        gc.collect()

    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        # Emergency cleanup
        gc.collect()
        raise


def load_misa_crm_data(**context):
    """Pulls transformed MISA CRM data from XComs and loads it."""
    logger = logging.getLogger(__name__)
    logger.info("Loading MISA CRM data...")
    transformed_data_json = context["ti"].xcom_pull(
        key="misa_crm_transformed_data", task_ids="misa_crm_etl.transform"
    )
    transformed_data_dict = json.loads(transformed_data_json)

    # Deserialize JSON back to DataFrames
    transformed_data = {
        k: pd.read_json(v, orient="split") for k, v in transformed_data_dict.items()
    }

    loader = MISACRMLoader()
    loader.load_all_data_to_staging(transformed_data, truncate_first=True)


# --- TikTok Shop ETL Functions (REFACTORED TO 3 SEPARATE TASKS) ---
def extract_tiktok_shop_full_load(**context):
    """Extract TikTok Shop full load data với auto-detection"""
    logger = logging.getLogger(__name__)
    logger.info("🔄 Starting TikTok Shop Full Load Extraction...")

    try:
        extractor = TikTokShopOrderExtractor()

        # Auto-detect earliest order date với fallback
        logger.info("🔍 Auto-detecting earliest order date...")
        start_date = None

        try:
            # Timeout protection
            import threading
            from queue import Queue

            result_queue = Queue()

            def auto_detect_worker():
                try:
                    earliest = extractor.find_earliest_order_date(max_lookback_years=2)
                    result_queue.put(("success", earliest))
                except Exception as e:
                    result_queue.put(("error", str(e)))

            worker_thread = threading.Thread(target=auto_detect_worker)
            worker_thread.daemon = True
            worker_thread.start()
            worker_thread.join(timeout=30)  # 30 seconds timeout

            if not result_queue.empty():
                status, result = result_queue.get()
                if status == "success" and result:
                    start_date = result

        except Exception as e:
            logger.warning(f"Auto-detection failed: {str(e)}")

        # Fallback to July 1, 2024 if auto-detection fails
        if start_date is None:
            start_date = datetime(2024, 7, 1)
            logger.info(f"📅 Using fallback date: {start_date.strftime('%Y-%m-%d')}")
        else:
            logger.info(
                f"✅ Auto-detected start date: {start_date.strftime('%Y-%m-%d')}"
            )

        end_date = datetime.now()
        start_timestamp = int(start_date.timestamp())
        end_timestamp = int(end_date.timestamp())

        days_span = (end_date - start_date).days
        logger.info(
            f"📅 Processing range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} ({days_span} days)"
        )

        # Extract all orders using streaming
        all_orders = []
        batch_count = 0

        logger.info("🚀 Starting streaming extraction...")
        for orders_batch in extractor.stream_orders_lightweight(
            start_timestamp, end_timestamp, batch_size=20
        ):
            batch_count += 1
            batch_size = len(orders_batch)

            if orders_batch:
                all_orders.extend(orders_batch)
                logger.info(
                    f"📥 Batch {batch_count}: extracted {batch_size} orders (total: {len(all_orders)})"
                )

            # Memory cleanup every 5 batches
            if batch_count % 5 == 0:
                import gc

                gc.collect()

        logger.info(
            f"✅ Extraction complete: {len(all_orders)} orders from {batch_count} batches"
        )

        # Push to XCom
        context["ti"].xcom_push(key="tiktok_shop_raw_data", value=all_orders)
        context["ti"].xcom_push(
            key="extraction_metadata",
            value={
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "total_orders": len(all_orders),
                "batch_count": batch_count,
            },
        )

        return f"Extracted {len(all_orders)} orders"

    except Exception as e:
        logger.error(f"❌ TikTok Shop extraction failed: {str(e)}")
        raise


def transform_tiktok_shop_full_load(**context):
    """Transform TikTok Shop full load data"""
    logger = logging.getLogger(__name__)
    logger.info("🔄 Starting TikTok Shop Full Load Transformation...")

    try:
        # Pull raw data from XCom
        all_orders = context["ti"].xcom_pull(key="tiktok_shop_raw_data")

        if not all_orders:
            logger.warning("📭 No raw data to transform")
            return "No data to transform"

        logger.info(f"📊 Transforming {len(all_orders)} orders...")

        # Transform data
        transformer = TikTokShopOrderTransformer()
        transformed_df = transformer.transform_orders_to_dataframe(all_orders)

        if transformed_df.empty:
            logger.warning("📭 No data after transformation")
            return "No data after transformation"

        logger.info(f"✅ Transformation complete: {len(transformed_df)} records")

        # Convert to dict for XCom (handle NaN values)
        transformed_df_clean = transformed_df.fillna(None)
        transformed_data = transformed_df_clean.to_dict("records")

        # Push to XCom
        context["ti"].xcom_push(
            key="tiktok_shop_transformed_data", value=transformed_data
        )

        # Memory cleanup
        del all_orders
        del transformed_df
        del transformed_df_clean
        import gc

        gc.collect()

        return f"Transformed {len(transformed_data)} records"

    except Exception as e:
        logger.error(f"❌ TikTok Shop transformation failed: {str(e)}")
        raise


def load_tiktok_shop_full_load(**context):
    """Load TikTok Shop full load data to staging"""
    logger = logging.getLogger(__name__)
    logger.info("🔄 Starting TikTok Shop Full Load Loading...")

    try:
        # Pull transformed data from XCom
        transformed_data = context["ti"].xcom_pull(key="tiktok_shop_transformed_data")

        if not transformed_data:
            logger.warning("📭 No transformed data to load")
            return "No data to load"

        # Convert back to DataFrame
        import pandas as pd

        df = pd.DataFrame(transformed_data)

        logger.info(f"📊 Loading {len(df)} records to staging...")

        # Load data (replace mode for full load)
        loader = TikTokShopOrderLoader()
        success = loader.load_orders(df, load_mode="replace")

        if success:
            logger.info(f"✅ Successfully loaded {len(df)} records to staging")

            # Get load statistics
            stats = loader.get_load_statistics()
            logger.info(f"📊 Load statistics: {stats}")

            return f"Successfully loaded {len(df)} records"
        else:
            logger.error(f"❌ Failed to load {len(df)} records")
            raise Exception(f"Failed to load {len(df)} records to staging")

    except Exception as e:
        logger.error(f"❌ TikTok Shop loading failed: {str(e)}")
        raise


# ========================
# SHOPEE ORDERS FUNCTIONS
# ========================


def extract_shopee_orders_full_load(**context):
    """
    Extract tất cả dữ liệu Shopee Orders (Full Load)
    """
    logger = logging.getLogger(__name__)
    logger.info("🚀 Starting Shopee Orders Full Load Extraction...")

    try:
        extractor = ShopeeOrderExtractor()

        # Auto-detect start date hoặc sử dụng fallback
        start_date = None
        try:
            # Timeout protection
            import threading
            from queue import Queue

            result_queue = Queue()

            def auto_detect_worker():
                try:
                    earliest = extractor.find_earliest_order_date(max_lookback_years=2)
                    result_queue.put(("success", earliest))
                except Exception as e:
                    result_queue.put(("error", str(e)))

            worker_thread = threading.Thread(target=auto_detect_worker)
            worker_thread.daemon = True
            worker_thread.start()
            worker_thread.join(timeout=30)  # 30 seconds timeout

            if not result_queue.empty():
                status, result = result_queue.get()
                if status == "success" and result:
                    start_date = result

        except Exception as e:
            logger.warning(f"Auto-detection failed: {str(e)}")

        # Fallback to July 1, 2024 if auto-detection fails
        if start_date is None:
            start_date = datetime(2024, 7, 1)
            logger.info(f"📅 Using fallback date: {start_date.strftime('%Y-%m-%d')}")
        else:
            logger.info(
                f"✅ Auto-detected start date: {start_date.strftime('%Y-%m-%d')}"
            )

        end_date = datetime.now()
        start_timestamp = int(start_date.timestamp())
        end_timestamp = int(end_date.timestamp())

        days_span = (end_date - start_date).days
        logger.info(
            f"📅 Processing range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} ({days_span} days)"
        )

        # Extract all orders
        all_orders = extractor.extract_orders_full_load(start_date, end_date)

        logger.info(f"✅ Extracted {len(all_orders)} orders from Shopee")

        # Push to XCom
        context["ti"].xcom_push(
            key="shopee_orders_raw_data", value=json.dumps(all_orders)
        )

        return f"Successfully extracted {len(all_orders)} orders"

    except Exception as e:
        logger.error(f"❌ Shopee Orders extraction failed: {str(e)}")
        raise


def transform_shopee_orders_full_load(**context):
    """
    Transform dữ liệu Shopee Orders (Full Load)
    """
    logger = logging.getLogger(__name__)
    logger.info("🔄 Starting Shopee Orders Full Load Transformation...")

    try:
        # Pull data from XCom
        raw_data_json = context["ti"].xcom_pull(key="shopee_orders_raw_data")
        if not raw_data_json:
            logger.warning("No raw data found in XCom")
            return "No data to transform"

        orders_data = json.loads(raw_data_json)

        if not orders_data:
            logger.warning("No orders data to transform")
            return "No orders to transform"

        # Transform data
        transformer = ShopeeOrderTransformer()

        # Transform to flat DataFrame (tương thích với loader hiện tại)
        df = transformer.transform_orders_to_flat_dataframe(orders_data)

        logger.info(f"✅ Transformed {len(df)} orders")

        # Convert DataFrame to dict for XCom
        transformed_data = df.to_dict("records")

        # Push to XCom
        context["ti"].xcom_push(
            key="shopee_orders_transformed_data", value=json.dumps(transformed_data)
        )

        return f"Successfully transformed {len(transformed_data)} orders"

    except Exception as e:
        logger.error(f"❌ Shopee Orders transformation failed: {str(e)}")
        raise


def load_shopee_orders_full_load(**context):
    """
    Load dữ liệu Shopee Orders vào staging (Full Load)
    """
    logger = logging.getLogger(__name__)
    logger.info("💾 Starting Shopee Orders Full Load Loading...")

    try:
        # Pull transformed data from XCom
        transformed_data_json = context["ti"].xcom_pull(
            key="shopee_orders_transformed_data"
        )
        if not transformed_data_json:
            logger.warning("No transformed data found in XCom")
            return "No data to load"

        transformed_data = json.loads(transformed_data_json)

        if not transformed_data:
            logger.warning("No transformed data to load")
            return "No data to load"

        # Convert back to DataFrame
        import pandas as pd

        df = pd.DataFrame(transformed_data)

        logger.info(f"📊 Loading {len(df)} records to staging...")

        # Load data (replace mode for full load)
        loader = ShopeeOrderLoader()
        success = loader.load_flat_orders_dataframe(df, load_type="full")

        if success:
            logger.info(f"✅ Successfully loaded {len(df)} records to staging")

            # Get load statistics
            stats = loader.validate_data_integrity()
            logger.info(f"📊 Load statistics: {stats}")

            return f"Successfully loaded {len(df)} records"
        else:
            logger.error(f"❌ Failed to load {len(df)} records")
            raise Exception(f"Failed to load {len(df)} records to staging")

    except Exception as e:
        logger.error(f"❌ Shopee Orders loading failed: {str(e)}")
        raise


# --- DAG Definition ---
with DAG(
    dag_id="full_load_etl_dag",
    default_args=default_args,
    description="Full Load ETL with 3-Task Structure - Extracts ALL historical data from earliest available date",
    schedule_interval=None,
    tags=["etl", "full-load", "refactored", "historical"],
    catchup=False,
) as dag:

    start_task = BashOperator(
        task_id="start_full_load",
        bash_command='echo "Starting Parallel Full Load ETL with Consistent 3-Task Structure..."',
    )

    with TaskGroup(group_id="misa_crm_etl") as misa_crm_group:
        extract_task = PythonOperator(
            task_id="extract",
            python_callable=extract_misa_crm_data,
        )
        transform_task = PythonOperator(
            task_id="transform",
            python_callable=transform_misa_crm_data,
            execution_timeout=timedelta(minutes=30),
            retries=1,
            retry_delay=timedelta(minutes=2),
        )
        load_task = PythonOperator(
            task_id="load",
            python_callable=load_misa_crm_data,
        )
        extract_task >> transform_task >> load_task

    with TaskGroup(group_id="tiktok_shop_etl") as tiktok_shop_group:
        extract_tiktok_task = PythonOperator(
            task_id="extract",
            python_callable=extract_tiktok_shop_full_load,
            execution_timeout=timedelta(hours=2),
        )
        transform_tiktok_task = PythonOperator(
            task_id="transform",
            python_callable=transform_tiktok_shop_full_load,
            execution_timeout=timedelta(minutes=30),
        )
        load_tiktok_task = PythonOperator(
            task_id="load",
            python_callable=load_tiktok_shop_full_load,
            execution_timeout=timedelta(minutes=30),
        )
        extract_tiktok_task >> transform_tiktok_task >> load_tiktok_task

    with TaskGroup(group_id="shopee_orders_etl") as shopee_orders_group:
        extract_shopee_task = PythonOperator(
            task_id="extract",
            python_callable=extract_shopee_orders_full_load,
            execution_timeout=timedelta(hours=2),
        )
        transform_shopee_task = PythonOperator(
            task_id="transform",
            python_callable=transform_shopee_orders_full_load,
            execution_timeout=timedelta(minutes=30),
        )
        load_shopee_task = PythonOperator(
            task_id="load",
            python_callable=load_shopee_orders_full_load,
            execution_timeout=timedelta(minutes=30),
        )
        extract_shopee_task >> transform_shopee_task >> load_shopee_task

    end_task = BashOperator(
        task_id="end_full_load",
        bash_command='echo "Parallel Full Load ETL with Consistent Structure completed for all sources."',
    )

    # PARALLEL execution cho MISA CRM, TikTok Shop và Shopee Orders
    start_task >> [misa_crm_group, tiktok_shop_group, shopee_orders_group] >> end_task
