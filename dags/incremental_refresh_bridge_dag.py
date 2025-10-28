"""
Incremental Refresh Bridge DAG - Chạy theo trigger để cập nhật dữ liệu 3 giờ gần nhất
Thiết kế để bù dữ liệu phát sinh trong khi Full Load đang chạy (không bật incre định kỳ)
"""

from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import logging
import json
import os
import sys
import pandas as pd

# Add project root to path
sys.path.append("/opt/airflow/")

# Import các modules
from src.extractors.misa_crm_extractor import MISACRMExtractor
from src.extractors.tiktok_shop_extractor import TikTokShopOrderExtractor
from src.extractors.shopee_orders_extractor import ShopeeOrderExtractor
from src.transformers.misa_crm_transformer import MISACRMTransformer
from src.transformers.tiktok_shop_transformer import TikTokShopOrderTransformer
from src.transformers.shopee_orders_transformer import ShopeeOrderTransformer
from src.loaders.misa_crm_loader import MISACRMLoader
from src.loaders.tiktok_shop_staging_loader import TikTokShopOrderLoader
from src.loaders.shopee_orders_loader import ShopeeOrderLoader
from config.settings import settings

logger = logging.getLogger(__name__)

# DAG Configuration
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 3),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "catchup": False,
}

dag = DAG(
    "incremental_refresh_bridge_dag",
    default_args=default_args,
    description="On-demand incremental refresh for last 3 hours while Full Load runs",
    schedule_interval=None,  # chỉ chạy khi trigger thủ công
    catchup=False,
    max_active_runs=1,  # Không cho chạy parallel
    tags=["etl", "incremental", "bridge", "refresh"],
)

# ========================
# TIKTOK SHOP INCREMENTAL FUNCTIONS
# ========================


def extract_tiktok_shop_incremental(**context):
    """
    Extract dữ liệu TikTok Shop incremental (mặc định 180 phút; có thể override qua dag_run.conf)
    """
    logger.info("🔄 Starting TikTok Shop Incremental Extraction...")

    try:
        extractor = TikTokShopOrderExtractor()

        # Lấy dữ liệu gần nhất theo cấu hình (buffer đã xử lý trong extractor)
        conf = (context.get("dag_run") or {}).conf or {}
        minutes_back = int(conf.get("minutes_back", 180))

        logger.info(f"📅 Incremental lookback: {minutes_back} minutes (bridge mode)")

        # Extract incremental orders từ API
        orders_data = extractor.extract_incremental_orders(minutes_back=minutes_back)

        logger.info(f"✅ Extracted {len(orders_data)} incremental orders")

        # Push to XCom
        context["ti"].xcom_push(key="tiktok_shop_incremental_data", value=orders_data)

        return f"Successfully extracted {len(orders_data)} incremental orders"

    except Exception as e:
        logger.error(f"❌ TikTok Shop incremental extraction failed: {str(e)}")
        raise


def transform_tiktok_shop_incremental(**context):
    """
    Transform dữ liệu TikTok Shop incremental
    """
    logger.info("🔄 Starting TikTok Shop Incremental Transformation...")

    try:
        # Pull data from XCom
        orders_data = context["ti"].xcom_pull(key="tiktok_shop_incremental_data")

        if not orders_data:
            logger.info("📭 No incremental data to transform")
            return "No data to transform"

        # Transform data
        transformer = TikTokShopOrderTransformer()
        transformed_df = transformer.transform_orders_to_dataframe(orders_data)

        logger.info(f"✅ Transformed {len(transformed_df)} incremental records")

        # Serialize DataFrame sang JSON-safe theo ISO 8601 (giữ NaT an toàn)
        payload_json = transformed_df.to_json(
            orient="records", date_format="iso", date_unit="s"
        )
        transformed_data = json.loads(payload_json)
        context["ti"].xcom_push(
            key="tiktok_shop_incremental_transformed", value=transformed_data
        )

        return f"Successfully transformed {len(transformed_df)} incremental records"

    except Exception as e:
        logger.error(f"❌ TikTok Shop incremental transformation failed: {str(e)}")
        raise


def load_tiktok_shop_incremental(**context):
    """
    Load dữ liệu TikTok Shop incremental vào staging
    """
    logger.info("🔄 Starting TikTok Shop Incremental Loading...")

    try:
        # Pull transformed data from XCom
        transformed_data = context["ti"].xcom_pull(
            key="tiktok_shop_incremental_transformed"
        )

        if not transformed_data:
            logger.info("📭 No incremental data to load")
            return "No data to load"

        # Convert back to DataFrame
        import pandas as pd

        df = pd.DataFrame(transformed_data)

        # Load to staging with UPSERT mode (update existing, insert new)
        loader = TikTokShopOrderLoader()

        # Use incremental load with UPSERT logic
        success = loader.load_incremental_orders(df)

        if success:
            logger.info(f"✅ Loaded {len(df)} incremental records to staging")
            return f"Successfully loaded {len(df)} incremental records"
        else:
            logger.error(f"❌ Failed to load {len(df)} incremental records")
            raise Exception(f"Failed to load {len(df)} incremental records")

    except Exception as e:
        logger.error(f"❌ TikTok Shop incremental loading failed: {str(e)}")
        raise


# ========================
# MISA CRM INCREMENTAL FUNCTIONS
# ========================


def extract_misa_crm_incremental(**context):
    """
    Extract dữ liệu MISA CRM incremental (mặc định lookback 180 phút; override qua dag_run.conf)
    """
    logger.info("🔄 Starting MISA CRM Incremental Extraction...")

    try:
        extractor = MISACRMExtractor()

        # Các endpoints cần extract incremental
        endpoints = ["customers", "sale_orders", "contacts", "products", "stocks"]
        incremental_data = {}

        # Lấy timestamp theo cấu hình lookback (phút) - FIXED: Sử dụng timezone-aware datetime
        conf = (context.get("dag_run") or {}).conf or {}
        minutes_back = int(conf.get("minutes_back", 180))
        cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=minutes_back)

        for endpoint in endpoints:
            logger.info(f"📥 Extracting incremental {endpoint}...")

            # Extract với filter modified_date >= cutoff_time
            # Note: Cần implement incremental logic trong MISA extractor
            endpoint_data = extractor.extract_incremental_data(
                endpoint, modified_since=cutoff_time
            )

            incremental_data[endpoint] = endpoint_data
            logger.info(f"✅ Extracted {len(endpoint_data)} incremental {endpoint}")

        # Push to XCom
        context["ti"].xcom_push(
            key="misa_crm_incremental_data", value=json.dumps(incremental_data)
        )

        total_records = sum(len(data) for data in incremental_data.values())
        return f"Successfully extracted {total_records} incremental MISA CRM records"

    except Exception as e:
        logger.error(f"❌ MISA CRM incremental extraction failed: {str(e)}")
        raise


def transform_misa_crm_incremental(**context):
    """
    Transform dữ liệu MISA CRM incremental
    """
    logger.info("🔄 Starting MISA CRM Incremental Transformation...")

    try:
        # Pull data from XCom
        raw_data = context["ti"].xcom_pull(key="misa_crm_incremental_data")
        incremental_data = json.loads(raw_data) if raw_data else {}

        if not any(incremental_data.values()):
            logger.info("📭 No incremental MISA CRM data to transform")
            return "No data to transform"

        # Transform data
        transformer = MISACRMTransformer()
        # Tạo batch_id dựa trên execution timestamp để truy vết phiên chạy
        batch_id = f"inc_{context.get('ts_nodash') or datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
        transformed_data = transformer.transform_all_endpoints(
            incremental_data, batch_id
        )

        # Convert DataFrames to dict for XCom
        serialized_data = {}
        total_records = 0

        for key, df in transformed_data.items():
            if df is not None and not df.empty:
                # Serialize DataFrame sang JSON-safe để tránh lỗi Timestamp/NaN

                payload_json = df.to_json(
                    orient="records", date_format="iso", date_unit="s"
                )
                serialized_data[key] = json.loads(payload_json)
                total_records += len(df)

        context["ti"].xcom_push(
            key="misa_crm_incremental_transformed", value=json.dumps(serialized_data)
        )

        logger.info(f"✅ Transformed {total_records} incremental MISA CRM records")
        return f"Successfully transformed {total_records} incremental records"

    except Exception as e:
        logger.error(f"❌ MISA CRM incremental transformation failed: {str(e)}")
        raise


def load_misa_crm_incremental(**context):
    """
    Load dữ liệu MISA CRM incremental vào staging
    """
    logger.info("🔄 Starting MISA CRM Incremental Loading...")

    try:
        # Pull transformed data from XCom
        raw_data = context["ti"].xcom_pull(key="misa_crm_incremental_transformed")
        transformed_data = json.loads(raw_data) if raw_data else {}

        if not transformed_data:
            logger.info("📭 No incremental MISA CRM data to load")
            return "No data to load"

        # Load to staging
        loader = MISACRMLoader()
        total_loaded = 0

        import pandas as pd

        for table_name, records in transformed_data.items():
            if records:
                df = pd.DataFrame(records)

                # Use incremental load with UPSERT logic
                success = loader.load_incremental_data(table_name, df)

                if success:
                    total_loaded += len(df)
                    logger.info(f"✅ Loaded {len(df)} incremental {table_name} records")
                else:
                    logger.error(
                        f"❌ Failed to load {len(df)} incremental {table_name} records"
                    )
                    raise Exception(f"Failed to load incremental {table_name} records")

        logger.info(f"✅ Total loaded {total_loaded} incremental MISA CRM records")
        return f"Successfully loaded {total_loaded} incremental records"

    except Exception as e:
        logger.error(f"❌ MISA CRM incremental loading failed: {str(e)}")
        raise


# ========================
# SHOPEE ORDERS INCREMENTAL FUNCTIONS
# ========================


def extract_shopee_orders_incremental(**context):
    """
    Extract dữ liệu Shopee Orders incremental (mặc định 180 phút; override qua dag_run.conf)
    """
    logger.info("🔄 Starting Shopee Orders Incremental Extraction...")

    try:
        extractor = ShopeeOrderExtractor()

        # Lấy dữ liệu gần nhất theo cấu hình (buffer đã xử lý trong extractor)
        conf = (context.get("dag_run") or {}).conf or {}
        minutes_back = int(conf.get("minutes_back", 180))

        logger.info(f"📅 Incremental lookback: {minutes_back} minutes (bridge mode)")

        # Extract incremental orders từ API
        orders_data = extractor.extract_orders_incremental(minutes_back=minutes_back)

        logger.info(f"✅ Extracted {len(orders_data)} incremental orders")

        # Push to XCom
        context["ti"].xcom_push(key="shopee_orders_incremental_data", value=orders_data)

        return f"Successfully extracted {len(orders_data)} incremental orders"

    except Exception as e:
        logger.error(f"❌ Shopee Orders incremental extraction failed: {str(e)}")
        raise


def transform_shopee_orders_incremental(**context):
    """
    Transform dữ liệu Shopee Orders incremental
    """
    logger.info("🔄 Starting Shopee Orders Incremental Transformation...")

    try:
        # Pull data from XCom
        orders_data = context["ti"].xcom_pull(key="shopee_orders_incremental_data")

        if not orders_data:
            logger.info("📭 No incremental data to transform")
            return "No data to transform"

        # Transform data thành 12 DataFrame theo ERD
        transformer = ShopeeOrderTransformer()
        tables_to_dfs = transformer.transform_orders_to_dataframes(orders_data)

        # Serialize từng bảng sang JSON-safe và đẩy XCom theo key riêng

        total_rows = 0
        for table_name, df in tables_to_dfs.items():
            if df is not None and not df.empty:
                payload_json = df.to_json(
                    orient="records", date_format="iso", date_unit="s"
                )
                table_records = json.loads(payload_json)
                context["ti"].xcom_push(
                    key=f"shopee_incremental_transformed__{table_name}",
                    value=table_records,
                )
                logger.info(
                    f"✅ Transformed {len(df)} rows for Shopee table '{table_name}'"
                )
                total_rows += len(df)
            else:
                # Đẩy danh sách rỗng để bước load biết bỏ qua
                context["ti"].xcom_push(
                    key=f"shopee_incremental_transformed__{table_name}", value=[]
                )

        return f"Successfully transformed Shopee incremental data into 12 tables, total {total_rows} rows"

    except Exception as e:
        logger.error(f"❌ Shopee Orders incremental transformation failed: {str(e)}")
        raise


def load_shopee_orders_incremental(**context):
    """
    Load dữ liệu Shopee Orders incremental vào staging
    """
    logger.info("🔄 Starting Shopee Orders Incremental Loading...")

    try:
        # Pull transformed data cho từng bảng từ XCom
        import pandas as pd

        loader = ShopeeOrderLoader()

        load_order = [
            "orders",
            "recipient_address",
            "order_items",
            "order_item_locations",
            "packages",
            "package_items",
        ]

        total_loaded = 0
        for table_name in load_order:
            records = (
                context["ti"].xcom_pull(
                    key=f"shopee_incremental_transformed__{table_name}"
                )
                or []
            )

            if not records:
                logger.info(f"📭 No incremental data for Shopee.{table_name}, skipping")
                continue

            df_table = pd.DataFrame(records)

            # Thay append bằng UPSERT per-table
            ok = loader.upsert_table(df_table, table_name)
            if not ok:
                raise Exception(
                    f"Failed to load Shopee incremental table '{table_name}' ({len(df_table)} rows)"
                )
            total_loaded += len(df_table)

        logger.info(
            f"✅ Loaded Shopee incremental data into 12 tables, total {total_loaded} rows"
        )
        return f"Successfully loaded Shopee incremental data: {total_loaded} rows"

    except Exception as e:
        logger.error(f"❌ Shopee Orders incremental loading failed: {str(e)}")
        raise


# ========================
# DAG STRUCTURE
# ========================

# Start task
start_incremental = EmptyOperator(task_id="start_incremental", dag=dag)

# TikTok Shop Incremental Pipeline
tiktok_extract_incremental = PythonOperator(
    task_id="tiktok_shop_extract_incremental",
    python_callable=extract_tiktok_shop_incremental,
    dag=dag,
)

tiktok_transform_incremental = PythonOperator(
    task_id="tiktok_shop_transform_incremental",
    python_callable=transform_tiktok_shop_incremental,
    dag=dag,
)

tiktok_load_incremental = PythonOperator(
    task_id="tiktok_shop_load_incremental",
    python_callable=load_tiktok_shop_incremental,
    dag=dag,
)

# MISA CRM Incremental Pipeline
misa_extract_incremental = PythonOperator(
    task_id="misa_crm_extract_incremental",
    python_callable=extract_misa_crm_incremental,
    dag=dag,
)

misa_transform_incremental = PythonOperator(
    task_id="misa_crm_transform_incremental",
    python_callable=transform_misa_crm_incremental,
    dag=dag,
)

misa_load_incremental = PythonOperator(
    task_id="misa_crm_load_incremental",
    python_callable=load_misa_crm_incremental,
    dag=dag,
)

# Shopee Orders Incremental Pipeline
shopee_extract_incremental = PythonOperator(
    task_id="shopee_orders_extract_incremental",
    python_callable=extract_shopee_orders_incremental,
    dag=dag,
)

shopee_transform_incremental = PythonOperator(
    task_id="shopee_orders_transform_incremental",
    python_callable=transform_shopee_orders_incremental,
    dag=dag,
)

shopee_load_incremental = PythonOperator(
    task_id="shopee_orders_load_incremental",
    python_callable=load_shopee_orders_incremental,
    dag=dag,
)

# End task
end_incremental = EmptyOperator(task_id="end_incremental", dag=dag)

# ========================
# TASK DEPENDENCIES
# ========================

# Parallel execution cho TikTok Shop, MISA CRM và Shopee Orders
start_incremental >> [
    tiktok_extract_incremental,
    misa_extract_incremental,
    shopee_extract_incremental,
]

# TikTok Shop pipeline
tiktok_extract_incremental >> tiktok_transform_incremental >> tiktok_load_incremental

# MISA CRM pipeline
misa_extract_incremental >> misa_transform_incremental >> misa_load_incremental

# Shopee Orders pipeline
shopee_extract_incremental >> shopee_transform_incremental >> shopee_load_incremental

# All pipelines must complete before ending
[
    tiktok_load_incremental,
    misa_load_incremental,
    shopee_load_incremental,
] >> end_incremental
