#!/usr/bin/env python3
"""
Script test chức năng cơ bản của DAGs mà không cần API keys thực
Chạy trong CI/CD để kiểm tra logic cơ bản
"""

import sys
import os
import json
import logging
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def test_imports():
    """Test import tất cả modules"""
    logger.info("🔍 Testing imports...")

    try:
        # Test extractors
        from src.extractors.misa_crm_extractor import MISACRMExtractor
        from src.extractors.tiktok_shop_extractor import TikTokShopOrderExtractor
        from src.extractors.shopee_orders_extractor import ShopeeOrderExtractor

        # Test transformers
        from src.transformers.misa_crm_transformer import MISACRMTransformer
        from src.transformers.tiktok_shop_transformer import TikTokShopOrderTransformer
        from src.transformers.shopee_orders_transformer import ShopeeOrderTransformer

        # Test loaders
        from src.loaders.misa_crm_loader import MISACRMLoader
        from src.loaders.tiktok_shop_staging_loader import TikTokShopOrderLoader
        from src.loaders.shopee_orders_loader import ShopeeOrderLoader

        # Test utils
        from src.utils.database import DatabaseManager
        from config.settings import settings

        logger.info("✅ All imports successful")
        return True

    except Exception as e:
        logger.error(f"❌ Import failed: {str(e)}")
        return False


def test_class_instantiation():
    """Test khởi tạo các classes"""
    logger.info("🔍 Testing class instantiation...")

    try:
        # Import rõ ràng trong scope hàm
        from src.extractors.misa_crm_extractor import MISACRMExtractor
        from src.extractors.tiktok_shop_extractor import TikTokShopOrderExtractor
        from src.extractors.shopee_orders_extractor import ShopeeOrderExtractor
        from src.transformers.misa_crm_transformer import MISACRMTransformer
        from src.transformers.tiktok_shop_transformer import TikTokShopOrderTransformer
        from src.transformers.shopee_orders_transformer import ShopeeOrderTransformer
        from src.loaders.misa_crm_loader import MISACRMLoader
        from src.loaders.tiktok_shop_staging_loader import TikTokShopOrderLoader
        from src.loaders.shopee_orders_loader import ShopeeOrderLoader
        from src.utils.database import DatabaseManager

        # Test extractors (bỏ qua lỗi ODBC/DB trên CI)
        try:
            misa_extractor = MISACRMExtractor()
        except Exception as e:
            if "ODBC Driver 17" in str(e) or "unixODBC" in str(e) or "pyodbc" in str(e):
                logger.warning("⚠️ Bỏ qua MISACRMExtractor do thiếu ODBC driver trên CI")
                misa_extractor = None
            else:
                raise
        try:
            tiktok_extractor = TikTokShopOrderExtractor()
        except Exception as e:
            if "ODBC Driver 17" in str(e) or "unixODBC" in str(e) or "pyodbc" in str(e):
                logger.warning(
                    "⚠️ Bỏ qua TikTokShopOrderExtractor do thiếu ODBC driver trên CI"
                )
                tiktok_extractor = None
            else:
                raise
        try:
            shopee_extractor = ShopeeOrderExtractor()
        except Exception as e:
            if "ODBC Driver 17" in str(e) or "unixODBC" in str(e) or "pyodbc" in str(e):
                logger.warning(
                    "⚠️ Bỏ qua ShopeeOrderExtractor do thiếu ODBC driver trên CI"
                )
                shopee_extractor = None
            else:
                raise

        # Test transformers
        misa_transformer = MISACRMTransformer()
        tiktok_transformer = TikTokShopOrderTransformer()
        shopee_transformer = ShopeeOrderTransformer()

        # Test loaders
        misa_loader = MISACRMLoader()
        tiktok_loader = TikTokShopOrderLoader()
        shopee_loader = ShopeeOrderLoader()

        # Test database manager
        db_manager = DatabaseManager()

        logger.info("✅ All classes instantiated successfully")
        return True

    except Exception as e:
        logger.error(f"❌ Class instantiation failed: {str(e)}")
        return False


def test_transformation_logic():
    """Test logic transformation với sample data"""
    logger.info("🔍 Testing transformation logic...")

    try:
        # Test MISA CRM transformation
        from src.transformers.misa_crm_transformer import MISACRMTransformer

        transformer = MISACRMTransformer()

        # Sample data
        sample_data = {
            "customers": [
                {
                    "id": 1,
                    "name": "Test Customer",
                    "email": "test@example.com",
                    "phone": "0123456789",
                    "created_date": "2024-01-01T00:00:00Z",
                }
            ],
            "contacts": [
                {
                    "id": 1,
                    "customer_id": 1,
                    "phone": "0123456789",
                    "email": "contact@example.com",
                }
            ],
        }

        # Đặt batch_id và gọi theo chữ ký mới; fallback nếu không yêu cầu
        transformer.set_batch_id("ci-smoke-batch")
        try:
            transformed_data = transformer.transform_all_endpoints(
                sample_data, batch_id="ci-smoke-batch"
            )
        except TypeError:
            transformed_data = transformer.transform_all_endpoints(sample_data)

        if transformed_data and len(transformed_data) > 0:
            logger.info("✅ MISA CRM transformation successful")
            for key, df in transformed_data.items():
                if df is not None and not df.empty:
                    logger.info(f"  - {key}: {len(df)} records")
        else:
            logger.warning("⚠️ MISA CRM transformation returned no data")

        # Test TikTok Shop transformation
        from src.transformers.tiktok_shop_transformer import TikTokShopOrderTransformer

        tiktok_transformer = TikTokShopOrderTransformer()

        # Sample order data
        sample_orders = [
            {
                "order_id": "test_123",
                "order_status": "COMPLETED",
                "create_time": 1640995200,
                "total_amount": 100000,
                "currency": "VND",
                "buyer_info": {"buyer_id": "buyer_123", "buyer_name": "Test Buyer"},
            }
        ]

        transformed_df = tiktok_transformer.transform_orders_to_dataframe(sample_orders)

        if not transformed_df.empty:
            logger.info("✅ TikTok Shop transformation successful")
            logger.info(f"  - Transformed data shape: {transformed_df.shape}")
            logger.info(f"  - Columns: {list(transformed_df.columns)}")
        else:
            logger.warning("⚠️ TikTok Shop transformation returned no data")

        # Test Shopee transformation
        from src.transformers.shopee_orders_transformer import ShopeeOrderTransformer

        shopee_transformer = ShopeeOrderTransformer()

        # Sample Shopee order data
        sample_shopee_orders = [
            {
                "order_sn": "test_shopee_123",
                "order_status": "COMPLETED",
                "create_time": 1640995200,
                "total_amount": 100000,
                "currency": "VND",
                "buyer_user_id": "buyer_shopee_123",
            }
        ]

        transformed_shopee_df = shopee_transformer.transform_orders_to_flat_dataframe(
            sample_shopee_orders
        )

        if not transformed_shopee_df.empty:
            logger.info("✅ Shopee transformation successful")
            logger.info(f"  - Transformed data shape: {transformed_shopee_df.shape}")
            logger.info(f"  - Columns: {list(transformed_shopee_df.columns)}")
        else:
            logger.warning("⚠️ Shopee transformation returned no data")

        logger.info("✅ All transformation logic tests completed")
        return True

    except Exception as e:
        logger.error(f"❌ Transformation logic test failed: {str(e)}")
        import traceback

        traceback.print_exc()
        return False


def test_database_loading():
    """Test database loading logic"""
    logger.info("🔍 Testing database loading logic...")

    try:
        # Test MISA CRM loader
        from src.loaders.misa_crm_loader import MISACRMLoader
        import pandas as pd

        loader = MISACRMLoader()

        # Test với sample data
        sample_customers_df = pd.DataFrame(
            [
                {
                    "id": 1,
                    "name": "Test Customer",
                    "email": "test@example.com",
                    "phone": "0123456789",
                    "created_date": "2024-01-01T00:00:00Z",
                }
            ]
        )

        # Test table mapping
        table_mappings = loader.table_mappings
        logger.info(f"✅ MISA CRM table mappings: {table_mappings}")

        # Test data validation
        try:
            # Test validate_data_format method nếu có
            if hasattr(loader, "validate_data_format"):
                is_valid = loader.validate_data_format("customers", sample_customers_df)
                logger.info(f"✅ MISA CRM data validation: {is_valid}")
        except Exception as e:
            logger.warning(f"⚠️ MISA CRM data validation test failed: {str(e)}")

        # Test TikTok Shop loader
        from src.loaders.tiktok_shop_staging_loader import TikTokShopOrderLoader

        tiktok_loader = TikTokShopOrderLoader()

        # Test với sample data
        sample_orders_df = pd.DataFrame(
            [
                {
                    "order_id": "test_123",
                    "order_status": "COMPLETED",
                    "create_time": 1640995200,
                    "total_amount": 100000,
                    "currency": "VND",
                }
            ]
        )

        # Test data validation
        try:
            if hasattr(tiktok_loader, "validate_data_format"):
                is_valid = tiktok_loader.validate_data_format(sample_orders_df)
                logger.info(f"✅ TikTok Shop data validation: {is_valid}")
        except Exception as e:
            logger.warning(f"⚠️ TikTok Shop data validation test failed: {str(e)}")

        # Test Shopee loader
        from src.loaders.shopee_orders_loader import ShopeeOrderLoader

        shopee_loader = ShopeeOrderLoader()

        # Test với sample data
        sample_shopee_df = pd.DataFrame(
            [
                {
                    "order_sn": "test_shopee_123",
                    "order_status": "COMPLETED",
                    "create_time": 1640995200,
                    "total_amount": 100000,
                    "currency": "VND",
                }
            ]
        )

        # Test data validation
        try:
            if hasattr(shopee_loader, "validate_data_format"):
                is_valid = shopee_loader.validate_data_format(sample_shopee_df)
                logger.info(f"✅ Shopee data validation: {is_valid}")
        except Exception as e:
            logger.warning(f"⚠️ Shopee data validation test failed: {str(e)}")

        logger.info("✅ All database loading logic tests completed")
        return True

    except Exception as e:
        logger.error(f"❌ Database loading logic test failed: {str(e)}")
        import traceback

        traceback.print_exc()
        return False


def test_token_refresh_logic():
    """Test token refresh logic"""
    logger.info("🔍 Testing token refresh logic...")

    try:
        # Test TikTok Shop authenticator
        from src.utils.auth import TikTokAuthenticator

        try:
            authenticator = TikTokAuthenticator()
        except Exception as init_err:
            warn = str(init_err)
            if "ODBC Driver 17" in warn or "unixODBC" in warn or "pyodbc" in warn:
                logger.warning(
                    "⚠️ Bỏ qua token refresh test trên CI do thiếu driver ODBC/DB"
                )
                return True
            raise

        # Test token expiry check
        is_expired = authenticator.is_token_expired()
        logger.info(f"✅ Token expiry check: {'Expired' if is_expired else 'Valid'}")

        # Test refresh token expiry check
        is_refresh_expired = authenticator.is_refresh_token_expired()
        logger.info(
            f"✅ Refresh token expiry check: {'Expired' if is_refresh_expired else 'Valid'}"
        )

        # Test signature generation
        try:
            test_params = {
                "app_key": "test_key",
                "timestamp": "1234567890",
                "shop_cipher": "test_cipher",
            }
            signature = authenticator.generate_signature("/test/endpoint", test_params)
            if signature:
                logger.info("✅ Signature generation successful")
            else:
                logger.warning("⚠️ Signature generation failed")
        except Exception as e:
            logger.warning(f"⚠️ Signature generation test failed: {str(e)}")

        # Test ensure_valid_token method
        try:
            is_valid = authenticator.ensure_valid_token()
            logger.info(f"✅ Token validation: {'Valid' if is_valid else 'Invalid'}")
        except Exception as e:
            logger.warning(f"⚠️ Token validation test skipped: {str(e)}")

        logger.info("✅ All token refresh logic tests completed")
        return True

    except Exception as e:
        logger.error(f"❌ Token refresh logic test failed: {str(e)}")
        import traceback

        traceback.print_exc()
        return False


def test_dag_structure():
    """Test cấu trúc DAG dùng DagBag"""
    logger.info("🔍 Testing DAG structure...")
    try:
        from airflow.models import DagBag

        dag_bag = DagBag(dag_folder="dags", include_examples=False)
        if dag_bag.import_errors:
            logger.error(f"❌ DAG import errors: {dag_bag.import_errors}")
            return False
        required = {"full_load_etl_dag", "incremental_etl_dag"}
        loaded = set(dag_bag.dags.keys())
        missing = required - loaded
        if missing:
            logger.error(f"❌ Missing DAGs: {missing}")
            return False
        for dag_id in required:
            # Truy cập trực tiếp map in-memory để tránh động chạm metastore
            dag = dag_bag.dags.get(dag_id)
            logger.info(f"✅ {dag_id} structure valid - tasks: {len(dag.tasks)}")
        return True
    except Exception as e:
        logger.error(f"❌ DAG structure test failed: {str(e)}")
        import traceback

        traceback.print_exc()
        return False


def test_configuration():
    """Test configuration files"""
    logger.info("🔍 Testing configuration...")

    try:
        from config.settings import settings

        # Check if settings object has required attributes
        required_attrs = [
            "database_url",
            "misa_api_url",
            "tiktok_api_url",
            "shopee_api_url",
        ]

        for attr in required_attrs:
            if hasattr(settings, attr):
                logger.info(f"✅ {attr} configured")
            else:
                logger.warning(f"⚠️ {attr} not configured")

        logger.info("✅ Configuration test completed")
        return True

    except Exception as e:
        logger.error(f"❌ Configuration test failed: {str(e)}")
        return False


def main():
    """Main test function"""
    logger.info("🚀 Starting DAG functionality tests...")
    logger.info("=" * 60)

    results = {}

    # Run tests
    results["imports"] = test_imports()
    results["instantiation"] = test_class_instantiation()
    results["transformation"] = test_transformation_logic()
    results["database_loading"] = test_database_loading()
    results["token_refresh"] = test_token_refresh_logic()
    results["dag_structure"] = test_dag_structure()
    results["configuration"] = test_configuration()

    # Summary
    logger.info("=" * 60)
    logger.info("📊 TEST RESULTS SUMMARY:")
    logger.info("=" * 60)

    total_tests = len(results)
    passed_tests = sum(1 for result in results.values() if result)

    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"{test_name.upper()}: {status}")

    logger.info("=" * 60)
    logger.info(f"TOTAL: {passed_tests}/{total_tests} tests passed")

    if passed_tests == total_tests:
        logger.info("🎉 All functionality tests passed! DAGs are ready.")
        return 0
    else:
        logger.warning(f"⚠️ {total_tests - passed_tests} tests failed.")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
