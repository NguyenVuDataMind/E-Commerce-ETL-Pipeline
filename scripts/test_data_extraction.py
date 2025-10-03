#!/usr/bin/env python3
"""
Script test thực tế việc lấy dữ liệu từ các API
Chạy script này để kiểm tra xem DAGs có thực sự lấy được dữ liệu hay không
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


def test_misa_crm_extraction():
    """Test MISA CRM data extraction"""
    logger.info("🔍 Testing MISA CRM data extraction...")

    try:
        from src.extractors.misa_crm_extractor import MISACRMExtractor

        extractor = MISACRMExtractor()

        # Test với một endpoint đơn giản
        logger.info("Testing customers endpoint...")
        customers_data = extractor.extract_all_data_from_endpoint("customers")

        if customers_data:
            logger.info(f"✅ MISA CRM customers: {len(customers_data)} records")
            logger.info(
                f"Sample data: {json.dumps(customers_data[0] if customers_data else {}, indent=2)}"
            )
            return True
        else:
            logger.warning("⚠️ MISA CRM customers: No data returned")
            return False

    except Exception as e:
        logger.error(f"❌ MISA CRM extraction failed: {str(e)}")
        return False


def test_tiktok_shop_extraction():
    """Test TikTok Shop data extraction"""
    logger.info("🔍 Testing TikTok Shop data extraction...")

    try:
        from src.extractors.tiktok_shop_extractor import TikTokShopOrderExtractor

        extractor = TikTokShopOrderExtractor()

        # Test với thời gian ngắn (1 ngày gần đây)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)

        start_timestamp = int(start_date.timestamp())
        end_timestamp = int(end_date.timestamp())

        logger.info(
            f"Testing orders from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        )

        # Test với batch size nhỏ
        orders_data = []
        for orders_batch in extractor.stream_orders_lightweight(
            start_timestamp, end_timestamp, batch_size=5
        ):
            orders_data.extend(orders_batch)
            if len(orders_data) >= 10:  # Chỉ lấy 10 orders để test
                break

        if orders_data:
            logger.info(f"✅ TikTok Shop orders: {len(orders_data)} records")
            logger.info(
                f"Sample data: {json.dumps(orders_data[0] if orders_data else {}, indent=2)}"
            )
            return True
        else:
            logger.warning("⚠️ TikTok Shop orders: No data returned")
            return False

    except Exception as e:
        logger.error(f"❌ TikTok Shop extraction failed: {str(e)}")
        return False


def test_shopee_orders_extraction():
    """Test Shopee orders data extraction"""
    logger.info("🔍 Testing Shopee orders data extraction...")

    try:
        from src.extractors.shopee_orders_extractor import ShopeeOrderExtractor

        extractor = ShopeeOrderExtractor()

        # Test với thời gian ngắn (1 ngày gần đây)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)

        logger.info(
            f"Testing orders from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        )

        # Test extraction
        orders_data = extractor.extract_orders_full_load(start_date, end_date)

        if orders_data:
            logger.info(f"✅ Shopee orders: {len(orders_data)} records")
            logger.info(
                f"Sample data: {json.dumps(orders_data[0] if orders_data else {}, indent=2)}"
            )
            return True
        else:
            logger.warning("⚠️ Shopee orders: No data returned")
            return False

    except Exception as e:
        logger.error(f"❌ Shopee orders extraction failed: {str(e)}")
        return False


def test_database_connection():
    """Test database connection"""
    logger.info("🔍 Testing database connection...")

    try:
        from src.utils.database import DatabaseManager

        db_manager = DatabaseManager()
        connection_string = db_manager.get_connection_string()

        if connection_string:
            logger.info("✅ Database connection string created successfully")
            logger.info(f"Connection format: {connection_string[:50]}...")

            # Test actual connection
            try:
                connection = db_manager.get_connection()
                if connection:
                    logger.info("✅ Database connection established successfully")
                    connection.close()
                    return True
                else:
                    logger.warning("⚠️ Database connection failed")
                    return False
            except Exception as e:
                logger.warning(f"⚠️ Database connection failed: {str(e)}")
                return False
        else:
            logger.warning("⚠️ Database connection string not created")
            return False

    except Exception as e:
        logger.error(f"❌ Database connection test failed: {str(e)}")
        return False


def test_data_transformation():
    """Test data transformation logic"""
    logger.info("🔍 Testing data transformation logic...")

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

        transformed_data = transformer.transform_all_endpoints(sample_data)

        if transformed_data and len(transformed_data) > 0:
            logger.info("✅ MISA CRM transformation successful")
            for key, df in transformed_data.items():
                if df is not None and not df.empty:
                    logger.info(f"  - {key}: {len(df)} records")
            return True
        else:
            logger.warning("⚠️ MISA CRM transformation returned no data")
            return False

    except Exception as e:
        logger.error(f"❌ Data transformation test failed: {str(e)}")
        return False


def main():
    """Main test function"""
    logger.info("🚀 Starting comprehensive ETL pipeline tests...")
    logger.info("=" * 60)

    results = {}

    # Test API extractions
    results["misa_crm"] = test_misa_crm_extraction()
    results["tiktok_shop"] = test_tiktok_shop_extraction()
    results["shopee_orders"] = test_shopee_orders_extraction()

    # Test database connection
    results["database"] = test_database_connection()

    # Test data transformation
    results["transformation"] = test_data_transformation()

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
        logger.info("🎉 All tests passed! ETL pipeline is ready.")
        return 0
    else:
        logger.warning(
            f"⚠️ {total_tests - passed_tests} tests failed. Please check configuration."
        )
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
