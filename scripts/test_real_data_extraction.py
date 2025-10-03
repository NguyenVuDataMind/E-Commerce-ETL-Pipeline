#!/usr/bin/env python3
"""
Script test thực tế việc lấy dữ liệu từ các API (chạy local)
Cần cấu hình API keys trước khi chạy
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


def test_misa_crm_real_extraction():
    """Test MISA CRM data extraction thực tế"""
    logger.info("🔍 Testing MISA CRM real data extraction...")

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

            # Test thêm endpoints khác
            endpoints = ["contacts", "sale_orders", "products"]
            for endpoint in endpoints:
                try:
                    data = extractor.extract_all_data_from_endpoint(endpoint)
                    logger.info(f"✅ MISA CRM {endpoint}: {len(data)} records")
                except Exception as e:
                    logger.warning(f"⚠️ MISA CRM {endpoint}: {str(e)}")

            return True
        else:
            logger.warning("⚠️ MISA CRM customers: No data returned")
            return False

    except Exception as e:
        logger.error(f"❌ MISA CRM extraction failed: {str(e)}")
        return False


def test_tiktok_shop_real_extraction():
    """Test TikTok Shop data extraction thực tế"""
    logger.info("🔍 Testing TikTok Shop real data extraction...")

    try:
        from src.extractors.tiktok_shop_extractor import TikTokShopOrderExtractor

        extractor = TikTokShopOrderExtractor()

        # Test với thời gian ngắn (7 ngày gần đây)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)

        start_timestamp = int(start_date.timestamp())
        end_timestamp = int(end_date.timestamp())

        logger.info(
            f"Testing orders from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        )

        # Test với batch size nhỏ
        orders_data = []
        batch_count = 0

        for orders_batch in extractor.stream_orders_lightweight(
            start_timestamp, end_timestamp, batch_size=10
        ):
            orders_data.extend(orders_batch)
            batch_count += 1
            logger.info(
                f"Batch {batch_count}: {len(orders_batch)} orders (total: {len(orders_data)})"
            )

            # Chỉ lấy 50 orders để test
            if len(orders_data) >= 50:
                break

        if orders_data:
            logger.info(
                f"✅ TikTok Shop orders: {len(orders_data)} records from {batch_count} batches"
            )
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


def test_shopee_orders_real_extraction():
    """Test Shopee orders data extraction thực tế"""
    logger.info("🔍 Testing Shopee orders real data extraction...")

    try:
        from src.extractors.shopee_orders_extractor import ShopeeOrderExtractor

        extractor = ShopeeOrderExtractor()

        # Test với thời gian ngắn (7 ngày gần đây)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)

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


def test_full_etl_pipeline():
    """Test toàn bộ ETL pipeline với dữ liệu thực"""
    logger.info("🔍 Testing full ETL pipeline...")

    try:
        # Test MISA CRM full pipeline
        logger.info("Testing MISA CRM full pipeline...")
        from src.extractors.misa_crm_extractor import MISACRMExtractor
        from src.transformers.misa_crm_transformer import MISACRMTransformer
        from src.loaders.misa_crm_loader import MISACRMLoader

        # Extract
        extractor = MISACRMExtractor()
        raw_data = {}
        endpoints = ["customers", "contacts"]

        for endpoint in endpoints:
            try:
                data = extractor.extract_all_data_from_endpoint(endpoint)
                raw_data[endpoint] = data
                logger.info(f"Extracted {len(data)} {endpoint} records")
            except Exception as e:
                logger.warning(f"Failed to extract {endpoint}: {str(e)}")

        if raw_data:
            # Transform
            transformer = MISACRMTransformer()
            transformed_data = transformer.transform_all_endpoints(raw_data)

            if transformed_data:
                logger.info("✅ MISA CRM transformation successful")

                # Test load (không thực sự load vào DB)
                loader = MISACRMLoader()
                logger.info("✅ MISA CRM loader instantiated successfully")

                return True
            else:
                logger.warning("⚠️ MISA CRM transformation failed")
                return False
        else:
            logger.warning("⚠️ MISA CRM extraction failed")
            return False

    except Exception as e:
        logger.error(f"❌ Full ETL pipeline test failed: {str(e)}")
        return False


def main():
    """Main test function"""
    logger.info("🚀 Starting REAL DATA extraction tests...")
    logger.info("⚠️  WARNING: This will make real API calls!")
    logger.info("=" * 60)

    # Ask for confirmation
    response = input("Do you want to proceed with real API calls? (y/N): ")
    if response.lower() != "y":
        logger.info("Test cancelled by user")
        return 0

    results = {}

    # Test real extractions
    results["misa_crm"] = test_misa_crm_real_extraction()
    results["tiktok_shop"] = test_tiktok_shop_real_extraction()
    results["shopee_orders"] = test_shopee_orders_real_extraction()
    results["full_pipeline"] = test_full_etl_pipeline()

    # Summary
    logger.info("=" * 60)
    logger.info("📊 REAL DATA TEST RESULTS SUMMARY:")
    logger.info("=" * 60)

    total_tests = len(results)
    passed_tests = sum(1 for result in results.values() if result)

    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"{test_name.upper()}: {status}")

    logger.info("=" * 60)
    logger.info(f"TOTAL: {passed_tests}/{total_tests} tests passed")

    if passed_tests == total_tests:
        logger.info("🎉 All real data tests passed! APIs are working correctly.")
        return 0
    else:
        logger.warning(
            f"⚠️ {total_tests - passed_tests} tests failed. Please check API configuration."
        )
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
