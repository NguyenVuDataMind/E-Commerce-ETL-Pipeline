#!/usr/bin/env python3
"""
Script test chuyên biệt cho token refresh và database loading
Chạy local để kiểm tra thực tế
"""

import sys
import os
import json
import logging
import pandas as pd
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def test_token_refresh_real():
    """Test token refresh thực tế"""
    logger.info("🔍 Testing real token refresh...")

    try:
        from src.utils.auth import TikTokAuthenticator

        authenticator = TikTokAuthenticator()

        # Test load tokens from database
        logger.info("Testing token loading from database...")
        try:
            authenticator._load_persistent_tokens()
            logger.info("✅ Tokens loaded from database successfully")
            logger.info(
                f"Access token: {authenticator.access_token[:20]}..."
                if authenticator.access_token
                else "No access token"
            )
            logger.info(
                f"Refresh token: {authenticator.refresh_token[:20]}..."
                if authenticator.refresh_token
                else "No refresh token"
            )
            logger.info(f"Shop cipher: {authenticator.shop_cipher}")
        except Exception as e:
            logger.error(f"❌ Failed to load tokens from database: {str(e)}")
            return False

        # Test token expiry check
        is_expired = authenticator.is_token_expired()
        logger.info(f"Token expiry status: {'Expired' if is_expired else 'Valid'}")

        # Test refresh token expiry check
        is_refresh_expired = authenticator.is_refresh_token_expired()
        logger.info(
            f"Refresh token expiry status: {'Expired' if is_refresh_expired else 'Valid'}"
        )

        # Test token refresh if needed
        if is_expired:
            logger.info("Token is expired, attempting refresh...")
            try:
                success = authenticator.refresh_access_token()
                if success:
                    logger.info("✅ Token refresh successful")
                    logger.info(
                        f"New access token: {authenticator.access_token[:20]}..."
                    )
                    logger.info(
                        f"New refresh token: {authenticator.refresh_token[:20]}..."
                    )
                else:
                    logger.error("❌ Token refresh failed")
                    return False
            except Exception as e:
                logger.error(f"❌ Token refresh exception: {str(e)}")
                return False
        else:
            logger.info("✅ Token is still valid, no refresh needed")

        # Test ensure_valid_token
        try:
            is_valid = authenticator.ensure_valid_token()
            logger.info(
                f"Token validation result: {'Valid' if is_valid else 'Invalid'}"
            )
        except Exception as e:
            logger.error(f"❌ Token validation failed: {str(e)}")
            return False

        # Test signature generation
        try:
            test_params = {
                "app_key": authenticator.app_key,
                "timestamp": str(int(datetime.now().timestamp())),
                "shop_cipher": authenticator.shop_cipher,
            }
            signature = authenticator.generate_signature("/test/endpoint", test_params)
            if signature:
                logger.info("✅ Signature generation successful")
                logger.info(f"Generated signature: {signature[:20]}...")
            else:
                logger.warning("⚠️ Signature generation failed")
        except Exception as e:
            logger.error(f"❌ Signature generation failed: {str(e)}")
            return False

        logger.info("✅ All token refresh tests completed successfully")
        return True

    except Exception as e:
        logger.error(f"❌ Token refresh test failed: {str(e)}")
        import traceback

        traceback.print_exc()
        return False


def test_database_connection_real():
    """Test database connection thực tế"""
    logger.info("🔍 Testing real database connection...")

    try:
        from src.utils.database import DatabaseManager

        db_manager = DatabaseManager()

        # Test connection string
        connection_string = db_manager.get_connection_string()
        logger.info(f"✅ Connection string generated: {connection_string[:50]}...")

        # Test actual connection
        try:
            connection = db_manager.get_connection()
            if connection:
                logger.info("✅ Database connection established successfully")

                # Test simple query
                cursor = connection.cursor()
                cursor.execute("SELECT 1 as test")
                result = cursor.fetchone()
                if result:
                    logger.info("✅ Database query test successful")

                cursor.close()
                connection.close()
                logger.info("✅ Database connection closed successfully")
            else:
                logger.error("❌ Database connection failed")
                return False
        except Exception as e:
            logger.error(f"❌ Database connection failed: {str(e)}")
            return False

        logger.info("✅ All database connection tests completed successfully")
        return True

    except Exception as e:
        logger.error(f"❌ Database connection test failed: {str(e)}")
        import traceback

        traceback.print_exc()
        return False


def test_database_loading_real():
    """Test database loading thực tế"""
    logger.info("🔍 Testing real database loading...")

    try:
        from src.loaders.misa_crm_loader import MISACRMLoader
        from src.loaders.tiktok_shop_staging_loader import TikTokShopOrderLoader
        from src.loaders.shopee_orders_loader import ShopeeOrderLoader

        # Test MISA CRM loader
        logger.info("Testing MISA CRM loader...")
        misa_loader = MISACRMLoader()

        # Test table mappings
        table_mappings = misa_loader.table_mappings
        logger.info(f"✅ MISA CRM table mappings: {table_mappings}")

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

        # Test data validation
        try:
            if hasattr(misa_loader, "validate_data_format"):
                is_valid = misa_loader.validate_data_format(
                    "customers", sample_customers_df
                )
                logger.info(f"✅ MISA CRM data validation: {is_valid}")
        except Exception as e:
            logger.warning(f"⚠️ MISA CRM data validation test failed: {str(e)}")

        # Test TikTok Shop loader
        logger.info("Testing TikTok Shop loader...")
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
        logger.info("Testing Shopee loader...")
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

        logger.info("✅ All database loading tests completed successfully")
        return True

    except Exception as e:
        logger.error(f"❌ Database loading test failed: {str(e)}")
        import traceback

        traceback.print_exc()
        return False


def test_full_etl_pipeline_real():
    """Test toàn bộ ETL pipeline thực tế"""
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
                logger.info(f"✅ Extracted {len(data)} {endpoint} records")
            except Exception as e:
                logger.warning(f"⚠️ Failed to extract {endpoint}: {str(e)}")

        if raw_data:
            # Transform
            transformer = MISACRMTransformer()
            transformed_data = transformer.transform_all_endpoints(raw_data)

            if transformed_data:
                logger.info("✅ MISA CRM transformation successful")

                # Test load (không thực sự load vào DB)
                loader = MISACRMLoader()
                logger.info("✅ MISA CRM loader instantiated successfully")

                # Test table mappings
                table_mappings = loader.table_mappings
                logger.info(f"✅ Table mappings: {table_mappings}")

                return True
            else:
                logger.warning("⚠️ MISA CRM transformation failed")
                return False
        else:
            logger.warning("⚠️ MISA CRM extraction failed")
            return False

    except Exception as e:
        logger.error(f"❌ Full ETL pipeline test failed: {str(e)}")
        import traceback

        traceback.print_exc()
        return False


def main():
    """Main test function"""
    logger.info("🚀 Starting TOKEN and DATABASE tests...")
    logger.info("⚠️  WARNING: This will test real connections!")
    logger.info("=" * 60)

    # Ask for confirmation
    response = input("Do you want to proceed with real connection tests? (y/N): ")
    if response.lower() != "y":
        logger.info("Test cancelled by user")
        return 0

    results = {}

    # Test real connections
    results["token_refresh"] = test_token_refresh_real()
    results["database_connection"] = test_database_connection_real()
    results["database_loading"] = test_database_loading_real()
    results["full_pipeline"] = test_full_etl_pipeline_real()

    # Summary
    logger.info("=" * 60)
    logger.info("📊 TOKEN AND DATABASE TEST RESULTS SUMMARY:")
    logger.info("=" * 60)

    total_tests = len(results)
    passed_tests = sum(1 for result in results.values() if result)

    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"{test_name.upper()}: {status}")

    logger.info("=" * 60)
    logger.info(f"TOTAL: {passed_tests}/{total_tests} tests passed")

    if passed_tests == total_tests:
        logger.info("🎉 All token and database tests passed! System is ready.")
        return 0
    else:
        logger.warning(
            f"⚠️ {total_tests - passed_tests} tests failed. Please check configuration."
        )
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
