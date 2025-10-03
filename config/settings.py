"""
Cấu hình settings cho Facolos Enterprise ETL System
Quản lý tất cả các settings từ environment variables cho multiple data sources
"""

import os
from typing import Optional, Dict, Any, List
import logging

logger = logging.getLogger(__name__)


class Settings:
    """Quản lý settings tập trung cho Facolos Enterprise ETL"""

    def __init__(self):
        # ========================
        # ENTERPRISE SETTINGS
        # ========================
        self.company_name: str = os.getenv("COMPANY_NAME", "Facolos")
        self.environment: str = os.getenv(
            "ENVIRONMENT", "development"
        )  # development, staging, production

        # ========================
        # TIKTOK SHOP API CREDENTIALS
        # ========================
        self.tiktok_shop_app_key: str = os.getenv("TIKTOK_APP_KEY", "6h2cosrovhjab")
        self.tiktok_shop_app_secret: str = os.getenv("TIKTOK_APP_SECRET", "")
        self.tiktok_shop_access_token: str = os.getenv("TIKTOK_ACCESS_TOKEN", "")
        self.tiktok_shop_refresh_token: str = os.getenv("TIKTOK_REFRESH_TOKEN", "")
        self.tiktok_shop_cipher: str = os.getenv("TIKTOK_SHOP_CIPHER", "")

        # ========================
        # MISA CRM CREDENTIALS
        # ========================
        self.misa_crm_client_id: Optional[str] = os.getenv("MISA_CRM_CLIENT_ID")
        self.misa_crm_client_secret: Optional[str] = os.getenv("MISA_CRM_CLIENT_SECRET")
        self.misa_crm_access_token: Optional[str] = os.getenv("MISA_CRM_ACCESS_TOKEN")
        self.misa_crm_base_url: Optional[str] = os.getenv("MISA_CRM_BASE_URL")

        # ========================
        # SHOPEE PLATFORM CREDENTIALS
        # ========================
        self.shopee_partner_id: str = os.getenv("SHOPEE_PARTNER_ID", "")
        self.shopee_partner_key: str = os.getenv("SHOPEE_PARTNER_KEY", "")
        self.shopee_shop_id: str = os.getenv("SHOPEE_SHOP_ID", "")
        self.shopee_redirect_uri: str = os.getenv("SHOPEE_REDIRECT_URI", "")
        self.shopee_access_token: str = os.getenv("SHOPEE_ACCESS_TOKEN", "")
        self.shopee_refresh_token: str = os.getenv("SHOPEE_REFRESH_TOKEN", "")

        # ========================
        # DATABASE SETTINGS
        # ========================
        self.sql_server_host: str = os.getenv(
            "SQL_SERVER_HOST", "sqlserver"
        )  # Use the service name from docker-compose for container-to-container communication
        self.sql_server_port: int = int(os.getenv("SQL_SERVER_PORT", "1433"))
        self.sql_server_database: str = os.getenv(
            "SQL_SERVER_DATABASE", "Facolos_Staging"
        )
        self.sql_server_username: str = os.getenv("SQL_SERVER_USERNAME", "sa")
        self.sql_server_password: str = os.getenv(
            "SQL_SERVER_PASSWORD", "FacolosDB2024!"
        )
        self.staging_schema: str = os.getenv("STAGING_SCHEMA", "staging")

        # ========================
        # ENTERPRISE SCHEMA MAPPINGS
        # ========================
        self.schema_mappings: Dict[str, str] = {
            # Current active platforms
            "tiktok_shop": "staging",
            "misa_crm": "staging",
            # Future platforms (Stage 2)
            "shopee": "staging",
            # Control schema
            "etl_control": "etl_control",
        }

        # ========================
        # CURRENT ACTIVE: TIKTOK SHOP SETTINGS
        # ========================
        self.tiktok_shop_schema: str = self.schema_mappings["tiktok_shop"]
        self.tiktok_shop_orders_table: str = "orders"
        self.staging_table_orders: str = (
            "tiktok_shop_order_detail"  # Fixed: Use correct table name
        )
        self.tiktok_shop_products_table: str = "products"  # Future
        self.tiktok_shop_customers_table: str = "customers"  # Future

        # ========================
        # MISA CRM SETTINGS
        # ========================
        self.misa_crm_schema: str = self.schema_mappings["misa_crm"]
        self.misa_crm_page_size: int = 100
        self.misa_crm_max_page_size: int = (
            5000000  # Max records per API call for full load
        )

        self.misa_crm_token_refresh_buffer: int = int(
            os.getenv("MISA_CRM_TOKEN_REFRESH_BUFFER", "300")
        )  # 5 minutes
        self.misa_crm_etl_batch_size: int = int(
            os.getenv("MISA_CRM_ETL_BATCH_SIZE", "1000")
        )
        self.misa_crm_incremental_lookback_hours: int = int(
            os.getenv("MISA_CRM_INCREMENTAL_LOOKBACK_HOURS", "24")
        )

        # ========================
        # SHOPEE SETTINGS
        # ========================
        self.shopee_schema: str = self.schema_mappings["shopee"]
        self.shopee_page_size: int = 100
        self.shopee_max_orders_per_batch: int = 50  # API limit for get_order_detail
        self.shopee_token_refresh_buffer: int = int(
            os.getenv("SHOPEE_TOKEN_REFRESH_BUFFER", "300")
        )  # 5 minutes
        self.shopee_etl_batch_size: int = int(
            os.getenv("SHOPEE_ETL_BATCH_SIZE", "1000")
        )
        self.shopee_incremental_lookback_minutes: int = int(
            os.getenv("SHOPEE_INCREMENTAL_LOOKBACK_MINUTES", "15")
        )
        # ========================
        # API SETTINGS
        # ========================
        self.tiktok_shop_api_base_url: str = os.getenv(
            "TIKTOK_SHOP_API_BASE_URL", "https://open-api.tiktokglobalshop.com"
        )
        self.api_timeout: int = int(os.getenv("API_TIMEOUT", "30"))
        self.api_retry_attempts: int = int(os.getenv("API_RETRY_ATTEMPTS", "3"))
        self.api_retry_delay: int = int(os.getenv("API_RETRY_DELAY", "5"))

        # ========================
        # ETL SETTINGS - STANDARDIZED INCREMENTAL WINDOWS
        # ========================
        self.etl_batch_size: int = int(os.getenv("ETL_BATCH_SIZE", "1000"))
        self.etl_page_size: int = int(os.getenv("ETL_PAGE_SIZE", "50"))
        self.etl_max_days_back: int = int(os.getenv("ETL_MAX_DAYS_BACK", "30"))
        self.etl_default_frequency_hours: float = float(
            os.getenv("ETL_DEFAULT_FREQUENCY_HOURS", "6")
        )

        # Standardized incremental lookback windows (đồng nhất 15 phút)
        self.etl_incremental_lookback_minutes: int = int(
            os.getenv("ETL_INCREMENTAL_LOOKBACK_MINUTES", "15")
        )

        # ========================
        # TEST ETl SETTINGS
        # ========================
        self.test_etl_days_back: int = int(
            os.getenv("TEST_ETL_DAYS_BACK", "2")
        )  # Test với 2 ngày gần nhất
        self.test_etl_max_orders: int = int(
            os.getenv("TEST_ETL_MAX_ORDERS", "50")
        )  # Test tối đa 50 orders
        self.test_etl_max_records_per_endpoint: int = int(
            os.getenv("TEST_ETL_MAX_RECORDS_PER_ENDPOINT", "5")
        )  # Test 5 records per endpoint

        # ========================
        # LOGGING SETTINGS
        # ========================
        self.log_level: str = os.getenv("LOG_LEVEL", "INFO")
        self.log_format: str = os.getenv(
            "LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        self.log_file_path: str = os.getenv(
            "LOG_FILE_PATH", "logs/facolos_enterprise_etl.log"
        )

    @property
    def sql_server_connection_string(self) -> str:
        """Tạo SQL Server connection string"""
        return (
            f"mssql+pyodbc://{self.sql_server_username}:{self.sql_server_password}@"
            f"{self.sql_server_host}:{self.sql_server_port}/{self.sql_server_database}"
            f"?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
        )

    @property
    def pyodbc_connection_string(self) -> str:
        """Tạo SQL Server connection string cho pyodbc trực tiếp"""
        driver = "ODBC Driver 17 for SQL Server"
        return (
            f"DRIVER={{{driver}}};"
            f"SERVER={self.sql_server_host},{self.sql_server_port};"
            f"DATABASE={self.sql_server_database};"
            f"UID={self.sql_server_username};"
            f"PWD={self.sql_server_password};"
            f"TrustServerCertificate=yes;"
        )

    @property
    def tiktok_shop_orders_full_table_name(self) -> str:
        """Tên đầy đủ của bảng TikTok Shop orders"""
        return f"{self.tiktok_shop_schema}.{self.tiktok_shop_orders_table}"

    def get_table_full_name(self, data_source: str, table_name: str) -> str:
        """
        Lấy tên đầy đủ của bảng theo data source

        Args:
            data_source: tên data source (vd: 'tiktok_shop', 'shopee')
            table_name: tên bảng (vd: 'orders', 'products')

        Returns:
            Tên đầy đủ schema.table
        """
        schema = self.schema_mappings.get(data_source)
        if not schema:
            raise ValueError(
                f"Không tìm thấy schema mapping cho data source: {data_source}"
            )

        return f"{schema}.{table_name}"

    def get_misa_crm_table_full_name(self, table_name: str) -> str:
        """
        Lấy tên đầy đủ của bảng MISA CRM

        Args:
            table_name: tên bảng (vd: 'customers')

        Returns:
            Tên đầy đủ schema.table cho MISA CRM
        """
        return self.get_table_full_name("misa_crm", table_name)

    @property
    def tiktok_shop_api_headers(self) -> Dict[str, str]:
        """Headers mặc định cho TikTok Shop API requests"""
        return {
            "Content-Type": "application/json",
            "User-Agent": f"{self.company_name}ETL/1.0",
        }

    def get_data_source_credentials(self, data_source: str) -> Dict[str, str]:
        """
        Lấy credentials theo data source

        Args:
            data_source: tên data source

        Returns:
            Dictionary chứa credentials
        """
        credentials_mapping = {
            "tiktok_shop": {
                "app_key": self.tiktok_shop_app_key,
                "app_secret": self.tiktok_shop_app_secret,
                "access_token": self.tiktok_shop_access_token,
                "refresh_token": self.tiktok_shop_refresh_token,
                "shop_cipher": self.tiktok_shop_cipher,
            },
            "misa_crm": self.get_misa_crm_credentials(),
            "shopee": self.get_shopee_credentials(),
        }

        return credentials_mapping.get(data_source, {})

    def get_misa_crm_credentials(self) -> Dict[str, Optional[str]]:
        """
        Lấy credentials cho MISA CRM một cách an toàn

        Returns:
            Dictionary chứa credentials của MISA CRM
        """
        return {
            "client_id": self.misa_crm_client_id,
            "client_secret": self.misa_crm_client_secret,
            "access_token": self.misa_crm_access_token,
            "base_url": self.misa_crm_base_url,
        }

    def get_shopee_credentials(self) -> Dict[str, str]:
        """
        Lấy credentials cho Shopee Platform một cách an toàn

        Returns:
            Dictionary chứa credentials của Shopee
        """
        return {
            "partner_id": self.shopee_partner_id,
            "partner_key": self.shopee_partner_key,
            "shop_id": self.shopee_shop_id,
            "redirect_uri": self.shopee_redirect_uri,
            "access_token": self.shopee_access_token,
            "refresh_token": self.shopee_refresh_token,
        }

    def validate_required_settings(self) -> bool:
        """
        Xác thực các settings bắt buộc

        Returns:
            True nếu tất cả settings required đều có
        """
        required_settings = {
            "sql_server_password": "SQL Server Password",
            "company_name": "Company Name",
        }

        missing_settings = []

        for setting_key, setting_name in required_settings.items():
            if not getattr(self, setting_key):
                missing_settings.append(setting_name)

        if missing_settings:
            missing_list = ", ".join(missing_settings)
            raise ValueError(f"Thiếu các settings bắt buộc: {missing_list}")

        logger.info("Tất cả settings required đã được cấu hình")
        return True

    def validate_data_source_credentials(self, data_source: str) -> bool:
        """
        Xác thực credentials của một data source cụ thể

        Args:
            data_source: tên data source cần validate

        Returns:
            True nếu credentials hợp lệ
        """
        if data_source == "tiktok_shop":
            required_fields = ["app_secret", "access_token"]
            credentials = self.get_data_source_credentials(data_source)

            missing_fields = [
                field for field in required_fields if not credentials.get(field)
            ]

            if missing_fields:
                missing_list = ", ".join(missing_fields)
                raise ValueError(f"TikTok Shop thiếu credentials: {missing_list}")

            logger.info("TikTok Shop credentials hợp lệ")
            return True

        elif data_source == "misa_crm":
            required_fields = ["client_id", "client_secret", "access_token"]
            credentials = self.get_misa_crm_credentials()

            missing_fields = [
                field for field in required_fields if not credentials.get(field)
            ]

            if missing_fields:
                missing_list = ", ".join(missing_fields)
                raise ValueError(f"MISA CRM thiếu credentials: {missing_list}")

            logger.info("MISA CRM credentials hợp lệ")
            return True

        elif data_source == "shopee":
            required_fields = ["partner_id", "partner_key", "shop_id", "access_token"]
            credentials = self.get_shopee_credentials()

            missing_fields = [
                field for field in required_fields if not credentials.get(field)
            ]

            if missing_fields:
                missing_list = ", ".join(missing_fields)
                raise ValueError(f"Shopee thiếu credentials: {missing_list}")

            logger.info("Shopee credentials hợp lệ")
            return True

        # Add validation for other data sources in the future
        logger.info(f"Validation cho {data_source} chưa được implement")
        return True

    def get_active_data_sources(self) -> List[str]:
        """
        Lấy danh sách data sources đang active (có credentials)

        Returns:
            List các data source names
        """
        active_sources = []

        # Check TikTok Shop
        if self.tiktok_shop_app_secret and self.tiktok_shop_access_token:
            active_sources.append("tiktok_shop")

        # Check MISA CRM
        if (
            self.misa_crm_client_id
            and self.misa_crm_client_secret
            and self.misa_crm_access_token
        ):
            active_sources.append("misa_crm")

        # Check Shopee
        if (
            self.shopee_partner_id
            and self.shopee_partner_key
            and self.shopee_shop_id
            and self.shopee_access_token
        ):
            active_sources.append("shopee")

        return active_sources

    def get_env_summary(self) -> Dict[str, Any]:
        """
        Lấy tóm tắt environment settings (không bao gồm sensitive data)

        Returns:
            Dictionary chứa thông tin settings summary
        """
        return {
            "company_name": self.company_name,
            "environment": self.environment,
            "sql_server_host": self.sql_server_host,
            "sql_server_database": self.sql_server_database,
            "active_data_sources": self.get_active_data_sources(),
            "available_schemas": list(self.schema_mappings.keys()),
            "etl_batch_size": self.etl_batch_size,
            "api_timeout": self.api_timeout,
            "log_level": self.log_level,
            "has_db_password": bool(self.sql_server_password),
            "tiktok_shop_configured": bool(
                self.tiktok_shop_app_secret and self.tiktok_shop_access_token
            ),
            "misa_crm_configured": bool(
                self.misa_crm_client_id
                and self.misa_crm_client_secret
                and self.misa_crm_access_token
            ),
            "shopee_configured": bool(
                self.shopee_partner_id
                and self.shopee_partner_key
                and self.shopee_shop_id
                and self.shopee_access_token
            ),
        }


# Global settings instance
settings = Settings()

# Validate settings khi import
try:
    if settings.sql_server_password:
        active_sources = settings.get_active_data_sources()
        if active_sources:
            logger.info(
                f"Facolos Enterprise ETL Settings initialized. Active sources: {', '.join(active_sources)}"
            )
        else:
            logger.warning(
                "Không có data source nào được cấu hình. Hãy kiểm tra file .env"
            )
    else:
        logger.warning("Database settings chưa được cấu hình. Hãy kiểm tra file .env")
except Exception as e:
    logger.error(f"Lỗi khởi tạo settings: {str(e)}")
