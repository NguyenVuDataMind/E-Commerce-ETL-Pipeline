"""
Database loader cho dữ liệu đơn hàng TikTok Shop
Tải dữ liệu đã chuyển đổi vào bảng staging SQL Server
"""

import pandas as pd
import logging
import sys
import os
from typing import Optional, Dict, Any

# Add project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from src.utils.database import db_manager
from src.utils.logging import setup_logging
from config.settings import settings

logger = setup_logging("tiktok_shop_loader")


class TikTokShopOrderLoader:
    """Tải dữ liệu đơn hàng TikTok Shop vào database staging"""

    def __init__(self):
        self.db = db_manager
        self.staging_schema = settings.staging_schema
        self.staging_table = settings.staging_table_orders

    def initialize_staging(self) -> bool:
        """
        Khởi tạo môi trường staging (schema và tables)

        Returns:
            True nếu thành công, False nếu ngược lại
        """
        try:
            logger.info("Đang khởi tạo môi trường staging...")

            # Khởi tạo kết nối database
            if not self.db.initialize():
                logger.error("Khởi tạo kết nối database thất bại")
                return False

            # Tạo staging schema
            if not self.db.create_staging_schema():
                logger.error("Tạo staging schema thất bại")
                return False

            # Thực thi script tạo bảng staging
            table_script_path = "sql/staging/create_tiktok_orders_table.sql"
            if not self.db.execute_sql_file(table_script_path):
                logger.error("Tạo bảng staging thất bại")
                return False

            logger.info("Khởi tạo môi trường staging thành công")
            return True

        except Exception as e:
            logger.error(f"Lỗi khởi tạo môi trường staging: {str(e)}")
            return False

    def load_orders(
        self,
        df: pd.DataFrame,
        load_mode: str = "append",
        validate_before_load: bool = True,
        batch_size: int = 15,
    ) -> bool:
        """
        Load order data into staging table với memory optimization

        Args:
            df: DataFrame with order data
            load_mode: Loading mode ('append', 'replace', 'truncate_insert')
            validate_before_load: Whether to validate data before loading
            batch_size: Số rows load mỗi batch để tránh OOM

        Returns:
            True if successful, False otherwise
        """
        try:
            if df.empty:
                logger.warning("DataFrame is empty, nothing to load")
                return True

            logger.info(
                f"Loading {len(df)} rows into staging table in batches of {batch_size}..."
            )

            # Validate data if requested
            if validate_before_load:
                if not self._validate_dataframe(df):
                    logger.error("DataFrame validation failed")
                    return False

            # Prepare DataFrame for loading
            df_prepared = self._prepare_dataframe_for_load(df)

            # Handle different load modes
            if load_mode == "truncate_insert":
                # Truncate table first, then insert
                if not self.db.truncate_table(self.staging_table, self.staging_schema):
                    logger.error("Failed to truncate staging table")
                    return False
                load_mode = "append"

            # Load data in batches để tránh memory issues
            total_batches = (len(df_prepared) + batch_size - 1) // batch_size
            logger.info(f"Processing {total_batches} batches for loading")

            for i in range(0, len(df_prepared), batch_size):
                batch_num = (i // batch_size) + 1
                batch_df = df_prepared.iloc[i : i + batch_size].copy()

                logger.info(
                    f"Loading batch {batch_num}/{total_batches}: {len(batch_df)} rows"
                )

                try:
                    # Load batch to database
                    success = self.db.insert_dataframe(
                        df=batch_df,
                        table_name=self.staging_table,
                        schema=self.staging_schema,  # Fixed: parameter name is 'schema' not 'schema_name'
                        if_exists=load_mode,
                    )

                    if not success:
                        logger.error(f"Failed to load batch {batch_num}")
                        return False

                    logger.info(f"Batch {batch_num} loaded successfully")

                    # Set subsequent batches to append mode
                    if load_mode == "replace":
                        load_mode = "append"

                    # Memory cleanup
                    del batch_df

                    if batch_num % 5 == 0:  # Cleanup every 5 batches
                        import gc

                        gc.collect()
                        logger.info(f"Memory cleanup after batch {batch_num}")

                except Exception as e:
                    logger.error(f"Error loading batch {batch_num}: {str(e)}")
                    return False

            logger.info(
                f"Successfully loaded all {len(df_prepared)} rows to staging table"
            )
            return True

        except Exception as e:
            logger.error(f"Error in load_orders: {str(e)}")
            return False

    def load_incremental_orders(self, df: pd.DataFrame) -> bool:
        """
        Load orders incrementally using UPSERT (INSERT/UPDATE) logic

        Args:
            df: DataFrame with order data

        Returns:
            True if successful, False otherwise
        """
        try:
            if df.empty:
                logger.warning("DataFrame is empty, nothing to load")
                return True

            logger.info(f"Loading {len(df)} rows incrementally with UPSERT logic...")

            # Prepare the data (incremental cần chuẩn hóa timestamp về datetime)
            df_prepared = self._prepare_dataframe_for_upsert(df)
            if df_prepared is None:
                return False

            # Use MERGE statement for proper UPSERT
            return self._upsert_orders(df_prepared)

        except Exception as e:
            logger.error(f"Error in incremental load: {str(e)}")
            return False

    def _upsert_orders(self, df: pd.DataFrame) -> bool:
        """
        Perform UPSERT operation using SQL MERGE statement

        Args:
            df: Prepared DataFrame

        Returns:
            True if successful, False otherwise
        """
        try:
            with self.db.get_connection() as conn:
                # Create temp table for batch processing
                temp_table = f"#{self.staging_table}_temp"

                # Create temp table with same structure
                create_temp_sql = f"""
                CREATE TABLE {temp_table} (
                    order_id NVARCHAR(50) NOT NULL,
                    shop_id NVARCHAR(50),
                    order_status INT,
                    create_time DATETIME2,
                    update_time DATETIME2,
                    buyer_email NVARCHAR(255),
                    shipping_provider NVARCHAR(100),
                    tracking_number NVARCHAR(100),
                    delivery_type INT,
                    delivery_option_id NVARCHAR(50),
                    is_cod_order BIT,
                    payment_method_name NVARCHAR(100),
                    payment_method_id INT,
                    shipping_fee_platform_discount DECIMAL(18,2),
                    platform_discount DECIMAL(18,2),
                    seller_discount DECIMAL(18,2),
                    tax_amount DECIMAL(18,2),
                    order_amount DECIMAL(18,2),
                    currency NVARCHAR(10),
                    recipient_address NVARCHAR(MAX),
                    etl_batch_id NVARCHAR(50),
                    etl_created_at DATETIME2,
                    etl_updated_at DATETIME2
                )
                """

                conn.execute(create_temp_sql)
                logger.info(f"Created temp table {temp_table}")

                # Insert data to temp table
                df.to_sql(
                    name=temp_table[1:],  # Remove # prefix for pandas
                    con=self.db.engine,
                    schema=self.staging_schema,
                    if_exists="append",
                    index=False,
                    method="multi",
                )

                # Perform MERGE operation
                merge_sql = f"""
                MERGE [{self.staging_schema}].[{self.staging_table}] AS target
                USING {temp_table} AS source
                ON target.order_id = source.order_id
                
                WHEN MATCHED AND (
                    target.update_time < source.update_time OR
                    target.order_status != source.order_status OR
                    target.tracking_number != source.tracking_number
                ) THEN
                    UPDATE SET
                        shop_id = source.shop_id,
                        order_status = source.order_status,
                        create_time = source.create_time,
                        update_time = source.update_time,
                        buyer_email = source.buyer_email,
                        shipping_provider = source.shipping_provider,
                        tracking_number = source.tracking_number,
                        delivery_type = source.delivery_type,
                        delivery_option_id = source.delivery_option_id,
                        is_cod_order = source.is_cod_order,
                        payment_method_name = source.payment_method_name,
                        payment_method_id = source.payment_method_id,
                        shipping_fee_platform_discount = source.shipping_fee_platform_discount,
                        platform_discount = source.platform_discount,
                        seller_discount = source.seller_discount,
                        tax_amount = source.tax_amount,
                        order_amount = source.order_amount,
                        currency = source.currency,
                        recipient_address = source.recipient_address,
                        etl_updated_at = GETDATE()
                        
                WHEN NOT MATCHED THEN
                    INSERT (
                        order_id, shop_id, order_status, create_time, update_time,
                        buyer_email, shipping_provider, tracking_number, delivery_type,
                        delivery_option_id, is_cod_order, payment_method_name, 
                        payment_method_id, shipping_fee_platform_discount, platform_discount,
                        seller_discount, tax_amount, order_amount, currency,
                        recipient_address, etl_batch_id, etl_created_at, etl_updated_at
                    )
                    VALUES (
                        source.order_id, source.shop_id, source.order_status, 
                        source.create_time, source.update_time, source.buyer_email,
                        source.shipping_provider, source.tracking_number, source.delivery_type,
                        source.delivery_option_id, source.is_cod_order, source.payment_method_name,
                        source.payment_method_id, source.shipping_fee_platform_discount,
                        source.platform_discount, source.seller_discount, source.tax_amount,
                        source.order_amount, source.currency, source.recipient_address,
                        source.etl_batch_id, source.etl_created_at, source.etl_updated_at
                    );
                """

                result = conn.execute(merge_sql)
                rows_affected = result.rowcount

                # Drop temp table
                conn.execute(f"DROP TABLE {temp_table}")

                logger.info(f"UPSERT completed: {rows_affected} rows affected")
                return True

        except Exception as e:
            logger.error(f"Error in _upsert_orders: {str(e)}")
            return False

    def get_load_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about loaded data

        Returns:
            Dictionary with load statistics
        """
        try:
            stats = {}

            # Get row count
            with self.db.get_connection() as conn:
                result = conn.execute(
                    f"SELECT COUNT(*) as row_count FROM [{self.staging_schema}].[{self.staging_table}]"
                ).fetchone()
                stats["total_rows"] = result.row_count

                # Get unique orders count
                result = conn.execute(
                    f"SELECT COUNT(DISTINCT order_id) as unique_orders FROM [{self.staging_schema}].[{self.staging_table}]"
                ).fetchone()
                stats["unique_orders"] = result.unique_orders

                # Get date range
                result = conn.execute(
                    f"""
                    SELECT 
                        MIN(create_time) as min_create_time,
                        MAX(create_time) as max_create_time,
                        MIN(etl_created_at) as min_etl_time,
                        MAX(etl_created_at) as max_etl_time
                    FROM [{self.staging_schema}].[{self.staging_table}]
                    """
                ).fetchone()

                if result:
                    stats.update(
                        {
                            "min_create_time": result.min_create_time,
                            "max_create_time": result.max_create_time,
                            "min_etl_time": result.min_etl_time,
                            "max_etl_time": result.max_etl_time,
                        }
                    )

            logger.info(f"Load statistics: {stats}")
            return stats

        except Exception as e:
            logger.error(f"Error getting load statistics: {str(e)}")
            return {}

    def _validate_dataframe(self, df: pd.DataFrame) -> bool:
        """
        Validate DataFrame before loading

        Args:
            df: DataFrame to validate

        Returns:
            True if valid, False otherwise
        """
        try:
            # Check required columns
            required_columns = [
                "order_id",
                "etl_batch_id",
                "etl_created_at",
                "etl_updated_at",
            ]

            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"Missing required columns: {missing_columns}")
                return False

            # Check for null order_ids
            null_order_ids = df["order_id"].isnull().sum()
            if null_order_ids > 0:
                logger.error(f"Found {null_order_ids} rows with null order_id")
                return False

            # Check data types and ranges
            numeric_columns = [
                "original_shipping_fee",
                "original_total_product_price",
                "seller_discount",
                "shipping_fee",
                "total_amount",
                "item_quantity",
                "item_unit_price",
            ]

            for col in numeric_columns:
                if col in df.columns:
                    # Check for negative values where they shouldn't be
                    if col in ["item_quantity"] and (df[col] < 0).any():
                        logger.warning(f"Found negative values in {col}")

            logger.info("DataFrame validation passed")
            return True

        except Exception as e:
            logger.error(f"Error validating DataFrame: {str(e)}")
            return False

    def _prepare_dataframe_for_load(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare DataFrame for database loading

        Args:
            df: Original DataFrame

        Returns:
            Prepared DataFrame
        """
        try:
            df_prepared = df.copy()

            # Ensure proper data types
            # Convert timestamp columns
            timestamp_columns = [
                "create_time",
                "update_time",
                "delivery_time",
                "collection_time",
                "delivery_due_time",
            ]

            for col in timestamp_columns:
                if col in df_prepared.columns:
                    # TikTok timestamps are in seconds, convert to proper datetime if needed
                    pass  # Keep as integer for now, can convert later if needed

            # Ensure string columns are not too long (tăng kích thước để phù hợp với database schema)
            string_columns = {
                "order_id": 50,
                "order_status": 50,
                "payment_currency": 10,
                "item_currency": 10,
                "region": 10,
                "item_id": 50,
                "item_sku_id": 50,
                "item_product_id": 50,
                "item_product_name": 500,
                "item_sku_name": 500,
                "item_sku_type": 50,
                "item_seller_sku": 100,
                "item_display_status": 50,
                "buyer_email": 255,
                "payment_method_name": 100,
                "warehouse_id": 50,
                "user_id": 50,
                "request_id": 100,
                "shop_id": 50,
                "shop_cipher": 100,
                "delivery_option_id": 50,
                "delivery_option_name": 100,
                "delivery_type": 50,
                "order_type": 50,
                "shipping_provider": 100,
                "shipping_provider_id": 50,
                "shipping_type": 50,
                "tracking_number": 100,
                "cancel_user": 50,
                "buyer_uid": 50,
                "split_or_combine_tag": 50,
                "delivery_option": 100,
                "order_line_id": 50,
                "cpf": 50,
                "recipient_first_name": 100,
                "recipient_first_name_local_script": 100,
                "recipient_last_name": 100,
                "recipient_last_name_local_script": 100,
                "recipient_name": 200,
                "recipient_phone_number": 50,
                "recipient_postal_code": 20,
                "recipient_region_code": 20,
                "package_id": 50,
                "package_status": 50,
                "package_shipping_provider_id": 50,
                "package_shipping_provider_name": 100,
                "package_tracking_number": 100,
                "item_package_id": 50,
                "item_package_status": 50,
                "item_shipping_provider_id": 50,
                "item_shipping_provider_name": 100,
                "item_tracking_number": 100,
                "item_cancel_user": 50,
                "etl_batch_id": 50,
                "etl_source": 50,
            }

            for col, max_length in string_columns.items():
                if col in df_prepared.columns:
                    # Convert to string and truncate if necessary
                    df_prepared[col] = df_prepared[col].astype(str).str[:max_length]
                    # Replace 'nan' strings with None
                    df_prepared[col] = df_prepared[col].replace("nan", None)

            # Handle None/NaN values appropriately
            df_prepared = df_prepared.where(pd.notnull(df_prepared), None)

            return df_prepared

        except Exception as e:
            logger.error(f"Error preparing DataFrame: {str(e)}")
            return df

    def test_connection(self) -> bool:
        """
        Kiểm tra kết nối database và truy cập bảng staging

        Returns:
            True nếu thành công, False nếu ngược lại
        """
        try:
            logger.info("Đang kiểm tra kết nối database...")

            # Kiểm tra kết nối cơ bản
            if not self.db.initialize():
                logger.error("✗ Kết nối database thất bại")
                return False

            logger.info("✓ Kết nối database thành công")

            # Kiểm tra sự tồn tại của bảng
            if not self.db.table_exists(self.staging_table, self.staging_schema):
                logger.error(
                    f"✗ Bảng staging {self.staging_schema}.{self.staging_table} không tồn tại"
                )
                return False

            logger.info(
                f"✓ Bảng staging {self.staging_schema}.{self.staging_table} tồn tại"
            )

            # Kiểm tra truy vấn thống kê
            stats = self.get_load_statistics()
            logger.info(f"✓ Thống kê bảng hiện tại: {stats}")

            return True

        except Exception as e:
            logger.error(f"✗ Kiểm tra kết nối database thất bại: {str(e)}")
            return False
