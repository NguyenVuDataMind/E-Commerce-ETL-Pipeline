"""
Database loader cho dữ liệu đơn hàng TikTok Shop
Tải dữ liệu đã chuyển đổi vào bảng staging SQL Server
"""

import pandas as pd
import logging
import sys
import os
from typing import Optional, Dict, Any
from sqlalchemy import text

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
            if load_mode == "truncate_insert" or load_mode == "replace":
                # Truncate table first, then insert (tránh pandas.to_sql if_exists="replace")
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

                    # Set subsequent batches to append mode (đã xử lý ở trên)

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
            # Khởi tạo database connection nếu chưa có
            if not self.db.engine:
                if not self.db.initialize():
                    logger.error("Failed to initialize database connection")
                    return False

            if df.empty:
                logger.warning("DataFrame is empty, nothing to load")
                return True

            logger.info(f"Loading {len(df)} rows incrementally with UPSERT logic...")

            # Prepare the data (chuẩn hóa timestamp về datetime để khớp schema SQL Server)
            df_prepared = self._prepare_dataframe_for_upsert(df)
            if df_prepared is None:
                return False

            # Use MERGE statement for proper UPSERT
            return self._upsert_orders(df_prepared)

        except Exception as e:
            logger.error(f"Error in incremental load: {str(e)}")
            return False

    def _prepare_dataframe_for_upsert(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Chuẩn hóa DataFrame trước khi UPSERT:
          - Thời gian: epoch (s/ms) -> DATETIME2-naive theo múi giờ Việt Nam (+07).
          - Tiền/số: ép numeric, thay NaN bằng None để SQL nhận NULL.
          - Số đếm: INT nullable (Int64) rồi cast object để giữ None khi bind.
          - Boolean: map True/False -> 1/0, null -> None (BIT).
          - Chuỗi/JSON: giữ nguyên chuỗi, thay 'nan'/NaN -> None.
          - etl_*: DATETIME2-naive +07 để đồng bộ với MISA.
        """
        try:
            prepared = df.copy()

            # 1) Chuẩn hóa các cột thời gian (epoch)
            epoch_cols = [
                # order-level
                "create_time",
                "update_time",
                "paid_time",
                "rts_time",
                "shipping_due_time",
                "collection_due_time",
                "cancel_order_sla_time",
                # recommended_shipping_time đã được transformer xử lý thành UTC datetime rồi
                "rts_sla_time",
                "tts_sla_time",
                # item-level
                "item_rts_time",
            ]

            for col in epoch_cols:
                if col in prepared.columns:
                    # FIX: Kiểm tra xem cột đã là datetime chưa
                    if pd.api.types.is_datetime64_any_dtype(prepared[col]):
                        # Cột đã là datetime (từ transformer) → chỉ cần convert timezone
                        dt = prepared[col]
                        # Convert UTC → +07-naive (DATETIME2)
                        dt = dt.dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
                        prepared[col] = dt
                    else:
                        # Nếu là chuỗi ISO/strftime → parse về UTC rồi convert +07
                        if pd.api.types.is_string_dtype(prepared[col]):
                            dt = pd.to_datetime(
                                prepared[col], utc=True, errors="coerce"
                            )
                            dt = dt.dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(
                                None
                            )
                            prepared[col] = dt.where(pd.notna(dt), None)
                        else:
                            # Cột là epoch integer → convert như cũ
                            s = pd.to_numeric(prepared[col], errors="coerce")
                            # Nếu giá trị ms (>=1e12) -> quy về giây
                            s = s.where(s.isna() | (s < 10**12), (s // 1000))
                            # Convert về datetime +07-naive (DATETIME2)
                            dt = pd.to_datetime(s, unit="s", utc=True, errors="coerce")
                            dt = dt.dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(
                                None
                            )
                            prepared[col] = dt.where(pd.notna(dt), None)

            # 2) Chuẩn hóa nhóm tiền/tổng (DECIMAL(18,2) phía DB)
            money_cols = [
                "payment_original_shipping_fee",
                "payment_original_total_product_price",
                "payment_platform_discount",
                "payment_seller_discount",
                "payment_shipping_fee",
                "payment_shipping_fee_cofunded_discount",
                "payment_shipping_fee_platform_discount",
                "payment_shipping_fee_seller_discount",
                "payment_sub_total",
                "payment_tax",
                "payment_total_amount",
                "item_original_price",
                "item_sale_price",
                "item_platform_discount",
                "item_seller_discount",
                "display_price",
                "total_product_price",
            ]
            for col in money_cols:
                if col in prepared.columns:
                    s = pd.to_numeric(prepared[col], errors="coerce")
                    prepared[col] = s.astype("object").where(pd.notnull(s), None)

            # 3) Số đếm
            for col in ["item_quantity", "quantity", "fulfillment_priority_level"]:
                if col in prepared.columns:
                    s = pd.to_numeric(prepared[col], errors="coerce")
                    prepared[col] = (
                        s.astype("Int64").astype("object").where(pd.notnull(s), None)
                    )

            # 4) Boolean -> BIT
            bool_cols = [
                "has_updated_recipient_address",
                "is_cod",
                "is_on_hold_order",
                "is_replacement_order",
                "is_sample_order",
                "is_buyer_request_cancel",
                "item_is_buyer_request_cancel",
                "item_is_gift",
                "is_gift",
            ]
            for col in bool_cols:
                if col in prepared.columns:
                    prepared[col] = (
                        prepared[col]
                        .map({True: 1, False: 0})
                        .where(pd.notnull(prepared[col]), None)
                    )

            # 5) Chuỗi/JSON: thay 'nan' -> None; giữ nguyên nội dung
            prepared = prepared.replace("nan", None)
            prepared = prepared.where(pd.notnull(prepared), None)

            # 6) etl timestamps -> DATETIME2-naive +07
            for etl_col in ["etl_created_at", "etl_updated_at"]:
                if (
                    etl_col in prepared.columns
                    and not pd.api.types.is_datetime64_any_dtype(prepared[etl_col])
                ):
                    prepared[etl_col] = pd.to_datetime(
                        prepared[etl_col], utc=True, errors="coerce"
                    )
                if (
                    etl_col in prepared.columns
                    and pd.api.types.is_datetime64_any_dtype(prepared[etl_col])
                ):
                    try:
                        prepared[etl_col] = (
                            prepared[etl_col]
                            .dt.tz_convert("Asia/Ho_Chi_Minh")
                            .dt.tz_localize(None)
                        )
                    except Exception:
                        pass

            return prepared
        except Exception as e:
            logger.error(f"Error in _prepare_dataframe_for_upsert: {e}")
            return None

    def _upsert_orders(self, df: pd.DataFrame) -> bool:
        """
        Thực hiện UPSERT bằng MERGE + VALUES (không tạo bảng tạm)

        Args:
            df: Prepared DataFrame

        Returns:
            True if successful, False otherwise
        """
        try:
            # Danh sách cột và PK
            columns = df.columns.tolist()
            if "order_id" not in columns:
                logger.error("Thiếu cột khóa 'order_id' trong DataFrame TikTok")
                return False
            if "item_id" not in columns:
                logger.error(
                    "Thiếu cột khóa 'item_id' trong DataFrame TikTok (yêu cầu khóa tổng hợp order_id + item_id)"
                )
                return False

            # Khử trùng lặp theo khóa (order_id, item_id), ưu tiên bản có update_time mới nhất nếu có
            try:
                if "update_time" in df.columns:
                    df = df.sort_values("update_time").drop_duplicates(
                        ["order_id", "item_id"], keep="last"
                    )
                else:
                    df = df.drop_duplicates(["order_id", "item_id"], keep="last")
            except Exception:
                # Nếu có lỗi trong bước khử trùng vẫn tiếp tục với df hiện tại
                pass

            # Cập nhật lại danh sách cột sau khi khử trùng (phòng trường hợp reindex)
            columns = df.columns.tolist()

            col_list_sql = ", ".join([f"[{c}]" for c in columns])
            on_clause = (
                "target.order_id = source.order_id AND target.item_id = source.item_id"
            )
            update_set_cols = [c for c in columns if c not in ("order_id", "item_id")]

            # Guard cập nhật: ưu tiên update_time; thêm trạng thái/ vận chuyển nếu có
            guards = []
            if "update_time" in columns:
                # So sánh DATETIME2 an toàn, tránh dùng 0 (int) cho NULL
                guards.append(
                    "COALESCE(target.update_time, '1900-01-01') < COALESCE(source.update_time, '1900-01-01')"
                )
            if "order_status" in columns:
                guards.append(
                    "ISNULL(target.order_status,'') <> ISNULL(source.order_status,'')"
                )
            if "tracking_number" in columns:
                guards.append(
                    "ISNULL(target.tracking_number,'') <> ISNULL(source.tracking_number,'')"
                )
            if "shipping_provider" in columns:
                guards.append(
                    "ISNULL(target.shipping_provider,'') <> ISNULL(source.shipping_provider,'')"
                )

            matched_guard = "WHEN MATCHED THEN"
            if guards:
                matched_guard = f"WHEN MATCHED AND (" + " OR ".join(guards) + ") THEN"

            # Loại trừ các cột ETL metadata khỏi auto-update để tránh gán trùng
            etl_meta = {
                "etl_created_at",
                "etl_updated_at",
                "etl_batch_id",
                "etl_source",
            }
            update_cols_effective = [c for c in update_set_cols if c not in etl_meta]
            set_clauses = [f"target.{c} = source.{c}" for c in update_cols_effective]
            # Cho phép cập nhật batch/source nếu có mặt
            if "etl_batch_id" in columns:
                set_clauses.append("target.etl_batch_id = source.etl_batch_id")
            if "etl_source" in columns:
                set_clauses.append("target.etl_source = source.etl_source")
            # etl_updated_at theo giờ Việt Nam (+07)
            set_clauses.append("target.etl_updated_at = DATEADD(HOUR, 7, GETUTCDATE())")
            update_set_sql = ",\n                        ".join(set_clauses)
            insert_values_sql = ", ".join([f"source.{c}" for c in columns])

            # Chia lô để tránh câu lệnh quá dài
            batch_size = min(200, len(df)) if len(df) > 0 else 0

            with self.db.engine.begin() as conn:
                total_rows = 0
                records = df.to_dict(orient="records")
                for i in range(0, len(records), batch_size):
                    batch = records[i : i + batch_size]

                    values_rows = []
                    params: Dict[str, Any] = {}
                    for r_idx, row in enumerate(batch):
                        placeholders = []
                        for c in columns:
                            pname = f"p_{r_idx}_{c}"
                            placeholders.append(f":{pname}")
                            val = row.get(c, None)
                            # Chốt: mọi NaN/NaT -> None để tránh lỗi 8023 khi bind vào BIGINT/DECIMAL/BIT
                            try:
                                if pd.isna(val):
                                    val = None
                            except Exception:
                                pass
                            params[pname] = val
                        values_rows.append(f"({', '.join(placeholders)})")

                    values_sql = ",\n                            ".join(values_rows)

                    merge_sql = f"""
                    MERGE [{self.staging_schema}].[{self.staging_table}] AS target
                    USING (
                        VALUES
                            {values_sql}
                    ) AS source ({col_list_sql})
                    ON {on_clause}

                    {matched_guard}
                        UPDATE SET
                            {update_set_sql}

                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT ({col_list_sql})
                        VALUES ({insert_values_sql});
                    """

                    conn.execute(text(merge_sql), params)
                    total_rows += len(batch)

            logger.info(
                f"UPSERT (no-temp) completed for TikTok: {len(df)} rows processed"
            )
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
        Prepare DataFrame for database loading (Full Load)
        Convert UTC datetime to +07-naive datetime (thống nhất với incremental load)

        Args:
            df: Original DataFrame with UTC datetime columns

        Returns:
            Prepared DataFrame with +07-naive datetime columns
        """
        try:
            df_prepared = df.copy()

            # Convert UTC datetime columns to +07-naive (thống nhất với incremental load)
            datetime_columns = [
                "create_time",
                "update_time",
                "paid_time",
                "rts_time",
                "shipping_due_time",
                "collection_due_time",
                "cancel_order_sla_time",
                "recommended_shipping_time",
                "rts_sla_time",
                "tts_sla_time",
                "item_rts_time",
            ]

            for col in datetime_columns:
                if col in df_prepared.columns and pd.api.types.is_datetime64_any_dtype(
                    df_prepared[col]
                ):
                    # Convert UTC datetime to +07-naive (như incremental load)
                    df_prepared[col] = (
                        df_prepared[col]
                        .dt.tz_convert("Asia/Ho_Chi_Minh")
                        .dt.tz_localize(None)
                    )

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
                "delivery_option_id": 50,
                "delivery_option_name": 100,
                "delivery_type": 50,
                "order_type": 50,
                "shipping_provider": 100,
                "shipping_provider_id": 50,
                "shipping_type": 50,
                "tracking_number": 100,
                "split_or_combine_tag": 50,
                "recipient_first_name": 100,
                "recipient_first_name_local_script": 100,
                "recipient_last_name": 100,
                "recipient_last_name_local_script": 100,
                "recipient_name": 200,
                "recipient_phone_number": 50,
                "recipient_postal_code": 20,
                "recipient_region_code": 20,
                "package_id": 50,
                "item_package_id": 50,
                "item_package_status": 50,
                "item_shipping_provider_id": 50,
                "item_shipping_provider_name": 100,
                "item_tracking_number": 100,
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
