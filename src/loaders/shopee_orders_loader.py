#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shopee Orders Data Loader
Tích hợp với Facolos Enterprise ETL Infrastructure
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging
import sys
import os

# Import shared utilities
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
from config.settings import settings
from src.utils.logging import setup_logging

logger = setup_logging(__name__)


class ShopeeOrderLoader:
    """
    Shopee Orders Data Loader - Tương tự TikTok Shop và MISA CRM pattern
    """

    def __init__(self):
        """Khởi tạo Shopee Order Loader"""
        self.db_engine = create_engine(settings.sql_server_connection_string)

        # Table mappings - sử dụng staging schema, bảng Shopee có tiền tố shopee_
        self.table_mappings = {
            "orders": settings.get_table_full_name("shopee", "shopee_orders"),
            "recipient_address": settings.get_table_full_name(
                "shopee", "shopee_recipient_address"
            ),
            "order_items": settings.get_table_full_name("shopee", "shopee_order_items"),
            "order_item_locations": settings.get_table_full_name(
                "shopee", "shopee_order_item_locations"
            ),
            "packages": settings.get_table_full_name("shopee", "shopee_packages"),
            "package_items": settings.get_table_full_name(
                "shopee", "shopee_package_items"
            ),
            "invoice": settings.get_table_full_name("shopee", "shopee_invoice"),
            "payment_info": settings.get_table_full_name(
                "shopee", "shopee_payment_info"
            ),
            "order_pending_terms": settings.get_table_full_name(
                "shopee", "shopee_order_pending_terms"
            ),
            "order_warnings": settings.get_table_full_name(
                "shopee", "shopee_order_warnings"
            ),
            "prescription_images": settings.get_table_full_name(
                "shopee", "shopee_prescription_images"
            ),
            "buyer_proof_of_collection": settings.get_table_full_name(
                "shopee", "shopee_buyer_proof_of_collection"
            ),
        }

        logger.info(f"Khởi tạo Shopee Order Loader cho {settings.company_name}")
        logger.info(f"Database: {settings.sql_server_host}")
        logger.info(f"Schema: {settings.schema_mappings.get('shopee', 'staging')}")

    def _get_table_info(self, table_full_name: str) -> Dict[str, Any]:
        """
        Lấy thông tin về table (schema, table name)

        Args:
            table_full_name: Tên đầy đủ của table (schema.table)

        Returns:
            Dict chứa schema và table name
        """
        parts = table_full_name.split(".")
        if len(parts) == 2:
            return {"schema": parts[0], "table": parts[1]}
        else:
            return {"schema": "staging", "table": table_full_name}

    def _convert_datetime_to_naive(self, df: pd.DataFrame) -> pd.DataFrame:
        """Chuyển datetime tz-aware về tz-naive theo múi giờ Việt Nam (+07)."""
        df_copy = df.copy()
        for col in df_copy.columns:
            if df_copy[col].dtype == "datetime64[ns, UTC]":
                df_copy[col] = (
                    df_copy[col].dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
                )
            elif "datetime" in str(df_copy[col].dtype):
                df_copy[col] = (
                    pd.to_datetime(df_copy[col], utc=True)
                    .dt.tz_convert("Asia/Ho_Chi_Minh")
                    .dt.tz_localize(None)
                )
        return df_copy

    def _normalize_datetime_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Chuẩn hóa các cột thời gian từ UNIX epoch (s/ms) hoặc ISO string về datetime tz-naive.

        Chỉ áp dụng cho các cột thời gian của Shopee orders theo schema staging.shopee_orders.
        """
        if df.empty:
            return df

        df_norm = df.copy()

        datetime_cols = [
            "create_time",
            "update_time",
            "ship_by_date",
            "note_update_time",
            "pay_time",
            "pickup_done_time",
            "edt_from",
            "edt_to",
            "return_request_due_date",
            "prescription_approval_time",
            "prescription_rejection_time",
        ]

        for col in datetime_cols:
            if col not in df_norm.columns:
                continue

            s = df_norm[col]

            # Bỏ qua hoàn toàn nếu series đã là datetime64[ns]
            if str(s.dtype).startswith("datetime64"):
                continue

            # Trường hợp numeric (epoch giây/ms)
            if pd.api.types.is_numeric_dtype(s):
                s_float = s.astype("float64")
                # Heuristic: >1e12 => ms, >1e9 => s
                if s_float.dropna().gt(1e12).any():
                    dt = pd.to_datetime(s_float, unit="ms", errors="coerce", utc=True)
                elif s_float.dropna().gt(1e9).any():
                    dt = pd.to_datetime(s_float, unit="s", errors="coerce", utc=True)
                else:
                    dt = pd.to_datetime(s_float, unit="s", errors="coerce", utc=True)
                df_norm[col] = (
                    dt.dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
                    if dt.dt.tz is not None
                    else dt
                )
                continue

            # Trường hợp string ISO hoặc chuỗi khác
            if pd.api.types.is_string_dtype(s):
                # Thử parse ISO; pandas hỗ trợ hậu tố Z nếu utc=True
                dt = pd.to_datetime(s, errors="coerce", utc=True)
                if dt.notna().any():
                    df_norm[col] = (
                        dt.dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
                        if dt.dt.tz is not None
                        else dt
                    )

        return df_norm

    def truncate_table(self, table_name: str) -> bool:
        """
        Xóa tất cả dữ liệu trong table (Full Load) theo chuẩn SQL Server

        Args:
            table_name: Tên table cần truncate

        Returns:
            True nếu thành công, False nếu thất bại
        """
        table_full_name = self.table_mappings.get(table_name)
        if not table_full_name:
            logger.error(f"❌ Table mapping not found for: {table_name}")
            return False

        try:
            with self.db_engine.begin() as conn:
                schema, table = table_full_name.split(".")

                # Với SQL Server, TRUNCATE TABLE không thể dùng khi có FK tham chiếu.
                # Vì bảng orders được tham chiếu bởi nhiều bảng con, thay thế bằng DELETE theo thứ tự an toàn.
                if table_name == "orders":
                    logger.info(
                        "↩️ Detected parent table 'orders' — performing safe cascade DELETE on child tables first"
                    )
                    self._delete_shopee_children_tables(conn, schema)

                # Thực hiện DELETE thay cho TRUNCATE để không vướng FK
                delete_sql = f"DELETE FROM {table_full_name}"
                result = conn.execute(text(delete_sql))

                logger.info(
                    f"✅ Cleared table via DELETE: {table_full_name} (rows affected: {result.rowcount})"
                )
                return True

        except Exception as e:
            logger.error(f"❌ Failed to truncate table {table_full_name}: {str(e)}")
            return False

    def _delete_shopee_children_tables(self, conn, schema: str) -> None:
        """Xóa dữ liệu các bảng con của Shopee theo đúng thứ tự để đảm bảo ràng buộc FK.

        Thứ tự xóa (child -> parent):
          - package_items (tham chiếu packages, order_items)
          - order_item_locations (tham chiếu order_items)
          - packages (tham chiếu orders)
          - invoice (tham chiếu orders)
          - payment_info (tham chiếu orders)
          - order_pending_terms (tham chiếu orders)
          - order_warnings (tham chiếu orders)
          - prescription_images (tham chiếu orders)
          - buyer_proof_of_collection (tham chiếu orders)
          - order_items (tham chiếu orders)
          - recipient_address (tham chiếu orders)
        """
        tables_in_order = [
            "shopee_package_items",
            "shopee_order_item_locations",
            "shopee_packages",
            "shopee_invoice",
            "shopee_payment_info",
            "shopee_order_pending_terms",
            "shopee_order_warnings",
            "shopee_prescription_images",
            "shopee_buyer_proof_of_collection",
            "shopee_order_items",
            "shopee_recipient_address",
        ]

        for tbl in tables_in_order:
            full_name = f"{schema}.{tbl}"
            try:
                res = conn.execute(
                    text(
                        f"IF OBJECT_ID('{full_name}', 'U') IS NOT NULL DELETE FROM {full_name}"
                    )
                )
                # Một số driver không trả rowcount cho câu IF... nên cần try/except riêng
                affected = getattr(res, "rowcount", None)
                logger.info(
                    f"   🗑️ Cleared child table: {full_name}{'' if affected is None else f' (rows: {affected})'}"
                )
            except Exception as e:
                logger.warning(f"   ⚠️ Skipped deleting {full_name}: {e}")

    def load_dataframe_to_table(
        self, df: pd.DataFrame, table_name: str, if_exists: str = "append"
    ) -> bool:
        """
        Load DataFrame vào table

        Args:
            df: DataFrame cần load
            table_name: Tên table đích
            if_exists: Xử lý khi table đã tồn tại ('append', 'replace', 'fail')

        Returns:
            True nếu thành công, False nếu thất bại
        """
        if df.empty:
            logger.warning(f"⚠️ DataFrame for {table_name} is empty, skipping")
            return True

        table_full_name = self.table_mappings.get(table_name)
        if not table_full_name:
            logger.error(f"❌ Table mapping not found for: {table_name}")
            return False

        try:
            # Xóa duplicate cho tất cả bảng Shopee (full load)
            df_deduped = self._deduplicate_shopee_dataframe(df, table_name)

            if df_deduped.empty:
                logger.warning(
                    f"⚠️ DataFrame for {table_name} is empty after deduplication, skipping"
                )
                return True

            # Convert datetime columns to timezone-naive
            df_export = self._convert_datetime_to_naive(df_deduped)

            # Bổ sung etl_* theo +07 nếu thiếu (full load dùng INSERT qua to_sql)
            current_time_vn = pd.Timestamp.now(tz="Asia/Ho_Chi_Minh").tz_localize(None)
            if "etl_created_at" not in df_export.columns:
                df_export["etl_created_at"] = current_time_vn
            if "etl_updated_at" not in df_export.columns:
                df_export["etl_updated_at"] = current_time_vn

            # Load to database (giới hạn chunksize nhỏ để tránh quá tải tham số ODBC/SQL Server)
            df_export.to_sql(
                name=table_full_name.split(".")[1],  # Table name only
                con=self.db_engine,
                schema=table_full_name.split(".")[0],  # Schema name
                if_exists=if_exists,
                index=False,
                method="multi",
                chunksize=15,
            )

            logger.info(f"✅ Loaded {len(df_deduped)} rows to {table_full_name}")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to load DataFrame to {table_full_name}: {str(e)}")
            return False

    def _deduplicate_shopee_dataframe(
        self, df: pd.DataFrame, table_name: str
    ) -> pd.DataFrame:
        """
        Xóa duplicate theo khóa chính của từng bảng Shopee (chỉ cho full load)
        """
        # Khóa chính theo bảng (theo sql/00_master_setup.sql)
        pk_map = {
            "orders": ["order_sn"],
            "recipient_address": ["order_sn"],
            "order_items": ["order_sn", "order_item_id", "model_id"],
            "order_item_locations": [
                "order_sn",
                "order_item_id",
                "model_id",
                "location_id",
            ],
            "packages": ["order_sn", "package_number"],
            "package_items": [
                "order_sn",
                "package_number",
                "order_item_id",
                "model_id",
            ],
            "invoice": ["order_sn"],
            "payment_info": ["order_sn", "transaction_id"],
            "order_pending_terms": ["order_sn", "term"],
            "order_warnings": ["order_sn", "warning"],
            "prescription_images": ["order_sn", "image_url"],
            "buyer_proof_of_collection": ["order_sn", "image_url"],
        }

        primary_keys = pk_map.get(table_name, [])
        if not primary_keys:
            logger.warning(
                f"⚠️ No primary key mapping for {table_name}, skipping deduplication"
            )
            return df

        # Kiểm tra các cột khóa chính có tồn tại không
        missing_cols = [col for col in primary_keys if col not in df.columns]
        if missing_cols:
            logger.warning(
                f"⚠️ Missing primary key columns {missing_cols} for {table_name}, skipping deduplication"
            )
            return df

        # Xóa duplicate, giữ lại bản ghi cuối cùng
        original_count = len(df)
        df_deduped = df.drop_duplicates(subset=primary_keys, keep="last")
        removed_count = original_count - len(df_deduped)

        if removed_count > 0:
            logger.info(
                f"🔄 Deduplicated {table_name}: removed {removed_count} duplicates ({original_count} → {len(df_deduped)})"
            )

        return df_deduped

    def load_orders_full_load(self, dataframes: Dict[str, pd.DataFrame]) -> bool:
        """
        Load dữ liệu full load cho tất cả các bảng Shopee

        Args:
            dataframes: Dictionary chứa các DataFrame theo ERD

        Returns:
            True nếu thành công, False nếu thất bại
        """
        logger.info("🚀 Starting Shopee full load data loading...")

        try:
            # Load theo thứ tự để tránh foreign key constraint
            load_order = [
                "orders",  # Main table first
                "recipient_address",
                "order_items",
                "order_item_locations",
                "packages",
                "package_items",
                "invoice",
                "payment_info",
                "order_pending_terms",
                "order_warnings",
                "prescription_images",
                "buyer_proof_of_collection",
            ]

            success_count = 0
            total_count = len(load_order)

            for table_name in load_order:
                if table_name in dataframes:
                    df = dataframes[table_name]

                    if not df.empty:
                        # Truncate table trước khi load (full load)
                        if self.truncate_table(table_name):
                            if self.load_dataframe_to_table(df, table_name, "append"):
                                success_count += 1
                                logger.info(
                                    f"✅ Successfully loaded {table_name}: {len(df)} rows"
                                )
                            else:
                                logger.error(f"❌ Failed to load {table_name}")
                        else:
                            logger.error(f"❌ Failed to truncate {table_name}")
                    else:
                        logger.info(f"📭 Skipping empty {table_name}")
                        success_count += 1  # Empty table is considered success

            if success_count == total_count:
                logger.info(
                    f"🎉 Full load completed successfully: {success_count}/{total_count} tables"
                )
                return True
            else:
                logger.error(
                    f"❌ Full load failed: {success_count}/{total_count} tables"
                )
                return False

        except Exception as e:
            logger.error(f"❌ Full load failed with exception: {str(e)}")
            return False

    def _clean_dataframe_for_upsert(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Làm sạch DataFrame trước khi UPSERT để tránh lỗi pyodbc
        """
        if df.empty:
            return df

        df_clean = df.copy()
        fixed_issues = []

        # Xử lý NaT values cho datetime columns
        datetime_columns = df_clean.select_dtypes(include=["datetime64"]).columns
        for col in datetime_columns:
            na_count = df_clean[col].isna().sum()
            df_clean[col] = df_clean[col].replace({pd.NaT: None})
            if na_count > 0:
                fixed_issues.append(f"Fixed {na_count} NaT values in {col}")

        # Xử lý NaN values cho tất cả columns
        df_clean = df_clean.where(pd.notnull(df_clean), None)

        # Xử lý string columns có giá trị 'nan', 'N/A', 'null', 'None'
        for col in df_clean.columns:
            if df_clean[col].dtype == "object":
                df_clean[col] = (
                    df_clean[col]
                    .astype(str)
                    .replace(["nan", "N/A", "null", "NULL", "None", "none", ""], None)
                )

        # Xử lý numeric columns - convert string numbers to numeric
        numeric_columns = df_clean.select_dtypes(include=["int64", "float64"]).columns
        for col in numeric_columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce")

        # Log các vấn đề đã fix
        if fixed_issues:
            logger.info(f"Fixed data quality issues: {fixed_issues}")

        return df_clean

    def upsert_table(self, df: pd.DataFrame, table_name: str) -> bool:
        """
        UPSERT (MERGE + VALUES) cho từng bảng Shopee theo khóa tự nhiên với xử lý lỗi pyodbc được cải thiện.
        - Không tạo bảng tạm.
        - UPDATE có guard theo update_time nếu cột này tồn tại và/hoặc trường quan trọng.
        - Xử lý data quality và batch size tối ưu.
        """
        if df.empty:
            logger.info(f"📭 No data to upsert for Shopee.{table_name}")
            return True

        table_full_name = self.table_mappings.get(table_name)
        if not table_full_name:
            logger.error(f"❌ Table mapping not found for: {table_name}")
            return False

        # Làm sạch DataFrame trước khi xử lý
        df_clean = self._clean_dataframe_for_upsert(df)

        # Batch size tối ưu cho Shopee: 40 rows cho 50 cột
        # 40×50 = 2000 parameters < 2100 limit của SQL Server
        if table_name == "orders":
            batch_size = min(40, len(df_clean))
        else:
            batch_size = min(100, len(df_clean))

        logger.info(
            f"UPSERT {len(df_clean)} rows to Shopee.{table_name} with batch_size={batch_size}"
        )

        try:
            return self._execute_upsert_batch(df_clean, table_name, batch_size)
        except Exception as e:
            logger.error(f"Batch UPSERT failed for Shopee.{table_name}: {e}")
            # Fallback: xử lý từng row một
            return self._upsert_records_row_by_row(df_clean, table_name)

    def _execute_upsert_batch(
        self, df: pd.DataFrame, table_name: str, batch_size: int
    ) -> bool:
        """
        Thực hiện UPSERT batch với error handling tốt hơn cho Shopee Orders
        """
        if df.empty:
            return True

        table_full_name = self.table_mappings.get(table_name)
        if not table_full_name:
            logger.error(f"❌ Table mapping not found for: {table_name}")
            return False

        schema, target_table = table_full_name.split(".")

        # Khóa tự nhiên theo bảng
        pk_map = {
            # Theo sql/00_master_setup.sql
            "orders": ["order_sn"],
            "recipient_address": ["order_sn"],
            # PRIMARY KEY (order_sn, order_item_id, model_id)
            "order_items": ["order_sn", "order_item_id", "model_id"],
            # PRIMARY KEY (order_sn, order_item_id, model_id, location_id)
            "order_item_locations": [
                "order_sn",
                "order_item_id",
                "model_id",
                "location_id",
            ],
            # PRIMARY KEY (order_sn, package_number)
            "packages": ["order_sn", "package_number"],
            # PRIMARY KEY (order_sn, package_number, order_item_id, model_id)
            "package_items": [
                "order_sn",
                "package_number",
                "order_item_id",
                "model_id",
            ],
            # PRIMARY KEY (order_sn)
            "invoice": ["order_sn"],
            # PRIMARY KEY (order_sn, transaction_id)
            "payment_info": ["order_sn", "transaction_id"],
            # PRIMARY KEY (order_sn, term)
            "order_pending_terms": ["order_sn", "term"],
            # PRIMARY KEY (order_sn, warning)
            "order_warnings": ["order_sn", "warning"],
            # PRIMARY KEY (order_sn, image_url)
            "prescription_images": ["order_sn", "image_url"],
            # PRIMARY KEY (order_sn, image_url)
            "buyer_proof_of_collection": ["order_sn", "image_url"],
        }

        primary_keys = pk_map.get(table_name)
        if not primary_keys:
            logger.error(f"❌ Missing primary key mapping for Shopee.{table_name}")
            return False

        # Lấy danh sách cột thật của bảng đích
        db_columns = []
        try:
            with self.db_engine.connect() as conn:
                rows = conn.execute(
                    text(
                        """
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table
                        ORDER BY ORDINAL_POSITION
                        """
                    ),
                    {"schema": schema, "table": target_table},
                ).fetchall()
                db_columns = [r[0] for r in rows]
                if not db_columns:
                    raise RuntimeError("Empty INFORMATION_SCHEMA result")
        except Exception:
            try:
                with self.db_engine.connect() as conn:
                    res = conn.execute(
                        text(f"SELECT TOP 0 * FROM [{schema}].[{target_table}]")
                    )
                    db_columns = list(res.keys())
            except Exception as e2:
                logger.error(
                    f"Không thể lấy danh sách cột DB cho {table_full_name}: {e2}"
                )
                return False

        # Đảm bảo các cột khóa có trong DataFrame
        for pk in primary_keys:
            if pk not in df.columns:
                logger.error(
                    f"❌ Primary key column '{pk}' not found in DataFrame for Shopee.{table_name}"
                )
                return False

        # Xóa duplicate theo khóa chính trước khi upsert
        df_deduped = self._deduplicate_shopee_dataframe(df, table_name)
        if df_deduped.empty:
            logger.info(
                f"📭 No data to upsert after deduplication for Shopee.{table_name}"
            )
            return True

        # Đảm bảo có đủ các cột cần thiết cho schema
        if "source_request_id" not in df_deduped.columns:
            df_deduped["source_request_id"] = None
        if "ingested_at" not in df_deduped.columns:
            df_deduped["ingested_at"] = pd.Timestamp.now(
                tz="Asia/Ho_Chi_Minh"
            ).tz_localize(None)

        # Thiết lập etl_* theo +07 cho đường MERGE (INSERT)
        current_time_vn = pd.Timestamp.now(tz="Asia/Ho_Chi_Minh").tz_localize(None)
        if "etl_created_at" not in df_deduped.columns:
            df_deduped["etl_created_at"] = current_time_vn
        if "etl_updated_at" not in df_deduped.columns:
            df_deduped["etl_updated_at"] = current_time_vn

        # Chuẩn hóa datetime: chuyển epoch/ISO -> datetime, rồi bỏ timezone
        df_deduped = self._normalize_datetime_fields(df_deduped)
        df_deduped = self._convert_datetime_to_naive(df_deduped)

        # Danh sách cột theo DataFrame, intersect với DB columns
        df_columns: List[str] = df_deduped.columns.tolist()
        columns: List[str] = [c for c in df_columns if c in db_columns]
        if not columns:
            logger.error(
                f"Không có cột nào của DataFrame khớp với bảng {table_full_name}. DF cols: {df_columns} — DB cols: {db_columns}"
            )
            return False

        # Thêm các cột thiếu vào DataFrame với giá trị NULL
        missing_df_cols = [c for c in db_columns if c not in df_columns]
        if missing_df_cols:
            logger.warning(
                f"Shopee.{table_name}: Thêm {len(missing_df_cols)} cột thiếu với giá trị NULL"
            )
            for c in missing_df_cols:
                df_deduped[c] = None

        # Loại bỏ cột không có trong DB
        extra_df_cols = [c for c in df_columns if c not in db_columns]
        if extra_df_cols:
            logger.info(
                f"Shopee.{table_name}: Loại bỏ cột không có trong DB: {extra_df_cols}"
            )
            df_deduped = df_deduped.drop(columns=extra_df_cols, errors="ignore")

        # Reorder DataFrame columns theo thứ tự DB
        df_deduped = df_deduped[db_columns]

        # Chốt lại NULL an toàn sau khi thêm cột thiếu (theo phân tích chính xác)
        # Đảm bảo không còn NaN/NA/NaT trong toàn bộ DataFrame
        df_deduped = df_deduped.replace({np.nan: None})
        df_deduped = df_deduped.where(pd.notnull(df_deduped), None)

        # Lấy lại columns sau mọi chỉnh sửa để đảm bảo đồng nhất
        columns = df_deduped.columns.tolist()

        # Bảo đảm các cột khóa có trong DataFrame
        for pk in primary_keys:
            if pk not in columns:
                logger.error(
                    f"Primary key column '{pk}' not found in DataFrame for Shopee.{table_name}"
                )
                return False

        # Xây dựng các phần tử của câu MERGE
        col_list_sql = ", ".join([f"[{c}]" for c in columns])
        on_clause = " AND ".join([f"target.{pk} = source.{pk}" for pk in primary_keys])

        # Loại bỏ các cột ETL khỏi auto-update
        etl_cols = {"etl_created_at", "etl_updated_at", "etl_batch_id", "etl_source"}
        update_set_cols = [
            c for c in columns if c not in primary_keys and c not in etl_cols
        ]
        set_clauses = [f"target.{c} = source.{c}" for c in update_set_cols]

        # Giữ batch/source nếu có trong nguồn
        if "etl_batch_id" in columns:
            set_clauses.append("target.etl_batch_id = source.etl_batch_id")
        if "etl_source" in columns:
            set_clauses.append("target.etl_source = source.etl_source")

        # Cập nhật mốc ETL theo giờ Việt Nam (+07)
        set_clauses.append("target.etl_updated_at = DATEADD(HOUR, 7, GETUTCDATE())")
        update_set_sql = ",\n                        ".join(set_clauses)
        insert_values_sql = ", ".join([f"source.{c}" for c in columns])

        # Update guard logic
        update_guard = None
        if "update_time" in df_deduped.columns:
            update_guard = "ISNULL(target.update_time, '1900-01-01') < ISNULL(source.update_time, '1900-01-01')"

        extra_changes = []
        if table_name == "orders":
            if "order_status" in df_deduped.columns:
                extra_changes.append(
                    "ISNULL(target.order_status,'') <> ISNULL(source.order_status,'')"
                )
            if "shipping_carrier" in df_deduped.columns:
                extra_changes.append(
                    "ISNULL(target.shipping_carrier,'') <> ISNULL(source.shipping_carrier,'')"
                )

        if extra_changes:
            update_guard = f"({update_guard} OR {' OR '.join(extra_changes)})"

        matched_guard = (
            f"WHEN MATCHED THEN"
            if not update_guard
            else f"WHEN MATCHED AND {update_guard} THEN"
        )

        try:
            with self.db_engine.begin() as conn:
                total_rows = 0
                records = df_deduped.to_dict(orient="records")
                for i in range(0, len(records), batch_size):
                    batch = records[i : i + batch_size]

                    # Xây VALUES và tham số ràng buộc
                    values_rows = []
                    params: Dict[str, Any] = {}
                    for r_idx, row in enumerate(batch):
                        placeholders = []
                        for c in columns:
                            pname = f"p_{r_idx}_{c}"
                            placeholders.append(f":{pname}")
                            val = row.get(c, None)
                            # Tuyệt đối dùng pd.isna, đừng chỉ check float (theo phân tích chính xác)
                            if pd.isna(val):
                                val = None
                            params[pname] = val
                        values_rows.append(f"({', '.join(placeholders)})")

                    values_sql = ",\n                        ".join(values_rows)

                    merge_sql = f"""
                    MERGE [{schema}].[{target_table}] AS target
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
                f"UPSERT (no-temp) completed for Shopee.{table_name}: {len(df_deduped)} rows processed"
            )
            return True

        except Exception as e:
            logger.error(
                f"Error in _execute_upsert_batch for Shopee.{table_name}: {str(e)}"
            )
            raise

    def _upsert_records_row_by_row(self, df: pd.DataFrame, table_name: str) -> bool:
        """
        UPSERT từng row một khi batch UPSERT thất bại cho Shopee Orders
        """
        if df.empty:
            return True

        success_count = 0
        error_count = 0
        total_rows = len(df)

        logger.info(
            f"Starting row-by-row UPSERT for Shopee.{table_name}: {total_rows} rows"
        )

        for index, row in df.iterrows():
            try:
                # Tạo DataFrame với 1 row
                single_row_df = pd.DataFrame([row])

                # Thực hiện UPSERT cho 1 row
                result = self._execute_upsert_batch(single_row_df, table_name, 1)
                if result:
                    success_count += 1
                else:
                    error_count += 1

            except Exception as e:
                error_count += 1
                logger.error(f"Row {index} failed for Shopee.{table_name}: {e}")

                # Nếu quá nhiều lỗi, dừng lại
                if error_count > 10:
                    logger.error(
                        f"Too many errors ({error_count}), stopping upsert for Shopee.{table_name}"
                    )
                    break

        # Tính success rate thực tế
        actual_success_rate = (success_count / total_rows) * 100
        actual_error_rate = (error_count / total_rows) * 100

        # Log kết quả thực tế
        logger.info(
            f"Row-by-row UPSERT completed for Shopee.{table_name}: {success_count}/{total_rows} ({actual_success_rate:.1f}%) success, {error_count}/{total_rows} ({actual_error_rate:.1f}%) errors"
        )

        return success_count > 0

    # Các tiện ích kiểm thử/đếm bản ghi không còn dùng trong pipeline chính đã được loại bỏ
