#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shopee Orders Data Loader
Tích hợp với Facolos Enterprise ETL Infrastructure
"""

import pandas as pd
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
        """Chuyển đổi datetime columns từ timezone-aware sang timezone-naive"""
        df_copy = df.copy()

        for col in df_copy.columns:
            if df_copy[col].dtype == "datetime64[ns, UTC]":
                df_copy[col] = df_copy[col].dt.tz_localize(None)
            elif "datetime" in str(df_copy[col].dtype):
                df_copy[col] = pd.to_datetime(df_copy[col], utc=True).dt.tz_localize(
                    None
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
                    # Nếu nhỏ hơn, cố gắng parse như giây
                    dt = pd.to_datetime(s_float, unit="s", errors="coerce", utc=True)
                df_norm[col] = dt.dt.tz_convert(None)
                continue

            # Trường hợp string ISO hoặc chuỗi khác
            if pd.api.types.is_string_dtype(s):
                # Thử parse ISO; pandas hỗ trợ hậu tố Z nếu utc=True
                dt = pd.to_datetime(s, errors="coerce", utc=True)
                # Nếu toàn bộ NaT (parse thất bại), giữ nguyên
                if dt.notna().any():
                    df_norm[col] = dt.dt.tz_convert(None)

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

    def load_orders_incremental(self, df: pd.DataFrame) -> bool:
        """
        Load dữ liệu incremental cho Shopee orders (UPSERT logic)

        Args:
            df: DataFrame chứa orders mới

        Returns:
            True nếu thành công, False nếu thất bại
        """
        logger.info("🔄 Starting Shopee incremental data loading...")

        if df.empty:
            logger.info("📭 No incremental data to load")
            return True

        try:
            # Chuẩn hóa datetime: chuyển epoch/ISO -> datetime, rồi bỏ timezone
            df_export = self._normalize_datetime_fields(df)
            df_export = self._convert_datetime_to_naive(df_export)

            target_full = self.table_mappings["orders"]
            schema, target_table = target_full.split(".")

            # 1) Ghi batch vào bảng tạm trong cùng schema (tránh lỗi ODBC bằng chunksize nhỏ)
            temp_table = (
                f"temp_shopee_orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )

            df_export.to_sql(
                name=temp_table,
                con=self.db_engine,
                schema=schema,
                if_exists="replace",
                index=False,
                method="multi",
                chunksize=15,
            )

            # 2) MERGE (UPSERT) theo khóa order_sn, chỉ UPDATE khi source.update_time mới hơn
            merge_sql = f"""
            MERGE [{schema}].[{target_table}] AS target
            USING [{schema}].[{temp_table}] AS source
              ON target.order_sn = source.order_sn

            WHEN MATCHED AND (
                 ISNULL(target.update_time, '1900-01-01') < ISNULL(source.update_time, '1900-01-01')
              OR ISNULL(target.order_status,'') <> ISNULL(source.order_status,'')
              OR ISNULL(target.shipping_carrier,'') <> ISNULL(source.shipping_carrier,'')
            ) THEN UPDATE SET
              target.region = source.region,
              target.currency = source.currency,
              target.cod = source.cod,
              target.total_amount = source.total_amount,
              target.order_status = source.order_status,
              target.shipping_carrier = source.shipping_carrier,
              target.payment_method = source.payment_method,
              target.estimated_shipping_fee = source.estimated_shipping_fee,
              target.message_to_seller = source.message_to_seller,
              target.create_time = source.create_time,
              target.update_time = source.update_time,
              target.days_to_ship = source.days_to_ship,
              target.ship_by_date = source.ship_by_date,
              target.buyer_user_id = source.buyer_user_id,
              target.buyer_username = source.buyer_username,
              target.actual_shipping_fee = source.actual_shipping_fee,
              target.actual_shipping_fee_confirmed = source.actual_shipping_fee_confirmed,
              target.goods_to_declare = source.goods_to_declare,
              target.note = source.note,
              target.note_update_time = source.note_update_time,
              target.pay_time = source.pay_time,
              target.dropshipper = source.dropshipper,
              target.dropshipper_phone = source.dropshipper_phone,
              target.split_up = source.split_up,
              target.buyer_cancel_reason = source.buyer_cancel_reason,
              target.cancel_by = source.cancel_by,
              target.cancel_reason = source.cancel_reason,
              target.buyer_cpf_id = source.buyer_cpf_id,
              target.fulfillment_flag = source.fulfillment_flag,
              target.pickup_done_time = source.pickup_done_time,
              target.reverse_shipping_fee = source.reverse_shipping_fee,
              target.order_chargeable_weight_gram = source.order_chargeable_weight_gram,
              target.prescription_check_status = source.prescription_check_status,
              target.pharmacist_name = source.pharmacist_name,
              target.prescription_approval_time = source.prescription_approval_time,
              target.prescription_rejection_time = source.prescription_rejection_time,
              target.edt_from = source.edt_from,
              target.edt_to = source.edt_to,
              target.booking_sn = source.booking_sn,
              target.advance_package = source.advance_package,
              target.return_request_due_date = source.return_request_due_date,
              target.is_buyer_shop_collection = source.is_buyer_shop_collection,
              target.etl_updated_at = GETUTCDATE()

            WHEN NOT MATCHED BY TARGET THEN
              INSERT (
                order_sn, region, currency, cod, total_amount, order_status, shipping_carrier,
                payment_method, estimated_shipping_fee, message_to_seller, create_time, update_time,
                days_to_ship, ship_by_date, buyer_user_id, buyer_username, actual_shipping_fee,
                actual_shipping_fee_confirmed, goods_to_declare, note, note_update_time, pay_time,
                dropshipper, dropshipper_phone, split_up, buyer_cancel_reason, cancel_by, cancel_reason,
                buyer_cpf_id, fulfillment_flag, pickup_done_time, reverse_shipping_fee,
                order_chargeable_weight_gram, prescription_check_status, pharmacist_name,
                prescription_approval_time, prescription_rejection_time, edt_from, edt_to, booking_sn,
                advance_package, return_request_due_date, is_buyer_shop_collection,
                etl_batch_id, etl_created_at, etl_updated_at, etl_source
              )
              VALUES (
                source.order_sn, source.region, source.currency, source.cod, source.total_amount,
                source.order_status, source.shipping_carrier, source.payment_method,
                source.estimated_shipping_fee, source.message_to_seller, source.create_time,
                source.update_time, source.days_to_ship, source.ship_by_date, source.buyer_user_id,
                source.buyer_username, source.actual_shipping_fee, source.actual_shipping_fee_confirmed,
                source.goods_to_declare, source.note, source.note_update_time, source.pay_time,
                source.dropshipper, source.dropshipper_phone, source.split_up, source.buyer_cancel_reason,
                source.cancel_by, source.cancel_reason, source.buyer_cpf_id, source.fulfillment_flag,
                source.pickup_done_time, source.reverse_shipping_fee, source.order_chargeable_weight_gram,
                source.prescription_check_status, source.pharmacist_name,
                source.prescription_approval_time, source.prescription_rejection_time,
                source.edt_from, source.edt_to, source.booking_sn, source.advance_package,
                source.return_request_due_date, source.is_buyer_shop_collection,
                source.etl_batch_id, source.etl_created_at, source.etl_updated_at, source.etl_source
              );
            """

            with self.db_engine.begin() as conn:
                conn.execute(text(merge_sql))
                # Dọn dẹp temp table (trong cùng transaction)
                conn.execute(text(f"DROP TABLE [{schema}].[{temp_table}]"))

            logger.info(
                f"✅ Upserted {len(df_export)} incremental orders into {target_full} via MERGE"
            )
            return True

        except Exception as e:
            logger.error(f"❌ Incremental load failed: {str(e)}")
            return False

    def upsert_table(self, df: pd.DataFrame, table_name: str) -> bool:
        """
        UPSERT (MERGE + VALUES) cho từng bảng Shopee theo khóa tự nhiên.
        - Không tạo bảng tạm.
        - UPDATE có guard theo update_time nếu cột này tồn tại và/hoặc trường quan trọng.
        """
        if df.empty:
            logger.info(f"📭 No data to upsert for Shopee.{table_name}")
            return True

        table_full_name = self.table_mappings.get(table_name)
        if not table_full_name:
            logger.error(f"❌ Table mapping not found for: {table_name}")
            return False

        try:
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

            # Chuẩn hóa datetime: chuyển epoch/ISO -> datetime, rồi bỏ timezone
            df_export = self._normalize_datetime_fields(df_deduped)
            df_export = self._convert_datetime_to_naive(df_export)

            columns = df_export.columns.tolist()
            col_list_sql = ", ".join([f"[{c}]" for c in columns])
            on_clause = " AND ".join(
                [f"target.{pk} = source.{pk}" for pk in primary_keys]
            )
            # Loại bỏ các cột ETL khỏi auto-update để tránh set trùng và kiểm soát rõ ràng
            etl_cols = {
                "etl_created_at",
                "etl_updated_at",
                "etl_batch_id",
                "etl_source",
            }
            update_set_cols = [
                c for c in columns if c not in primary_keys and c not in etl_cols
            ]

            update_guard = None
            if "update_time" in df_export.columns:
                update_guard = "ISNULL(target.update_time, '1900-01-01') < ISNULL(source.update_time, '1900-01-01')"

            extra_changes = []
            if table_name == "orders":
                if "order_status" in df_export.columns:
                    extra_changes.append(
                        "ISNULL(target.order_status,'') <> ISNULL(source.order_status,'')"
                    )
                if "shipping_carrier" in df_export.columns:
                    extra_changes.append(
                        "ISNULL(target.shipping_carrier,'') <> ISNULL(source.shipping_carrier,'')"
                    )

            if extra_changes:
                if update_guard:
                    update_guard = (
                        f"({update_guard} OR " + " OR ".join(extra_changes) + ")"
                    )
                else:
                    update_guard = "(" + " OR ".join(extra_changes) + ")"

            # Xây dựng SET: dữ liệu nghiệp vụ cập nhật từ source; etl_batch_id/etl_source (nếu có) và etl_updated_at dùng thời gian hiện tại
            set_clauses = [f"target.{c} = source.{c}" for c in update_set_cols]
            if "etl_batch_id" in columns:
                set_clauses.append("target.etl_batch_id = source.etl_batch_id")
            if "etl_source" in columns:
                set_clauses.append("target.etl_source = source.etl_source")
            set_clauses.append("target.etl_updated_at = GETUTCDATE()")
            update_set_sql = ",\n                            ".join(set_clauses)
            insert_values_sql = ", ".join([f"source.{c}" for c in columns])

            # Chia lô nhỏ để tránh câu lệnh quá dài
            batch_size = min(200, len(df_export)) if len(df_export) > 0 else 0

            with self.db_engine.begin() as conn:
                total_rows = 0
                records = df_export.to_dict(orient="records")
                for i in range(0, len(records), batch_size):
                    batch = records[i : i + batch_size]

                    values_rows = []
                    params = {}
                    for r_idx, row in enumerate(batch):
                        placeholders = []
                        for c in columns:
                            pname = f"p_{r_idx}_{c}"
                            placeholders.append(f":{pname}")
                            params[pname] = row.get(c, None)
                        values_rows.append(f"({', '.join(placeholders)})")

                    values_sql = ",\n                            ".join(values_rows)

                    matched_guard = "WHEN MATCHED THEN"
                    if update_guard:
                        matched_guard = f"WHEN MATCHED AND {update_guard} THEN"

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

            logger.info(f"✅ Upserted {len(df_export)} rows into {table_full_name}")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to upsert Shopee.{table_name}: {str(e)}")
            return False

    # Các tiện ích kiểm thử/đếm bản ghi không còn dùng trong pipeline chính đã được loại bỏ
