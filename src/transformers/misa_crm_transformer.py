#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MISA CRM Data Transformer
Tích hợp với TikTok Shop Infrastructure - Cấu trúc src/
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging
import sys
import os
import gc

# Import shared utilities
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
from config.settings import settings
from src.utils.logging import setup_logging

logger = setup_logging(__name__)


class MISACRMTransformer:
    """
    MISA CRM Data Transformer - Tương tự TikTok Shop Transformer pattern
    """

    def __init__(self):
        """Khởi tạo MISA CRM Transformer"""
        self.batch_id = None
        logger.info(f"Khởi tạo MISA CRM Transformer cho {settings.company_name}")

    def set_batch_id(self, batch_id: str):
        """Set batch ID cho transformation session"""
        self.batch_id = batch_id
        logger.info(f"Set batch ID: {batch_id}")

    def _add_etl_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Thêm ETL metadata vào DataFrame - tương tự TikTok Shop pattern

        Args:
            df: DataFrame cần thêm metadata

        Returns:
            DataFrame với ETL metadata
        """
        df = df.copy()
        df["etl_batch_id"] = self.batch_id

        # Sử dụng giờ Việt Nam (+07) và lưu dạng tz-naive để khớp DATETIME2
        current_time = pd.Timestamp.now(tz="Asia/Ho_Chi_Minh").tz_localize(None)
        df["etl_created_at"] = current_time
        df["etl_updated_at"] = current_time
        df["etl_source"] = "misa_crm_api"

        return df

    def _optimize_memory_usage(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Tối ưu hóa việc sử dụng bộ nhớ của DataFrame bằng cách downcast các kiểu số
        và chuyển đổi các cột object có độ đa dạng thấp thành 'category'.
        """
        # Chỉ log nếu DataFrame lớn để tránh spam log
        if len(df) > 1000 or len(df.columns) > 50:
            logger.info(
                f"Optimizing memory for large DataFrame: {len(df)} rows, {len(df.columns)} cols"
            )

        for col in df.columns:
            col_type = df[col].dtype

            if col_type != object and "datetime" not in str(col_type):
                # Downcast numeric types
                c_min = df[col].min()
                c_max = df[col].max()
                if str(col_type)[:3] == "int":
                    if df[col].isnull().sum() == 0:
                        if c_min > 0:
                            if c_max < 255:
                                df[col] = df[col].astype("uint8")
                            elif c_max < 65535:
                                df[col] = df[col].astype("uint16")
                            elif c_max < 4294967295:
                                df[col] = df[col].astype("uint32")
                            else:
                                df[col] = df[col].astype("uint64")
                        else:
                            if c_min > -128 and c_max < 128:
                                df[col] = df[col].astype("int8")
                            elif c_min > -32768 and c_max < 32768:
                                df[col] = df[col].astype("int16")
                            elif c_min > -2147483648 and c_max < 2147483648:
                                df[col] = df[col].astype("int32")
                            else:
                                df[col] = df[col].astype("int64")
                elif str(col_type)[:5] == "float":
                    df[col] = df[col].astype("float32")

            elif "datetime" not in str(col_type):
                # Convert low-cardinality strings to category
                if df[col].nunique() / len(df) < 0.5:
                    df[col] = df[col].astype("category")

        # Chỉ log completion cho dataset lớn
        if len(df) > 1000 or len(df.columns) > 50:
            logger.info("Memory optimization completed")
        return df

    def transform_customers(self, customers_data: List[Dict]) -> pd.DataFrame:
        """
        Transform customers data

        Args:
            customers_data: Raw customers data từ API

        Returns:
            Transformed DataFrame
        """
        if len(customers_data) > 1000:
            logger.info(f"Transform {len(customers_data)} customers... (large dataset)")
        else:
            logger.info(f"Transform {len(customers_data)} customers...")

        if not customers_data:
            logger.warning("Không có dữ liệu customers để transform")
            return pd.DataFrame()

        # Convert to DataFrame
        df = pd.DataFrame(customers_data)

        # Data type conversions
        numeric_columns = [
            "annual_revenue",
            "debt",
            "debt_limit",
            "number_of_days_owed",
            "number_orders",
            "order_sales",
            "average_order_value",
            "average_number_of_days_between_purchases",
            "number_days_without_purchase",
            "billing_long",
            "billing_lat",
            "shipping_long",
            "shipping_lat",
            "total_score",
        ]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Date columns
        date_columns = [
            "purchase_date_recent",
            "purchase_date_first",
            "customer_since_date",
            "last_interaction_date",
            "last_visit_date",
            "last_call_date",
            "issued_on",
            "celebrate_date",
            "created_date",
            "modified_date",
            "last_modified_date",
        ]

        for col in date_columns:
            if col in df.columns:
                # Parse ISO có offset và chuẩn hóa về múi giờ Việt Nam (+07), lưu tz-naive
                df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
                df[col] = (
                    df[col].dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
                    if df[col].dt.tz is not None
                    else df[col]
                )
                df[col] = df[col].where(pd.notna(df[col]), None)

        # Boolean columns
        boolean_columns = [
            "is_personal",
            "inactive",
            "is_public",
            "is_distributor",
            "is_portal_access",
        ]

        for col in boolean_columns:
            if col in df.columns:
                # FIXED: Xử lý null values cho boolean columns (NaN → False)
                df[col] = df[col].fillna(False).astype(bool)

        # Add ETL metadata
        df = self._add_etl_metadata(df)

        # Optimize memory
        df = self._optimize_memory_usage(df)

        logger.info(f"Transform customers hoàn thành: {len(df)} records")
        return df

    def transform_sale_orders_streaming(
        self, sale_orders_data: List[Dict], batch_size: int = 500
    ) -> pd.DataFrame:
        """
        Transform sale orders với streaming approach để tránh memory issues
        FIXED: Giảm batch_size từ 1000 xuống 500 để tránh OOM
        Xử lý từng batch nhỏ thay vì load tất cả vào memory
        """
        if len(sale_orders_data) > 3000:
            logger.info(
                f"Transform {len(sale_orders_data)} sale orders (large dataset) with batch_size={batch_size}"
            )
        else:
            logger.info(
                f"Transform {len(sale_orders_data)} sale orders with batch_size={batch_size}"
            )

        # ENHANCED MEMORY MANAGEMENT: Force garbage collection before starting
        import gc

        gc.collect()

        processed_batches = []
        total_batches = (len(sale_orders_data) + batch_size - 1) // batch_size

        # ✅ THÊM LOGIC XỬ LÝ BATCHES
        for i in range(0, len(sale_orders_data), batch_size):
            batch_data = sale_orders_data[i : i + batch_size]
            batch_number = (i // batch_size) + 1

            logger.debug(
                f"Processing batch {batch_number}/{total_batches} ({len(batch_data)} orders)"
            )

            # Transform batch
            batch_df = self._transform_sale_orders_batch(batch_data)

            if not batch_df.empty:
                processed_batches.append(batch_df)

            # Memory cleanup after each batch
            del batch_data
            del batch_df
            gc.collect()

        # Combine all batches
        if processed_batches:
            final_df = pd.concat(processed_batches, ignore_index=True)
            logger.info(f"Transform sale orders hoàn thành: {len(final_df)} records")
            return final_df
        else:
            logger.warning("No batches processed successfully")
            return pd.DataFrame()

    def _transform_sale_orders_batch(self, batch_data: List[Dict]) -> pd.DataFrame:
        """
        Transform một batch nhỏ sale orders - logic cũ được tách riêng
        """
        logger.debug(
            f"Transform và flatten {len(batch_data)} sale orders trong batch..."
        )
        if not batch_data:
            return pd.DataFrame()

        # Pass 1: Thu thập tất cả các keys có thể có để đảm bảo cấu trúc DataFrame nhất quán
        all_order_keys = set()
        all_item_keys = set()
        has_any_items = False
        for order in batch_data:
            all_order_keys.update(
                k for k in order.keys() if k != "sale_order_product_mappings"
            )
            items = order.get("sale_order_product_mappings", [])
            if items:
                has_any_items = True
                for item in items:
                    all_item_keys.update(item.keys())

        # Nếu không có item nào trong toàn bộ dữ liệu, sử dụng danh sách cố định để tránh lỗi
        if not has_any_items:
            all_item_keys.update(
                [
                    "id",
                    "product_code",
                    "unit",
                    "price",
                    "amount",
                    "total",
                    "tax_percent",
                    "discount_percent",
                    "stock_name",
                    "description",
                ]
            )

        # Khởi tạo dict of lists
        flattened_data = {f"order_{key}": [] for key in all_order_keys}
        flattened_data.update({f"item_{key}": [] for key in all_item_keys})
        flattened_data["has_multiple_items"] = []
        flattened_data["total_items_in_order"] = []

        # Pass 2: Điền dữ liệu vào các list
        for order in batch_data:
            all_order_keys.update(
                k for k in order.keys() if k != "sale_order_product_mappings"
            )
            items = order.get("sale_order_product_mappings", [])
            if items:
                has_any_items = True
                for item in items:
                    all_item_keys.update(item.keys())

        # Nếu không có item nào trong toàn bộ dữ liệu, sử dụng danh sách cố định để tránh lỗi
        if not has_any_items:
            all_item_keys.update(
                [
                    "id",
                    "product_code",
                    "unit",
                    "price",
                    "amount",
                    "total",
                    "tax_percent",
                    "discount_percent",
                    "stock_name",
                    "description",
                ]
            )

        # Khởi tạo dict of lists
        flattened_data = {f"order_{key}": [] for key in all_order_keys}
        flattened_data.update({f"item_{key}": [] for key in all_item_keys})
        flattened_data["has_multiple_items"] = []
        flattened_data["total_items_in_order"] = []

        # Pass 2: Điền dữ liệu vào các list
        for order in batch_data:
            order_main = {
                k: v for k, v in order.items() if k != "sale_order_product_mappings"
            }
            product_mappings = order.get("sale_order_product_mappings", [])
            num_items = len(product_mappings)

            if num_items > 0:
                for item in product_mappings:
                    for key in all_order_keys:
                        flattened_data[f"order_{key}"].append(order_main.get(key))
                    for key in all_item_keys:
                        flattened_data[f"item_{key}"].append(item.get(key))

                    flattened_data["has_multiple_items"].append(num_items > 1)
                    flattened_data["total_items_in_order"].append(num_items)
            else:
                # Xử lý order không có item
                for key in all_order_keys:
                    flattened_data[f"order_{key}"].append(order_main.get(key))
                for key in all_item_keys:
                    flattened_data[f"item_{key}"].append(None)

                flattened_data["has_multiple_items"].append(False)
                flattened_data["total_items_in_order"].append(0)

        # Tạo DataFrame từ dict of lists
        df = pd.DataFrame(flattened_data)

        if df.empty:
            logger.warning("DataFrame rỗng sau khi flatten")
            return df

        # Data type conversions cho order fields
        order_numeric_columns = [
            "order_sale_order_amount",
            "order_total_summary",
            "order_tax_summary",
            "order_discount_summary",
            "order_to_currency_summary",
            "order_total_receipted_amount",
            "order_balance_receipt_amount",
            "order_invoiced_amount",
            "order_un_invoiced_amount",
            "order_exchange_rate",
        ]
        for col in order_numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Data type conversions cho item fields
        item_numeric_columns = [
            "item_price",
            "item_amount",
            "item_usage_unit_amount",
            "item_usage_unit_price",
            "item_total",
            "item_to_currency",
            "item_discount",
            "item_tax",
            "item_discount_percent",
            "item_price_after_tax",
            "item_price_after_discount",
            "item_to_currency_after_discount",
            "item_height",
            "item_width",
            "item_length",
            "item_radius",
            "item_mass",
            "item_exist_amount",
            "item_shipping_amount",
            "item_ratio",
            "item_custom_field1",
            "item_produced_quantity",
            "item_quantity_ordered",
        ]
        for col in item_numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Date columns
        order_date_columns = [
            "order_sale_order_date",
            "order_due_date",
            "order_book_date",
            "order_deadline_date",
            "order_delivery_date",
            "order_paid_date",
            "order_invoice_date",
            "order_production_date",
        ]
        for col in order_date_columns:
            if col in df.columns:
                # Chuẩn hóa về +07-naive
                df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
                df[col] = (
                    df[col].dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
                    if df[col].dt.tz is not None
                    else df[col]
                )
                df[col] = df[col].where(pd.notna(df[col]), None)

        item_date_columns = ["item_expire_date"]
        for col in item_date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
                df[col] = (
                    df[col].dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
                    if df[col].dt.tz is not None
                    else df[col]
                )
                df[col] = df[col].where(pd.notna(df[col]), None)

        # Boolean columns
        boolean_columns = ["order_is_use_currency", "item_is_promotion"]
        for col in boolean_columns:
            if col in df.columns:
                # FIXED: Xử lý null values cho boolean columns (NaN → False)
                df[col] = df[col].fillna(False).astype(bool)

        # Add ETL metadata
        df = self._add_etl_metadata(df)

        # Optimize memory
        df = self._optimize_memory_usage(df)

        logger.debug(
            f"Transform sale orders batch hoàn thành: {len(df)} rows (từ {len(batch_data)} orders)"
        )
        return df

    def transform_sale_orders_flattened(
        self, sale_orders_data: List[Dict]
    ) -> pd.DataFrame:
        """
        Legacy method - forward to streaming version for memory optimization
        """
        logger.info("Using streaming approach for memory optimization")
        return self.transform_sale_orders_streaming(sale_orders_data)

    def transform_contacts(self, contacts_data: List[Dict]) -> pd.DataFrame:
        """Transform contacts data"""
        logger.info(f"Transform {len(contacts_data)} contacts...")

        if not contacts_data:
            return pd.DataFrame()

        df = pd.DataFrame(contacts_data)

        # Data type conversions
        numeric_columns = [
            "mailing_long",
            "mailing_lat",
            "shipping_long",
            "shipping_lat",
            "total_score",
            "number_days_not_interacted",
        ]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Date columns
        date_columns = [
            "date_of_birth",
            "customer_since_date",
            "last_interaction_date",
            "last_visit_date",
            "last_call_date",
            "created_date",
            "modified_date",
        ]

        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
                df[col] = (
                    df[col].dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
                    if df[col].dt.tz is not None
                    else df[col]
                )
                df[col] = df[col].where(pd.notna(df[col]), None)

        # Boolean columns
        boolean_columns = ["email_opt_out", "phone_opt_out", "inactive", "is_public"]

        for col in boolean_columns:
            if col in df.columns:
                # FIXED: Xử lý null values cho boolean columns (NaN → False)
                df[col] = df[col].fillna(False).astype(bool)

        df = self._add_etl_metadata(df)

        # Optimize memory
        df = self._optimize_memory_usage(df)

        logger.info(f"Transform contacts hoàn thành: {len(df)} records")
        return df

    def transform_stocks(self, stocks_data: List[Dict]) -> pd.DataFrame:
        """Transform stocks data"""
        logger.info(f"Transform {len(stocks_data)} stocks...")

        if not stocks_data:
            return pd.DataFrame()

        df = pd.DataFrame(stocks_data)

        # Date columns
        date_columns = ["created_date", "modified_date"]

        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
                df[col] = (
                    df[col].dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
                    if df[col].dt.tz is not None
                    else df[col]
                )
                df[col] = df[col].where(pd.notna(df[col]), None)

        # Boolean columns
        boolean_columns = ["inactive"]

        for col in boolean_columns:
            if col in df.columns:
                # FIXED: Xử lý null values cho boolean columns (NaN → False)
                df[col] = df[col].fillna(False).astype(bool)

        df = self._add_etl_metadata(df)

        # Optimize memory
        df = self._optimize_memory_usage(df)

        logger.info(f"Transform stocks hoàn thành: {len(df)} records")
        return df

    def transform_products(self, products_data: List[Dict]) -> pd.DataFrame:
        """Transform products data"""
        logger.info(f"Transform {len(products_data)} products...")

        if not products_data:
            return pd.DataFrame()

        df = pd.DataFrame(products_data)

        # Data type conversions
        numeric_columns = [
            "unit_price",
            "purchased_price",
            "unit_cost",
            "unit_price1",
            "unit_price2",
            "unit_price_fixed",
        ]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Date columns
        date_columns = ["created_date", "modified_date"]

        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
                df[col] = (
                    df[col].dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
                    if df[col].dt.tz is not None
                    else df[col]
                )
                df[col] = df[col].where(pd.notna(df[col]), None)

        # Boolean columns
        boolean_columns = [
            "price_after_tax",
            "is_use_tax",
            "is_follow_serial_number",
            "is_set_product",
            "inactive",
            "is_public",
        ]

        for col in boolean_columns:
            if col in df.columns:
                # FIXED: Xử lý null values cho boolean columns (NaN → False)
                df[col] = df[col].fillna(False).astype(bool)

        df = self._add_etl_metadata(df)

        # Optimize memory
        df = self._optimize_memory_usage(df)

        logger.info(f"Transform products hoàn thành: {len(df)} records")
        return df

    def transform_all_endpoints(
        self, raw_data: Dict[str, List[Dict]], batch_id: str
    ) -> Dict[str, pd.DataFrame]:
        """
        Transform tất cả endpoint data với tối ưu hóa bộ nhớ.
        Xử lý tuần tự, giải phóng bộ nhớ sau mỗi bước.
        """
        self.set_batch_id(batch_id)
        logger.info(
            "Bắt đầu transform tất cả endpoint data với chế độ tối ưu hóa bộ nhớ..."
        )

        transformed_data = {}

        # Định nghĩa thứ tự xử lý và hàm tương ứng
        endpoint_map = {
            "customers": ("customers", self.transform_customers),
            "sale_orders": (
                "sale_orders_flattened",
                self.transform_sale_orders_streaming,
            ),  # Use streaming
            "contacts": ("contacts", self.transform_contacts),
            "stocks": ("stocks", self.transform_stocks),
            "products": ("products", self.transform_products),
        }

        for endpoint_key, (target_key, transform_func) in endpoint_map.items():
            if endpoint_key in raw_data and raw_data[endpoint_key]:
                logger.info(f"Đang xử lý endpoint: {endpoint_key}")

                # Lấy dữ liệu và xóa khỏi dict gốc để giải phóng bộ nhớ
                endpoint_data = raw_data.pop(endpoint_key)

                # Transform
                try:
                    # ENHANCED MEMORY: Force cleanup before processing large endpoint
                    if endpoint_key == "sale_orders":
                        gc.collect()
                        # Chỉ log cho dataset lớn
                        if len(endpoint_data) > 1000:
                            logger.info(
                                f"Pre-processing memory cleanup for large {endpoint_key}"
                            )

                    transformed_df = transform_func(endpoint_data)

                    # Ensure we have a valid DataFrame
                    if transformed_df is not None and not transformed_df.empty:
                        transformed_data[target_key] = transformed_df
                        # Gộp log thành 1 dòng ngắn gọn
                        logger.info(f"✅ {target_key}: {len(transformed_df)} records")
                    else:
                        logger.warning(f"❌ {endpoint_key}: returned empty/None")
                        transformed_data[target_key] = (
                            pd.DataFrame()
                        )  # Empty DataFrame instead of None

                    # ENHANCED MEMORY: Additional cleanup for large datasets
                    if endpoint_key == "sale_orders" and len(endpoint_data) > 1000:
                        gc.collect()

                except Exception as e:
                    logger.error(f"❌ {endpoint_key} failed: {str(e)}")
                    # Create empty DataFrame on error instead of None
                    transformed_data[target_key] = pd.DataFrame()
                    # Still cleanup even on error
                    del endpoint_data
                    gc.collect()
                    # Continue processing other endpoints instead of raising
                    continue

                # Giải phóng bộ nhớ một cách tường minh
                del endpoint_data
                del transformed_df
                gc.collect()
                # Chỉ log cho endpoint lớn
                if (
                    endpoint_key in ["sale_orders", "customers"]
                    and len(transformed_data.get(target_key, [])) > 500
                ):
                    logger.debug(f"Memory freed for {endpoint_key}")

        # Summary - Bỏ qua DataFrame None
        total_rows = sum(len(df) for df in transformed_data.values() if df is not None)
        logger.info(f"Transform hoàn thành tất cả endpoints: {total_rows} tổng rows")

        # Remove None values from result
        transformed_data = {k: v for k, v in transformed_data.items() if v is not None}

        return transformed_data

    def validate_flattened_data(
        self, flattened_df: pd.DataFrame, original_orders: List[Dict]
    ) -> Dict[str, Any]:
        """
        Validate flattened sale orders data

        Args:
            flattened_df: Flattened DataFrame
            original_orders: Original orders data

        Returns:
            Dict với validation results
        """
        logger.info("Đang validate flattened data...")

        validation_results = {
            "total_original_orders": len(original_orders),
            "total_flattened_rows": len(flattened_df),
            "unique_orders_in_flattened": 0,
            "orders_with_multiple_items": 0,
            "orders_without_items": 0,
            "total_items_original": 0,
            "total_items_flattened": 0,
            "validation_passed": False,
        }

        if flattened_df.empty:
            logger.warning("Flattened DataFrame rỗng")
            return validation_results

        validation_results["unique_orders_in_flattened"] = flattened_df[
            "order_id"
        ].nunique()

        # Count items trong original data
        for order in original_orders:
            items = order.get("sale_order_product_mappings", [])
            validation_results["total_items_original"] += len(items)

            if len(items) > 1:
                validation_results["orders_with_multiple_items"] += 1
            elif len(items) == 0:
                validation_results["orders_without_items"] += 1

        # Count items trong flattened data
        validation_results["total_items_flattened"] = len(
            flattened_df[flattened_df["item_id"].notna()]
        )

        # Validation checks
        checks = [
            validation_results["unique_orders_in_flattened"]
            == validation_results["total_original_orders"],
            validation_results["total_items_flattened"]
            == validation_results["total_items_original"],
        ]

        validation_results["validation_passed"] = all(checks)

        logger.info(f"📊 Validation Results:")
        logger.info(
            f"   Original orders: {validation_results['total_original_orders']}"
        )
        logger.info(f"   Flattened rows: {validation_results['total_flattened_rows']}")
        logger.info(
            f"   Unique orders: {validation_results['unique_orders_in_flattened']}"
        )
        logger.info(f"   Original items: {validation_results['total_items_original']}")
        logger.info(
            f"   Flattened items: {validation_results['total_items_flattened']}"
        )
        logger.info(
            f"   Validation: {'✅ PASSED' if validation_results['validation_passed'] else '❌ FAILED'}"
        )

        return validation_results
