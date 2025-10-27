#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shopee Orders Data Transformer
Chuyển đổi dữ liệu JSON từ Shopee API thành các DataFrame theo thiết kế ERD
"""

import pandas as pd
import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import uuid
import sys
import os

# Import shared utilities
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
from src.utils.logging import setup_logging

logger = setup_logging(__name__)


class ShopeeOrderTransformer:
    """
    Shopee Orders Data Transformer - Chuyển đổi JSON thành các bảng DataFrame theo ERD
    """

    def __init__(self):
        """Khởi tạo Shopee Order Transformer"""
        self.batch_id = str(uuid.uuid4())
        logger.info(f"Khởi tạo Shopee Order Transformer - Batch ID: {self.batch_id}")

    def _unix_to_datetime(self, unix_timestamp):
        """Chuyển đổi Unix timestamp thành datetime, trả về None nếu timestamp = 0 hoặc None"""
        if not unix_timestamp or unix_timestamp == 0:
            return None
        try:
            return pd.to_datetime(unix_timestamp, unit="s", utc=True)
        except:
            return None

    def _safe_string(self, value: Any, max_length: int = None) -> str:
        """Safely convert value to string and truncate if needed"""
        if value is None:
            return None

        str_value = str(value)

        if max_length and len(str_value) > max_length:
            logger.warning(
                f"Truncating string from {len(str_value)} to {max_length} chars"
            )
            return str_value[:max_length]

        return str_value

    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to int"""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_bool(self, value: Any) -> Optional[bool]:
        """Safely convert value to boolean"""
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes", "on")
        return bool(value)

    def _add_etl_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Thêm ETL metadata vào DataFrame"""
        df = df.copy()
        df["etl_batch_id"] = self.batch_id
        df["etl_created_at"] = datetime.now()
        df["etl_updated_at"] = datetime.now()
        df["etl_source"] = "shopee_api"
        return df

    def transform_orders_to_dataframes(
        self, orders_list: List[Dict[str, Any]]
    ) -> Dict[str, pd.DataFrame]:
        """
        Chuyển đổi danh sách orders thành các DataFrame theo thiết kế ERD

        Args:
            orders_list: List chứa orders từ Shopee API

        Returns:
            Dictionary chứa các DataFrame theo ERD
        """
        logger.info(f"🔄 Transforming {len(orders_list)} orders into DataFrames")

        # Initialize all DataFrames
        dataframes = {
            "orders": [],
            "recipient_address": [],
            "order_items": [],
            "order_item_locations": [],
            "packages": [],
            "package_items": [],
        }

        for order in orders_list:
            order_sn = order.get("order_sn")
            if not order_sn:
                logger.warning("⚠️ Skipping order without order_sn")
                continue

            # Process each order
            self._process_single_order(order, dataframes)

        # Convert lists to DataFrames and add metadata
        result = {}
        for table_name, data_list in dataframes.items():
            if data_list:
                df = pd.DataFrame(data_list)
                df = self._add_etl_metadata(df)
                result[table_name] = df
                logger.info(f"✅ Created {table_name}: {len(df)} rows")
            else:
                # Create empty DataFrame with proper columns
                result[table_name] = pd.DataFrame()
                logger.info(f"📭 Empty {table_name}: 0 rows")

        return result

    def _process_single_order(self, order: Dict[str, Any], dataframes: Dict[str, List]):
        """Xử lý một order đơn lẻ và thêm vào các dataframes"""
        order_sn = order.get("order_sn")

        # 1. Orders table (main table)
        self._process_orders_table(order, dataframes["orders"])

        # 2. Recipient address table
        self._process_recipient_address(order, dataframes["recipient_address"])

        # 3. Order items and locations
        self._process_order_items(
            order, dataframes["order_items"], dataframes["order_item_locations"]
        )

        # 4. Packages and package items
        self._process_packages(
            order, dataframes["packages"], dataframes["package_items"]
        )

        # 5. Array tables (pending_terms, warnings, etc.) - REMOVED: không có trong API

    def _process_orders_table(self, order: Dict[str, Any], orders_list: List):
        """Xử lý bảng orders (bảng chính)"""
        order_data = {
            "order_sn": order.get("order_sn"),
            "region": order.get("region"),
            "currency": order.get("currency"),
            "cod": self._safe_bool(order.get("cod")),
            "total_amount": self._safe_float(order.get("total_amount")),
            "order_status": order.get("order_status"),
            "shipping_carrier": order.get("shipping_carrier"),
            "payment_method": order.get("payment_method"),
            "estimated_shipping_fee": self._safe_float(
                order.get("estimated_shipping_fee")
            ),
            "message_to_seller": order.get("message_to_seller"),
            "create_time": self._unix_to_datetime(order.get("create_time")),
            "update_time": self._unix_to_datetime(order.get("update_time")),
            "days_to_ship": self._safe_int(order.get("days_to_ship")),
            "ship_by_date": self._unix_to_datetime(order.get("ship_by_date")),
            "buyer_user_id": self._safe_int(order.get("buyer_user_id")),
            "buyer_username": order.get("buyer_username"),
            # Optional fields
            "actual_shipping_fee": self._safe_float(order.get("actual_shipping_fee")),
            "actual_shipping_fee_confirmed": self._safe_bool(
                order.get("actual_shipping_fee_confirmed")
            ),
            "goods_to_declare": self._safe_bool(order.get("goods_to_declare")),
            "note": order.get("note"),
            "note_update_time": self._unix_to_datetime(order.get("note_update_time")),
            "pay_time": self._unix_to_datetime(order.get("pay_time")),
            "dropshipper": order.get("dropshipper"),
            "dropshipper_phone": order.get("dropshipper_phone"),
            "split_up": self._safe_bool(order.get("split_up")),
            "buyer_cancel_reason": order.get("buyer_cancel_reason"),
            "cancel_by": order.get("cancel_by"),
            "cancel_reason": order.get("cancel_reason"),
            "buyer_cpf_id": order.get("buyer_cpf_id"),
            "fulfillment_flag": order.get("fulfillment_flag"),
            "pickup_done_time": self._unix_to_datetime(order.get("pickup_done_time")),
            "reverse_shipping_fee": self._safe_float(order.get("reverse_shipping_fee")),
            "order_chargeable_weight_gram": self._safe_int(
                order.get("order_chargeable_weight_gram")
            ),
            "edt_from": self._unix_to_datetime(order.get("edt_from")),
            "edt_to": self._unix_to_datetime(order.get("edt_to")),
            "booking_sn": order.get("booking_sn"),
            "advance_package": self._safe_bool(order.get("advance_package")),
            "return_request_due_date": self._unix_to_datetime(
                order.get("return_request_due_date")
            ),
            "is_buyer_shop_collection": self._safe_bool(
                order.get("is_buyer_shop_collection")
            ),
            "hot_listing_order": self._safe_bool(order.get("hot_listing_order")),
        }
        orders_list.append(order_data)

    def _process_recipient_address(self, order: Dict[str, Any], address_list: List):
        """Xử lý bảng recipient_address"""
        recipient_address = order.get("recipient_address")
        if not recipient_address:
            return

        order_sn = order.get("order_sn")
        address_data = {
            "order_sn": order_sn,
            "name": recipient_address.get("name"),
            "phone": recipient_address.get("phone"),
            "town": recipient_address.get("town"),
            "district": recipient_address.get("district"),
            "city": recipient_address.get("city"),
            "state": recipient_address.get("state"),
            "region": recipient_address.get("region"),
            "zipcode": recipient_address.get("zipcode"),
            "full_address": recipient_address.get("full_address"),
        }
        address_list.append(address_data)

    def _process_order_items(
        self, order: Dict[str, Any], items_list: List, locations_list: List
    ):
        """Xử lý bảng order_items và order_item_locations"""
        order_sn = order.get("order_sn")
        item_list = order.get("item_list", [])

        for item in item_list:
            # Order items table
            item_data = {
                "order_sn": order_sn,
                "order_item_id": self._safe_int(item.get("order_item_id")),
                "item_id": self._safe_int(item.get("item_id")),
                "item_name": item.get("item_name"),
                "item_sku": item.get("item_sku"),
                "model_id": self._safe_int(item.get("model_id")),
                "model_name": item.get("model_name"),
                "model_sku": item.get("model_sku"),
                "model_quantity_purchased": self._safe_int(
                    item.get("model_quantity_purchased")
                ),
                "model_original_price": self._safe_float(
                    item.get("model_original_price")
                ),
                "model_discounted_price": self._safe_float(
                    item.get("model_discounted_price")
                ),
                "wholesale": self._safe_bool(item.get("wholesale")),
                "weight": self._safe_float(item.get("weight")),
                "add_on_deal": self._safe_bool(item.get("add_on_deal")),
                "main_item": self._safe_bool(item.get("main_item")),
                "add_on_deal_id": self._safe_int(item.get("add_on_deal_id")),
                "promotion_type": item.get("promotion_type"),
                "promotion_id": self._safe_int(item.get("promotion_id")),
                "promotion_group_id": self._safe_int(item.get("promotion_group_id")),
                "is_prescription_item": self._safe_bool(
                    item.get("is_prescription_item")
                ),
                "is_b2c_owned_item": self._safe_bool(item.get("is_b2c_owned_item")),
                "consultation_id": item.get("consultation_id"),
                "image_url": (
                    item.get("image_info", {}).get("image_url")
                    if item.get("image_info")
                    else None
                ),
                "hot_listing_item": self._safe_bool(item.get("hot_listing_item")),
            }
            items_list.append(item_data)

            # Order item locations table
            product_location_id = item.get("product_location_id", [])
            if isinstance(product_location_id, list):
                for location_id in product_location_id:
                    locations_list.append(
                        {
                            "order_sn": order_sn,
                            "order_item_id": self._safe_int(item.get("order_item_id")),
                            "model_id": self._safe_int(item.get("model_id")),
                            "location_id": location_id,
                        }
                    )
            elif isinstance(product_location_id, str):
                locations_list.append(
                    {
                        "order_sn": order_sn,
                        "order_item_id": self._safe_int(item.get("order_item_id")),
                        "model_id": self._safe_int(item.get("model_id")),
                        "location_id": product_location_id,
                    }
                )

    def _process_packages(
        self, order: Dict[str, Any], packages_list: List, package_items_list: List
    ):
        """Xử lý bảng packages và package_items"""
        order_sn = order.get("order_sn")
        package_list = order.get("package_list", [])

        for package in package_list:
            # Packages table
            package_data = {
                "order_sn": order_sn,
                "package_number": package.get("package_number"),
                "logistics_status": package.get("logistics_status"),
                "logistics_channel_id": self._safe_int(
                    package.get("logistics_channel_id")
                ),
                "shipping_carrier": package.get("shipping_carrier"),
                "allow_self_design_awb": self._safe_bool(
                    package.get("allow_self_design_awb")
                ),
                "parcel_chargeable_weight_gram": self._safe_int(
                    package.get("parcel_chargeable_weight_gram")
                ),
                "group_shipment_id": self._safe_int(package.get("group_shipment_id")),
                "sorting_group": package.get("sorting_group"),
            }
            packages_list.append(package_data)

            # Package items table
            package_item_list = package.get("item_list", [])
            for pkg_item in package_item_list:
                package_items_list.append(
                    {
                        "order_sn": order_sn,
                        "package_number": package.get("package_number"),
                        "order_item_id": self._safe_int(pkg_item.get("order_item_id")),
                        "item_id": self._safe_int(pkg_item.get("item_id")),
                        "model_id": self._safe_int(pkg_item.get("model_id")),
                        "model_quantity": self._safe_int(
                            pkg_item.get("model_quantity")
                        ),
                        "promotion_group_id": self._safe_int(
                            pkg_item.get("promotion_group_id")
                        ),
                        "product_location_id": pkg_item.get("product_location_id"),
                        "parcel_chargeable_weight": self._safe_int(
                            pkg_item.get("parcel_chargeable_weight")
                        ),
                    }
                )

    def transform_orders_to_flat_dataframe(
        self, orders_list: List[Dict[str, Any]]
    ) -> pd.DataFrame:
        """
        Chuyển đổi orders thành một DataFrame phẳng (tương tự TikTok Shop pattern)
        Để tương thích với loader hiện tại

        Args:
            orders_list: List chứa orders từ Shopee API

        Returns:
            DataFrame phẳng chứa tất cả thông tin orders
        """
        logger.info(f"🔄 Transforming {len(orders_list)} orders into flat DataFrame")

        flat_data = []

        for order in orders_list:
            # Extract basic order info
            order_data = {
                "order_sn": order.get("order_sn"),
                "region": order.get("region"),
                "currency": order.get("currency"),
                "cod": self._safe_bool(order.get("cod")),
                "total_amount": self._safe_float(order.get("total_amount")),
                "order_status": order.get("order_status"),
                "shipping_carrier": order.get("shipping_carrier"),
                "payment_method": order.get("payment_method"),
                "estimated_shipping_fee": self._safe_float(
                    order.get("estimated_shipping_fee")
                ),
                "message_to_seller": order.get("message_to_seller"),
                "create_time": self._unix_to_datetime(order.get("create_time")),
                "update_time": self._unix_to_datetime(order.get("update_time")),
                "days_to_ship": self._safe_int(order.get("days_to_ship")),
                "ship_by_date": self._unix_to_datetime(order.get("ship_by_date")),
                "buyer_user_id": self._safe_int(order.get("buyer_user_id")),
                "buyer_username": order.get("buyer_username"),
                # Optional fields
                "actual_shipping_fee": self._safe_float(
                    order.get("actual_shipping_fee")
                ),
                "actual_shipping_fee_confirmed": self._safe_bool(
                    order.get("actual_shipping_fee_confirmed")
                ),
                "goods_to_declare": self._safe_bool(order.get("goods_to_declare")),
                "note": order.get("note"),
                "note_update_time": self._unix_to_datetime(
                    order.get("note_update_time")
                ),
                "pay_time": self._unix_to_datetime(order.get("pay_time")),
                "dropshipper": order.get("dropshipper"),
                "dropshipper_phone": order.get("dropshipper_phone"),
                "split_up": self._safe_bool(order.get("split_up")),
                "buyer_cancel_reason": order.get("buyer_cancel_reason"),
                "cancel_by": order.get("cancel_by"),
                "cancel_reason": order.get("cancel_reason"),
                "buyer_cpf_id": order.get("buyer_cpf_id"),
                "fulfillment_flag": order.get("fulfillment_flag"),
                "pickup_done_time": self._unix_to_datetime(
                    order.get("pickup_done_time")
                ),
                "reverse_shipping_fee": self._safe_float(
                    order.get("reverse_shipping_fee")
                ),
                "order_chargeable_weight_gram": self._safe_int(
                    order.get("order_chargeable_weight_gram")
                ),
                "edt_from": self._unix_to_datetime(order.get("edt_from")),
                "edt_to": self._unix_to_datetime(order.get("edt_to")),
                "booking_sn": order.get("booking_sn"),
                "advance_package": self._safe_bool(order.get("advance_package")),
                "return_request_due_date": self._unix_to_datetime(
                    order.get("return_request_due_date")
                ),
                "is_buyer_shop_collection": self._safe_bool(
                    order.get("is_buyer_shop_collection")
                ),
            }

            # Add recipient address info (flattened)
            recipient_address = order.get("recipient_address", {})
            if recipient_address:
                order_data.update(
                    {
                        "recipient_name": recipient_address.get("name"),
                        "recipient_phone": recipient_address.get("phone"),
                        "recipient_town": recipient_address.get("town"),
                        "recipient_district": recipient_address.get("district"),
                        "recipient_city": recipient_address.get("city"),
                        "recipient_state": recipient_address.get("state"),
                        "recipient_region": recipient_address.get("region"),
                        "recipient_zipcode": recipient_address.get("zipcode"),
                        "recipient_full_address": recipient_address.get("full_address"),
                    }
                )

            # Add item info (first item only for flat structure)
            item_list = order.get("item_list", [])
            if item_list:
                first_item = item_list[0]
                order_data.update(
                    {
                        "item_id": self._safe_int(first_item.get("item_id")),
                        "item_name": first_item.get("item_name"),
                        "item_sku": first_item.get("item_sku"),
                        "model_id": self._safe_int(first_item.get("model_id")),
                        "model_name": first_item.get("model_name"),
                        "model_sku": first_item.get("model_sku"),
                        "model_quantity_purchased": self._safe_int(
                            first_item.get("model_quantity_purchased")
                        ),
                        "model_original_price": self._safe_float(
                            first_item.get("model_original_price")
                        ),
                        "model_discounted_price": self._safe_float(
                            first_item.get("model_discounted_price")
                        ),
                        "item_weight": self._safe_float(first_item.get("weight")),
                        "item_image_url": (
                            first_item.get("image_info", {}).get("image_url")
                            if first_item.get("image_info")
                            else None
                        ),
                    }
                )

            # Add package info (first package only for flat structure)
            package_list = order.get("package_list", [])
            if package_list:
                first_package = package_list[0]
                order_data.update(
                    {
                        "package_number": first_package.get("package_number"),
                        "package_logistics_status": first_package.get(
                            "logistics_status"
                        ),
                        "package_shipping_carrier": first_package.get(
                            "shipping_carrier"
                        ),
                        "package_logistics_channel_id": self._safe_int(
                            first_package.get("logistics_channel_id")
                        ),
                    }
                )

            # Add invoice info
            invoice_data = order.get("invoice_data")
            if invoice_data:
                order_data.update(
                    {
                        "invoice_number": invoice_data.get("number"),
                        "invoice_series_number": invoice_data.get("series_number"),
                        "invoice_access_key": invoice_data.get("access_key"),
                        "invoice_issue_date": self._unix_to_datetime(
                            invoice_data.get("issue_date")
                        ),
                        "invoice_total_value": self._safe_float(
                            invoice_data.get("total_value")
                        ),
                        "invoice_products_total_value": self._safe_float(
                            invoice_data.get("products_total_value")
                        ),
                        "invoice_tax_code": invoice_data.get("tax_code"),
                    }
                )

            flat_data.append(order_data)

        # Create DataFrame and add metadata
        df = pd.DataFrame(flat_data)
        df = self._add_etl_metadata(df)

        logger.info(f"✅ Created flat DataFrame: {len(df)} rows")
        return df
