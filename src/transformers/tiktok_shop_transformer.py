"""
Chuyển đổi dữ liệu cho dữ liệu đơn hàng TikTok Shop
Thực hiện cấu trúc phẳng (Option B) từ notebook
"""

import pandas as pd
import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import uuid
from src.utils.logging import setup_logging

logger = setup_logging("tiktok_shop_transformer")


class TikTokShopOrderTransformer:
    """Chuyển đổi dữ liệu đơn hàng TikTok Shop thành định dạng staging phẳng"""

    def __init__(self):
        self.batch_id = str(uuid.uuid4())

    def _safe_string(self, value: Any, max_length: int = None) -> str:
        """
        Safely convert value to string and truncate if needed

        Args:
            value: Value to convert
            max_length: Maximum length (None for no truncation)

        Returns:
            Safe string value
        """
        if value is None:
            return None

        str_value = str(value)

        if max_length and len(str_value) > max_length:
            logger.warning(
                f"Truncating string from {len(str_value)} to {max_length} chars"
            )
            return str_value[:max_length]

        return str_value

    def transform_orders_to_dataframe(
        self, orders: List[Dict[str, Any]], chunk_size: int = 50
    ) -> pd.DataFrame:
        """
        Chuyển đổi danh sách từ điển đơn hàng thành DataFrame phẳng với memory optimization

        Args:
            orders: Danh sách từ điển đơn hàng từ API
            chunk_size: Số orders xử lý mỗi chunk để tránh OOM

        Returns:
            DataFrame phẳng sẵn sàng để tải vào database
        """
        try:
            if not orders:
                logger.warning("Không có đơn hàng để chuyển đổi")
                return pd.DataFrame()

            logger.info(f"Transforming {len(orders)} orders in chunks of {chunk_size}")

            # Process orders in chunks để tránh memory issues
            all_dataframes = []
            total_chunks = (len(orders) + chunk_size - 1) // chunk_size

            for i in range(0, len(orders), chunk_size):
                chunk_num = (i // chunk_size) + 1
                chunk_orders = orders[i : i + chunk_size]

                logger.info(
                    f"Processing transformation chunk {chunk_num}/{total_chunks}: {len(chunk_orders)} orders"
                )

                # Transform chunk
                chunk_df = self._transform_orders_chunk(chunk_orders)
                all_dataframes.append(chunk_df)

                # Memory cleanup
                del chunk_orders

                if chunk_num % 5 == 0:  # Cleanup every 5 chunks
                    import gc

                    gc.collect()
                    logger.info(f"Memory cleanup after chunk {chunk_num}")

            # Combine all chunks
            logger.info(f"Combining {len(all_dataframes)} chunks into final DataFrame")
            final_df = pd.concat(all_dataframes, ignore_index=True)

            # Final cleanup
            del all_dataframes
            import gc

            gc.collect()

            logger.info(
                f"Transformation completed. Final DataFrame shape: {final_df.shape}"
            )
            return final_df

        except Exception as e:
            logger.error(f"Error in transform_orders_to_dataframe: {str(e)}")
            raise

    def _transform_orders_chunk(self, orders: List[Dict[str, Any]]) -> pd.DataFrame:
        """Transform a chunk of orders"""
        flattened_rows = []

        for order in orders:
            # Trích xuất thông tin cấp đơn hàng
            order_info = self._extract_order_info(order)

            # Trích xuất thông tin người nhận
            recipient_info = self._extract_recipient_info(order)

            # Trích xuất line items (làm phẳng mỗi item)
            line_items = order.get("line_items", [])

            if not line_items:
                # Nếu không có line items, tạo một hàng chỉ với thông tin đơn hàng
                row = {**order_info, **recipient_info}
                row.update(self._get_empty_item_fields())
                flattened_rows.append(row)
            else:
                # Tạo một hàng cho mỗi line item
                for item in line_items:
                    item_info = self._extract_item_info(item)

                    row = {**order_info, **recipient_info, **item_info}
                    flattened_rows.append(row)

        # Chuyển đổi thành DataFrame
        df = pd.DataFrame(flattened_rows)

        # Thêm metadata ETL
        df = self._add_etl_metadata(df)

        return df

    def _extract_order_info(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """Trích xuất thông tin cấp đơn hàng - FIXED mapping theo JSON structure thực tế"""
        # FIXED: payment nên là 'payment' không phải 'order_amount'
        payment_info = order.get("payment", {})
        packages = order.get("packages", [])
        package = packages[0] if packages else {}

        # JSON serialization for complex fields
        payment_info_json = json.dumps(payment_info) if payment_info else None
        packages_json = json.dumps(packages) if packages else None
        raw_order_json = json.dumps(order)

        return {
            # Order Basic Info - FIXED mapping theo JSON structure thực tế
            "order_id": self._safe_string(order.get("id"), 100),
            "order_status": self._safe_string(order.get("status"), 100),
            "buyer_email": self._safe_string(order.get("buyer_email"), 255),
            "buyer_message": order.get("buyer_message"),
            "create_time": self._safe_timestamp_utc(order.get("create_time")),
            "update_time": self._safe_timestamp_utc(order.get("update_time")),
            "fulfillment_type": self._safe_string(order.get("fulfillment_type"), 100),
            "payment_method_name": self._safe_string(
                order.get("payment_method_name"), 200
            ),
            "warehouse_id": self._safe_string(order.get("warehouse_id"), 100),
            "user_id": self._safe_string(order.get("user_id"), 100),
            "request_id": self._safe_string(order.get("request_id"), 200),
            "shop_id": self._safe_string(order.get("shop_id"), 100),
            "region": self._safe_string(order.get("region"), 50),
            "cancel_order_sla_time": self._safe_timestamp_utc(
                order.get("cancel_order_sla_time")
            ),
            "collection_due_time": self._safe_timestamp_utc(
                order.get("collection_due_time")
            ),
            "commerce_platform": self._safe_string(order.get("commerce_platform"), 100),
            "delivery_option_id": self._safe_string(
                order.get("delivery_option_id"), 100
            ),
            "delivery_option_name": self._safe_string(
                order.get("delivery_option_name"), 200
            ),
            "delivery_type": self._safe_string(order.get("delivery_type"), 100),
            "fulfillment_priority_level": self._safe_int(
                order.get("fulfillment_priority_level")
            ),
            "has_updated_recipient_address": self._safe_bool(
                order.get("has_updated_recipient_address")
            ),
            "is_cod": self._safe_bool(order.get("is_cod")),
            "is_on_hold_order": self._safe_bool(order.get("is_on_hold_order")),
            "is_replacement_order": self._safe_bool(order.get("is_replacement_order")),
            "is_sample_order": self._safe_bool(order.get("is_sample_order")),
            "order_type": self._safe_string(order.get("order_type"), 100),
            "paid_time": self._safe_timestamp_utc(order.get("paid_time")),
            "recommended_shipping_time": self._safe_timestamp_utc_milliseconds(
                order.get("recommended_shipping_time")
            ),
            "rts_sla_time": self._safe_timestamp_utc(order.get("rts_sla_time")),
            "shipping_due_time": self._safe_timestamp_utc(
                order.get("shipping_due_time")
            ),
            "shipping_provider": self._safe_string(order.get("shipping_provider"), 200),
            "shipping_provider_id": self._safe_string(
                order.get("shipping_provider_id"), 100
            ),
            "shipping_type": self._safe_string(order.get("shipping_type"), 100),
            "tts_sla_time": self._safe_timestamp_utc(order.get("tts_sla_time")),
            "tracking_number": self._safe_string(order.get("tracking_number"), 200),
            "is_buyer_request_cancel": self._safe_bool(
                order.get("is_buyer_request_cancel")
            ),
            "cancel_reason": order.get("cancel_reason"),
            "split_or_combine_tag": self._safe_string(
                order.get("split_or_combine_tag"), 100
            ),
            "rts_time": self._safe_timestamp_utc(order.get("rts_time")),
            # FIXED: Payment Info - Map từ 'payment' object theo JSON structure thực tế
            "payment_currency": self._safe_string(payment_info.get("currency"), 10),
            "payment_original_shipping_fee": self._safe_decimal(
                payment_info.get("original_shipping_fee")
            ),
            "payment_original_total_product_price": self._safe_decimal(
                payment_info.get("original_total_product_price")
            ),
            "payment_platform_discount": self._safe_decimal(
                payment_info.get("platform_discount")
            ),
            "payment_seller_discount": self._safe_decimal(
                payment_info.get("seller_discount")
            ),
            "payment_shipping_fee": self._safe_decimal(
                payment_info.get("shipping_fee")
            ),
            "payment_shipping_fee_cofunded_discount": self._safe_decimal(
                payment_info.get("shipping_fee_cofunded_discount")
            ),
            "payment_shipping_fee_platform_discount": self._safe_decimal(
                payment_info.get("shipping_fee_platform_discount")
            ),
            "payment_shipping_fee_seller_discount": self._safe_decimal(
                payment_info.get("shipping_fee_seller_discount")
            ),
            "payment_sub_total": self._safe_decimal(payment_info.get("sub_total")),
            "payment_tax": self._safe_decimal(payment_info.get("tax")),
            "payment_total_amount": self._safe_decimal(
                payment_info.get("total_amount")
            ),
            # Package Info - Lấy từ package đầu tiên (chỉ có 'id' theo JSON structure)
            "package_id": self._safe_string(package.get("id"), 100),
            # JSON Fields
            "payment_info_json": payment_info_json,
            "line_items_json": json.dumps(order.get("line_items", [])),
            "packages_json": packages_json,
            "raw_order_json": raw_order_json,
        }

    def _extract_recipient_info(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """Trích xuất thông tin người nhận/giao hàng - FIXED mapping theo JSON structure thực tế"""
        recipient = order.get("recipient_address", {})

        # JSON serialization for complex fields
        recipient_address_json = json.dumps(recipient) if recipient else None
        district_info_json = (
            json.dumps(recipient.get("district_info", []))
            if recipient.get("district_info")
            else None
        )

        return {
            # FIXED: Recipient Address Fields - Map theo JSON structure thực tế
            "recipient_address_detail": recipient.get(
                "address_detail"
            ),  # FIXED: từ 'detail' thành 'address_detail'
            "recipient_address_line1": recipient.get("address_line1"),
            "recipient_address_line2": recipient.get("address_line2"),
            "recipient_address_line3": recipient.get("address_line3"),
            "recipient_address_line4": recipient.get("address_line4"),
            "recipient_first_name": recipient.get("first_name"),
            "recipient_first_name_local_script": recipient.get(
                "first_name_local_script"
            ),
            "recipient_last_name": recipient.get("last_name"),
            "recipient_last_name_local_script": recipient.get("last_name_local_script"),
            "recipient_name": recipient.get("name"),
            "recipient_full_address": recipient.get("full_address"),
            "recipient_phone_number": recipient.get("phone_number"),
            "recipient_postal_code": recipient.get("postal_code"),
            "recipient_region_code": recipient.get("region_code"),
            "recipient_district_info": district_info_json,
            "recipient_address_json": recipient_address_json,
        }

    def _extract_item_info(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Trích xuất thông tin line item - FIXED mapping theo JSON structure thực tế"""
        # FIXED: Theo JSON structure thực tế, không có sku_info nested object
        # Tất cả fields đều ở level đầu của item

        return {
            # FIXED: Item Basic Info - Map trực tiếp từ item theo JSON structure thực tế
            "item_id": str(
                item.get("id", "")
            ),  # FIXED: sử dụng 'id' thay vì 'product_id'
            "item_product_id": item.get("product_id"),
            "item_product_name": item.get("product_name"),
            "item_sku_id": item.get("sku_id"),
            "item_sku_name": item.get(
                "sku_name"
            ),  # FIXED: trực tiếp từ item, không có sku_info
            "item_sku_type": item.get("sku_type"),  # FIXED: trực tiếp từ item
            "item_sku_image": item.get("sku_image"),  # FIXED: trực tiếp từ item
            "item_seller_sku": item.get("seller_sku"),  # FIXED: trực tiếp từ item
            "item_quantity": self._safe_int(item.get("quantity")),
            "item_currency": item.get("currency"),
            "item_display_status": item.get("display_status"),
            "item_is_gift": self._safe_bool(item.get("is_gift")),
            "item_original_price": self._safe_decimal(item.get("original_price")),
            "item_sale_price": self._safe_decimal(item.get("sale_price")),
            "item_platform_discount": self._safe_decimal(item.get("platform_discount")),
            "item_seller_discount": self._safe_decimal(item.get("seller_discount")),
            "item_package_id": item.get("package_id"),
            "item_package_status": item.get("package_status"),
            "item_shipping_provider_id": item.get("shipping_provider_id"),
            "item_shipping_provider_name": item.get("shipping_provider_name"),
            "item_tracking_number": item.get("tracking_number"),
            "item_cancel_reason": item.get("cancel_reason"),
            "item_rts_time": self._safe_timestamp_utc(item.get("rts_time")),
            # FIXED: Sử dụng generic JSON field thay vì sales_attributes
            "item_sku_attributes": (
                json.dumps(item) if item else None
            ),  # Store toàn bộ item data
        }

    def _get_empty_item_fields(self) -> Dict[str, Any]:
        """Lấy các trường item trống cho đơn hàng không có line items - Full item schema"""
        return {
            "item_id": None,
            "item_product_id": None,
            "item_product_name": None,
            "item_sku_id": None,
            "item_sku_name": None,
            "item_sku_type": None,
            "item_sku_image": None,
            "item_seller_sku": None,
            "item_quantity": None,
            "item_currency": None,
            "item_display_status": None,
            "item_is_gift": None,
            "item_original_price": None,
            "item_sale_price": None,
            "item_platform_discount": None,
            "item_seller_discount": None,
            "item_package_id": None,
            "item_package_status": None,
            "item_shipping_provider_id": None,
            "item_shipping_provider_name": None,
            "item_tracking_number": None,
            "item_cancel_reason": None,
            "item_rts_time": None,
            "item_sku_attributes": None,
        }

    def _add_etl_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add ETL metadata columns - Match database schema"""
        now = datetime.utcnow()

        df["etl_created_at"] = now
        df["etl_updated_at"] = now
        df["etl_batch_id"] = self.batch_id
        df["etl_source"] = "tiktok_shop_api"

        return df

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float (deprecated - use _safe_decimal)"""
        return self._safe_decimal(value)

    def _safe_decimal(self, value: Any) -> Optional[float]:
        """Safely convert to decimal/float for database"""
        if value is None or value == "":
            return None
        try:
            return float(str(value))
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to int"""
        if value is None or value == "":
            return None
        try:
            return int(float(str(value)))
        except (ValueError, TypeError):
            return None

    def _safe_timestamp_utc(self, value: Any) -> Optional[datetime]:
        """Safely convert timestamp value to UTC datetime, handling pandas Timestamp objects (thống nhất với Shopee)"""
        if value is None or value == "":
            return None
        try:
            # Handle pandas Timestamp objects
            if hasattr(value, "timestamp"):
                epoch_seconds = int(value.timestamp())
            else:
                # Handle regular int/float values
                epoch_seconds = int(float(str(value)))

            # Convert to UTC datetime (như Shopee)
            return pd.to_datetime(epoch_seconds, unit="s", utc=True)
        except (ValueError, TypeError, AttributeError):
            return None

    def _safe_timestamp_utc_milliseconds(self, value: Any) -> Optional[datetime]:
        """Safely convert millisecond timestamp to UTC datetime (dành cho recommended_shipping_time)"""
        if value is None or value == "":
            return None
        try:
            # Handle pandas Timestamp objects
            if hasattr(value, "timestamp"):
                epoch_seconds = int(value.timestamp())
            else:
                # Handle regular int/float values
                epoch_seconds = int(float(str(value)))

            # Convert milliseconds to seconds (nếu > 10 digits thì là milliseconds)
            if epoch_seconds > 1e10:  # If > 10 digits, it's milliseconds
                epoch_seconds = epoch_seconds // 1000

            # Convert to UTC datetime (giống các field khác)
            return pd.to_datetime(epoch_seconds, unit="s", utc=True)
        except (ValueError, TypeError, AttributeError):
            return None

    def _safe_bool(self, value: Any) -> Optional[bool]:
        """Safely convert to boolean"""
        if value is None or value == "":
            return None
        if isinstance(value, bool):
            return value
        if str(value).lower() in ("true", "1", "yes", "on"):
            return True
        if str(value).lower() in ("false", "0", "no", "off"):
            return False
        return None

    def validate_dataframe(self, df: pd.DataFrame) -> bool:
        """
        Validate transformed DataFrame

        Args:
            df: DataFrame to validate

        Returns:
            True if valid, False otherwise
        """
        try:
            if df.empty:
                logger.warning("DataFrame is empty")
                return False

            # Check required columns
            required_columns = ["order_id", "etl_batch_id"]
            missing_columns = [col for col in required_columns if col not in df.columns]

            if missing_columns:
                logger.error(f"Missing required columns: {missing_columns}")
                return False

            # Check for null order_ids
            null_order_ids = df["order_id"].isnull().sum()
            if null_order_ids > 0:
                logger.warning(f"Found {null_order_ids} rows with null order_id")

            # Log data quality metrics
            total_rows = len(df)
            unique_orders = df["order_id"].nunique()

            logger.info(
                f"DataFrame validation: {total_rows} rows, {unique_orders} unique orders"
            )

            return True

        except Exception as e:
            logger.error(f"Error validating DataFrame: {str(e)}")
            return False
