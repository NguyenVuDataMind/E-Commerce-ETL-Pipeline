"""
Trích xuất dữ liệu từ TikTok Shop API
Dựa trên implementation                      # Request body cho POST request - FULL LOAD: Lấy TẤT CẢ orders
                # Từ 1/7/2024 đến hiện tại (theo yêu cầu business)
                request_body = {
                    'create_time_ge': int(datetime(2024, 7, 1).timestamp()),  # Từ 1/7/2024
                    'create_time_lt': end_time
                }     # Request body cho POST request theo notebook pattern - chỉ date filters
                # FULL LOAD: Lấy orders từ 1 năm trước để tránh timeout
                request_body = {
                    'create_time_ge': int(datetime(2023, 1, 1).timestamp()),  # Từ 2023 thay vì 2020
                    'create_time_lt': end_time
                }ebook đã hoạt động (Steps 6-8)
"""

import requests
import time
import json
import pandas as pd
import logging
import sys
import os
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta

# Add project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from src.utils.auth import TikTokAuthenticator
from src.utils.logging import setup_logging
from config.settings import settings

logger = setup_logging("tiktok_shop_extractor")


class TikTokShopOrderExtractor:
    """Trích xuất dữ liệu đơn hàng từ TikTok Shop API"""

    def __init__(self):
        self.auth = TikTokAuthenticator()
        self.logger = logger  # Add logger reference

    def stream_orders_lightweight(
        self,
        start_time: int,
        end_time: int,
        batch_size: int = 20,  # Increased for full load
        order_status: Optional[str] = None,
    ):
        """
        Generator để stream orders theo batch nhỏ - tránh OOM hoàn toàn
        Yield từng batch orders thay vì load tất cả vào memory
        """
        try:
            if not self.auth.ensure_valid_token():
                raise RuntimeError("Cannot authenticate with TikTok Shop API")

            cursor = ""
            page_number = 1
            total_processed = 0

            self.logger.info(
                f"Starting lightweight streaming with batch_size={batch_size}"
            )

            while True:
                self.logger.info(f"Streaming page {page_number}, cursor: '{cursor}'")

                # Parameters cho API call
                params = {
                    "app_key": self.auth.app_key,
                    "timestamp": str(int(time.time())),
                    "shop_cipher": self.auth.shop_cipher,
                    "sign_method": "hmac_sha256",
                    "page_size": batch_size,  # Increased for full load efficiency
                    "sort_order": "DESC",
                    "sort_field": "create_time",
                }

                if cursor:
                    params["cursor"] = cursor

                # Request body
                request_body = {
                    "create_time_ge": start_time,
                    "create_time_lt": end_time,
                }

                body_string = json.dumps(
                    request_body, separators=(",", ":"), sort_keys=True
                )
                signature = self.auth.generate_signature(
                    "/order/202309/orders/search", params, body_string
                )
                params["sign"] = signature

                headers = {
                    "x-tts-access-token": self.auth.access_token,
                    "Content-Type": "application/json",
                }

                url = f"{self.auth.base_api_url}/order/202309/orders/search"

                # API call
                response = requests.post(
                    url, params=params, data=body_string, headers=headers, timeout=30
                )

                if response.status_code == 200:
                    data = response.json()
                    if data.get("code") == 0:
                        orders = data.get("data", {}).get("orders", [])

                        if orders:
                            total_processed += len(orders)
                            self.logger.info(
                                f"Page {page_number}: Yielding {len(orders)} orders, total processed: {total_processed}"
                            )

                            # YIELD batch nhỏ để không tích tụ trong memory
                            yield orders

                        # Check pagination
                        data_section = data.get("data", {})
                        more = data_section.get("more", False)
                        next_cursor = data_section.get("next_cursor", "")

                        self.logger.info(
                            f"Pagination info - more: {more}, next_cursor: '{next_cursor}'"
                        )

                        if more and next_cursor:
                            cursor = next_cursor
                            page_number += 1
                            self.logger.info(
                                f"Continuing to next page with cursor: {cursor}"
                            )
                            time.sleep(0.5)  # Rate limiting
                        else:
                            # Stop pagination if more=False or no next_cursor
                            self.logger.info(
                                f"Streaming complete. Total processed: {total_processed}"
                            )
                            break

                    else:
                        error_message = f"API Error: {data.get('message')}"
                        self.logger.error(error_message)
                        raise RuntimeError(error_message)
                else:
                    error_message = (
                        f"HTTP Error {response.status_code}: {response.text}"
                    )
                    self.logger.error(error_message)
                    raise RuntimeError(error_message)

        except Exception as e:
            self.logger.error(f"Error in stream_orders_lightweight: {e}")
            raise RuntimeError("Streaming extraction failed")

    def search_orders_for_ids(
        self,
        start_time: int,
        end_time: int,
        order_status: Optional[str] = None,
        page_size: int = 100,
    ) -> List[str]:  # FIXED: Tăng từ 10 lên 100 để lấy nhiều orders hơn
        """
        Tìm kiếm ID đơn hàng - LIGHTWEIGHT với page_size nhỏ để tránh OOM
        """
        try:
            # Ensure valid authentication
            if not self.auth.ensure_valid_token():
                raise RuntimeError("Cannot authenticate with TikTok Shop API")

            all_order_ids = []
            page_token = ""  # TikTok Shop sử dụng page_token
            page_number = 1

            # Chỉ log starting message nếu không ở QUIET mode
            if self.logger.level <= logging.INFO:
                self.logger.info(
                    f"Starting pagination search with page_size={page_size}"
                )

            while True:
                # Chỉ log nếu không ở QUIET mode (WARNING level)
                if self.logger.level <= logging.INFO:
                    self.logger.info(
                        f"Fetching page {page_number}, page_token: '{page_token}'"
                    )

                # Parameters theo TikTok Shop API specification
                params = {
                    "app_key": self.auth.app_key,
                    "timestamp": str(int(time.time())),
                    "shop_cipher": self.auth.shop_cipher,
                    "sign_method": "hmac_sha256",
                    "page_size": page_size,
                    "sort_order": "DESC",
                    "sort_field": "create_time",
                }

                if page_token:
                    params["page_token"] = page_token

                # Request body cho POST request theo TikTok Shop API specification
                request_body = {
                    "create_time_ge": start_time,
                    "create_time_lt": end_time,
                }

                # Serialize body string để dùng cho signature và request
                body_string = json.dumps(
                    request_body, separators=(",", ":"), sort_keys=True
                )

                # Generate signature với body
                signature = self.auth.generate_signature(
                    "/order/202309/orders/search", params, body_string
                )
                params["sign"] = signature

                headers = {
                    "x-tts-access-token": self.auth.access_token,
                    "Content-Type": "application/json",
                }

                url = f"{self.auth.base_api_url}/order/202309/orders/search"

                # POST request với body theo notebook pattern
                response = requests.post(
                    url, params=params, data=body_string, headers=headers, timeout=30
                )

                if response.status_code == 200:
                    data = response.json()
                    if data.get("code") == 0:
                        orders = data.get("data", {}).get("orders", [])
                        order_ids = [order["id"] for order in orders]
                        all_order_ids.extend(order_ids)

                        # Chỉ log nếu không ở QUIET mode
                        if self.logger.level <= logging.INFO:
                            self.logger.info(
                                f"Page {page_number}: Found {len(order_ids)} orders, total: {len(all_order_ids)}"
                            )
                            # Debug: Log page_size để kiểm tra
                            self.logger.debug(
                                f"Requested page_size: {page_size}, received: {len(order_ids)}"
                            )

                        # Check pagination theo TikTok Shop API response format
                        data_section = data.get("data", {})
                        next_page_token = data_section.get("next_page_token", "")
                        total_count = data_section.get("total_count", 0)

                        # Log pagination status để debug
                        self.logger.info(
                            f"Pagination info - next_page_token: '{next_page_token}', total: {total_count}"
                        )

                        # Check for more pages theo API spec
                        if not next_page_token:
                            self.logger.info(
                                "No more pages available - pagination complete"
                            )
                            break

                        # Continue to next page
                        page_token = next_page_token
                        page_number += 1

                        if self.logger.level <= logging.DEBUG:
                            self.logger.debug(f"Continuing to page {page_number}")
                        time.sleep(0.5)  # Rate limiting

                    else:
                        error_message = f"API Error: {data.get('message')}"
                        self.logger.error(error_message)
                        raise RuntimeError(error_message)
                else:
                    error_message = (
                        f"HTTP Error {response.status_code}: {response.text}"
                    )
                    self.logger.error(error_message)
                    raise RuntimeError(error_message)

            # Log final count summary (luôn log để có tổng kết)
            self.logger.info(f"Total order IDs found: {len(all_order_ids)}")
            return all_order_ids

        except Exception as e:
            self.logger.error(f"Error in search_orders_for_ids: {e}")
            raise RuntimeError(
                "Không thể thực hiện request - có thể do lỗi token hoặc kết nối database"
            )

    def _search_orders_full_by_update(
        self, start_time: int, end_time: int, page_size: int = 50
    ) -> List[Dict[str, Any]]:
        """Trả về FULL orders trực tiếp từ /orders/search, lọc theo update_time."""
        try:
            if not self.auth.ensure_valid_token():
                raise RuntimeError("Cannot authenticate with TikTok Shop API")

            orders_all: List[Dict[str, Any]] = []
            cursor = ""

            while True:
                params = {
                    "app_key": self.auth.app_key,
                    "timestamp": str(int(time.time())),
                    "shop_cipher": self.auth.shop_cipher,
                    "sign_method": "hmac_sha256",
                    "page_size": page_size,
                    "sort_order": "DESC",
                    "sort_field": "update_time",
                }
                if cursor:
                    params["cursor"] = cursor

                body = {
                    "update_time_ge": start_time,
                    "update_time_lt": end_time,
                }
                body_string = json.dumps(body, separators=(",", ":"), sort_keys=True)
                params["sign"] = self.auth.generate_signature(
                    "/order/202309/orders/search", params, body_string
                )

                headers = {
                    "x-tts-access-token": self.auth.access_token,
                    "Content-Type": "application/json",
                }
                url = f"{self.auth.base_api_url}/order/202309/orders/search"

                resp = requests.post(
                    url, params=params, data=body_string, headers=headers, timeout=30
                )
                resp.raise_for_status()
                data = resp.json()
                if data.get("code") != 0:
                    raise RuntimeError(f"API Error: {data.get('message')}")

                batch = data.get("data", {}).get("orders", []) or []
                if batch:
                    orders_all.extend(batch)

                more = data.get("data", {}).get("more", False)
                cursor = data.get("data", {}).get("next_cursor", "")
                if not (more and cursor):
                    break

                time.sleep(0.4)

            return orders_all
        except Exception as e:
            self.logger.error(f"Error in _search_orders_full_by_update: {e}")
            raise

    def get_order_details_with_ids(self, order_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Get order details theo pattern từ notebook - sử dụng GET request với order_id_list
        """
        try:
            if not order_ids:
                return []

            # Ensure valid authentication
            if not self.auth.ensure_valid_token():
                raise RuntimeError("Cannot authenticate with TikTok Shop API")

            all_orders = []
            batch_size = 50  # API limit cho order details

            self.logger.info(
                f"Processing {len(order_ids)} order IDs in batches of {batch_size}"
            )

            for i in range(0, len(order_ids), batch_size):
                batch_ids = order_ids[i : i + batch_size]
                batch_number = i // batch_size + 1

                self.logger.info(
                    f"Processing batch {batch_number}/{(len(order_ids)-1)//batch_size + 1}: {len(batch_ids)} orders"
                )

                params = {
                    "app_key": self.auth.app_key,
                    "timestamp": str(int(time.time())),
                    "shop_cipher": self.auth.shop_cipher,
                    "sign_method": "hmac_sha256",
                    "ids": ",".join(batch_ids),  # Process ALL batch_ids, no limit
                }

                # Generate signature
                signature = self.auth.generate_signature("/order/202309/orders", params)
                params["sign"] = signature

                headers = {
                    "x-tts-access-token": self.auth.access_token,
                    "Content-Type": "application/json",
                }

                url = f"{self.auth.base_api_url}/order/202309/orders"

                response = requests.get(url, params=params, headers=headers, timeout=30)

                if response.status_code == 200:
                    data = response.json()
                    if data.get("code") == 0:
                        # Debug: Log full response structure
                        self.logger.info(f"Response data keys: {list(data.keys())}")
                        if "data" in data:
                            self.logger.info(
                                f"Data section keys: {list(data['data'].keys())}"
                            )

                        # Try multiple possible keys for orders
                        orders = []
                        data_section = data.get("data", {})
                        if "order_list" in data_section:
                            orders = data_section.get("order_list", [])
                            self.logger.info(f"Found orders in 'order_list' field")
                        elif "orders" in data_section:
                            orders = data_section.get("orders", [])
                            self.logger.info(f"Found orders in 'orders' field")
                        else:
                            self.logger.warning(
                                f"No orders found. Available keys: {list(data_section.keys())}"
                            )

                        all_orders.extend(orders)
                        self.logger.info(
                            f"Retrieved {len(orders)} orders from batch {i//batch_size + 1}"
                        )
                    else:
                        error_message = f"API error: {data.get('message')}"
                        self.logger.error(error_message)
                        raise RuntimeError(error_message)
                else:
                    error_message = (
                        f"HTTP error {response.status_code}: {response.text}"
                    )
                    self.logger.error(error_message)
                    raise RuntimeError(error_message)

                # Rate limiting
                time.sleep(0.5)

            self.logger.info(f"Total orders retrieved: {len(all_orders)}")
            return all_orders

        except Exception as e:
            self.logger.error(f"Error in get_order_details_with_ids: {e}")
            raise RuntimeError(
                "Không thể thực hiện request - có thể do lỗi token hoặc kết nối database"
            )

    # Dọn dẹp: loại bỏ các hàm không được DAGs sử dụng để tránh rối code

    def extract_incremental_orders(
        self, minutes_back: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Extract orders updated in the last N minutes for incremental ETL

        Args:
            minutes_back: Number of minutes to look back (default 10 for standardized incremental window)

        Returns:
            List of order dictionaries from recent updates
        """
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(minutes=minutes_back)

            self.logger.info(
                f"Extracting incremental orders from {start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')}"
            )

            # Lấy theo update_time và trả FULL orders trực tiếp từ search (không GET theo id)
            start_timestamp = int(start_time.timestamp())
            end_timestamp = int(end_time.timestamp())

            orders = self._search_orders_full_by_update(
                start_timestamp, end_timestamp, page_size=50
            )

            self.logger.info(f"Extracted {len(orders)} orders for incremental update")
            return orders

        except Exception as e:
            self.logger.error(f"Error in extract_incremental_orders: {str(e)}")
            raise

    def test_api_connection(self) -> bool:
        """
        Kiểm tra kết nối API và xác thực

        Returns:
            True nếu kết nối thành công, False nếu ngược lại
        """
        try:
            logger.info("Đang kiểm tra kết nối TikTok API...")

            # Kiểm tra xác thực và lấy shop cipher
            if self.auth.ensure_valid_token():
                logger.info("✓ Xác thực thành công")
                logger.info(f"✓ Shop cipher: {self.auth.shop_cipher}")

                # Kiểm tra một API call đơn giản
                test_start = datetime.now() - timedelta(days=1)
                test_end = datetime.now()

                order_ids = self.search_orders_for_ids(
                    int(test_start.timestamp()), int(test_end.timestamp()), page_size=1
                )

                logger.info(
                    f"✓ API call thành công, tìm thấy {len(order_ids)} đơn hàng trong 24h qua"
                )
                return True
            else:
                logger.error("✗ Xác thực thất bại")
                return False

        except Exception as e:
            logger.error(f"✗ Kiểm tra kết nối API thất bại: {str(e)}")
            return False

    def get_fixed_start_date(self) -> datetime:
        """
        Trả về ngày bắt đầu cố định là 1/7/2024

        Returns:
            Datetime cố định 1/7/2024
        """
        fixed_date = datetime(2024, 7, 1)
        logger.info(f"Using fixed start date: {fixed_date.strftime('%Y-%m-%d')}")
        return fixed_date
