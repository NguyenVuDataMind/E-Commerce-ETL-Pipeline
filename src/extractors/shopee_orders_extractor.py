#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shopee Orders Data Extractor
Tích hợp với Facolos Enterprise ETL Infrastructure
"""

import requests
import hmac
import hashlib
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
import sys
import os
import threading

# Import shared utilities
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
from config.settings import settings
from src.utils.logging import setup_logging

logger = setup_logging(__name__)


class ShopeeOrderExtractor:
    """
    Shopee Orders Data Extractor - Tương tự TikTok Shop và MISA CRM pattern
    """

    def __init__(self):
        """Khởi tạo Shopee Order Extractor"""
        self.credentials = settings.get_data_source_credentials("shopee")
        self.partner_id = self.credentials.get("partner_id")
        self.partner_key = self.credentials.get("partner_key")
        self.shop_id = self.credentials.get("shop_id")
        self.redirect_uri = self.credentials.get("redirect_uri")

        # API Configuration
        self.base_url = "https://partner.shopeemobile.com"
        self.api_timeout = settings.api_timeout
        self.api_retry_attempts = settings.api_retry_attempts
        self.api_retry_delay = settings.api_retry_delay

        # Add threading lock for token refresh safety
        self._lock = threading.Lock()

        # Token management
        self.access_token = None
        self.refresh_token = None
        self.token_expires_at = None

        logger.info(f"Khởi tạo Shopee Order Extractor cho {settings.company_name}")

        # Ưu tiên đọc từ DB, nếu chưa có thì fallback .env cho lần đầu
        self._load_persistent_tokens()

    def _load_tokens_from_credentials(self):
        """Load tokens từ credentials"""
        self.access_token = self.credentials.get("access_token")
        self.refresh_token = self.credentials.get("refresh_token")

        if self.access_token:
            logger.info("✅ Loaded access token from credentials")
        else:
            logger.warning("⚠️ No access token found in credentials")

    def _load_persistent_tokens(self):
        """Ưu tiên đọc token Shopee từ DB; nếu không có thì dùng `.env` cho lần đầu.

        - Nếu tìm thấy trong DB: set `access_token`, `refresh_token`, `token_expires_at` theo bản ghi.
        - Nếu không có trong DB: dùng `.env` (settings.shopee_access_token, settings.shopee_refresh_token`).
        """
        try:
            import pyodbc

            with pyodbc.connect(settings.pyodbc_connection_string) as conn:
                cursor = conn.cursor()
                query = """
                    SELECT access_token, refresh_token, expires_at
                    FROM etl_control.api_token_storage
                    WHERE platform = 'shopee'
                    """
                cursor.execute(query)
                row = cursor.fetchone()

                if row:
                    logger.info("✅ Loaded Shopee tokens from database")
                    self.access_token = getattr(row, "access_token", None)
                    self.refresh_token = getattr(row, "refresh_token", None)
                    self.token_expires_at = getattr(row, "expires_at", None)
                    return
        except Exception as e:
            logger.error(f"❌ Lỗi khi đọc Shopee tokens từ DB: {e}")
            # Tiếp tục fallback .env

        # Fallback: .env (lần đầu)
        self._load_tokens_from_credentials()
        if self.access_token:
            logger.warning(
                "⚠️ No Shopee token found in DB. Using .env temporarily for first run."
            )
        else:
            logger.warning(
                "⚠️ Chưa có Shopee access_token trong DB và .env. Hãy thêm SHOPEE_ACCESS_TOKEN/SHOPEE_REFRESH_TOKEN vào .env cho lần đầu."
            )

    def create_signature(
        self, path: str, timestamp: int, access_token: str = None, shop_id: str = None
    ) -> str:
        """
        Tạo chữ ký cho API request theo chuẩn Shopee

        Args:
            path: API path
            timestamp: Unix timestamp
            access_token: Access token (optional)
            shop_id: Shop ID (optional)

        Returns:
            HMAC-SHA256 signature
        """
        if access_token and shop_id:
            base_string = f"{self.partner_id}{path}{timestamp}{access_token}{shop_id}"
        else:
            base_string = f"{self.partner_id}{path}{timestamp}"

        signature = hmac.new(
            self.partner_key.encode("utf-8"),
            base_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        return signature

    def get_access_token_from_code(self, authorization_code: str) -> Dict[str, Any]:
        """
        Lấy access token từ authorization code

        Args:
            authorization_code: Authorization code từ Shopee OAuth

        Returns:
            Dictionary chứa access_token, refresh_token, expire_in
        """
        path = "/api/v2/auth/token/get"
        timestamp = int(time.time())
        sign = self.create_signature(path, timestamp)

        url = f"{self.base_url}{path}"

        # Query parameters (phải nằm trong URL)
        query_params = {
            "partner_id": int(self.partner_id),
            "timestamp": timestamp,
            "sign": sign,
        }

        # Request body (JSON)
        request_body = {
            "shop_id": int(self.shop_id),
            "code": authorization_code,
            "partner_id": int(self.partner_id),
        }

        try:
            response = requests.post(
                url,
                params=query_params,
                headers={"Content-Type": "application/json"},
                data=json.dumps(request_body),
                timeout=self.api_timeout,
            )

            response_json = response.json()

            if response.status_code == 200 and not response_json.get("error"):
                self.access_token = response_json.get("access_token")
                self.refresh_token = response_json.get("refresh_token")
                self.token_expires_at = datetime.now() + timedelta(
                    seconds=response_json.get("expire_in", 14400)
                )

                logger.info("✅ Successfully obtained access token")
                return response_json
            else:
                logger.error(
                    f"❌ Failed to get access token: {response_json.get('error')} - {response_json.get('message')}"
                )
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Request failed: {e}")
            return None

    def refresh_access_token(self) -> bool:
        """
        Làm mới access token bằng refresh token

        Returns:
            True nếu thành công, False nếu thất bại
        """
        if not self.refresh_token:
            logger.error("❌ No refresh token available")
            return False

        path = "/api/v2/auth/access_token/get"
        timestamp = int(time.time())
        sign = self.create_signature(path, timestamp)

        url = f"{self.base_url}{path}"

        # Query parameters
        query_params = {
            "partner_id": int(self.partner_id),
            "timestamp": timestamp,
            "sign": sign,
        }

        # Request body
        request_body = {
            "shop_id": int(self.shop_id),
            "refresh_token": self.refresh_token,
            "partner_id": int(self.partner_id),
        }

        try:
            response = requests.post(
                url,
                params=query_params,
                headers={"Content-Type": "application/json"},
                data=json.dumps(request_body),
                timeout=self.api_timeout,
            )

            response_json = response.json()

            if response.status_code == 200 and not response_json.get("error"):
                self.access_token = response_json.get("access_token")
                self.refresh_token = response_json.get("refresh_token")
                self.token_expires_at = datetime.now() + timedelta(
                    seconds=response_json.get("expire_in", 14400)
                )

                # Lưu token vào database
                self._save_tokens_to_db()

                logger.info("✅ Successfully refreshed access token")
                return True
            else:
                logger.error(
                    f"❌ Failed to refresh token: {response_json.get('error')} - {response_json.get('message')}"
                )
                return False

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Token refresh request failed: {e}")
            return False

    def _is_token_expired(self) -> bool:
        """
        Kiểm tra xem access token có hết hạn không

        Returns:
            True nếu token hết hạn hoặc không có, False nếu còn hạn
        """
        if not self.access_token:
            return True

        if not self.token_expires_at:
            return True

        # Thêm buffer 5 phút trước khi hết hạn để refresh sớm
        buffer_time = timedelta(minutes=5)
        return datetime.now() >= (self.token_expires_at - buffer_time)

    def _ensure_valid_token(self) -> bool:
        """
        Đảm bảo có access token hợp lệ

        Returns:
            True nếu có token hợp lệ, False nếu không thể lấy token
        """
        with self._lock:
            # Kiểm tra nếu token còn hạn
            if not self._is_token_expired():
                return True

            # Nếu không có token hoặc hết hạn, thử refresh
            if self.refresh_token:
                logger.info("🔄 Token expired or missing, attempting refresh...")
                return self.refresh_access_token()

            logger.error("❌ No valid token available and no refresh token")
            return False

    def get_order_list(
        self,
        time_from: int,
        time_to: int,
        page_size: int = 100,
        time_range_field: str = "create_time",
    ) -> Optional[Dict[str, Any]]:
        """
        Lấy danh sách đơn hàng từ Shopee API

        Args:
            time_from: Unix timestamp bắt đầu
            time_to: Unix timestamp kết thúc
            page_size: Số lượng đơn hàng mỗi trang
            time_range_field: Trường thời gian để filter

        Returns:
            Dictionary chứa response từ API hoặc None nếu lỗi
        """
        if not self._ensure_valid_token():
            logger.error("❌ Cannot get order list: no valid token")
            return None

        path = "/api/v2/order/get_order_list"
        timestamp = int(time.time())
        sign = self.create_signature(path, timestamp, self.access_token, self.shop_id)

        url = f"{self.base_url}{path}"

        # Query parameters
        params = {
            "partner_id": int(self.partner_id),
            "timestamp": timestamp,
            "sign": sign,
            "access_token": self.access_token,
            "shop_id": int(self.shop_id),
            "time_range_field": time_range_field,
            "time_from": int(time_from),
            "time_to": int(time_to),
            "page_size": int(page_size),
        }

        try:
            # DEBUG: Log request details
            logger.debug(f"🔍 DEBUG: Getting order list from {time_from} to {time_to}")
            logger.debug(
                f"🔍 DEBUG: Page size: {page_size}, Time field: {time_range_field}"
            )
            logger.debug(f"🔍 DEBUG: URL: {url}")

            response = requests.get(url, params=params, timeout=self.api_timeout)
            data = response.json()

            # DEBUG: Log response details
            logger.debug(f"🔍 DEBUG: Response status: {response.status_code}")
            logger.debug(
                f"🔍 DEBUG: Response data keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}"
            )

            if response.status_code != 200 or data.get("error"):
                logger.error(
                    f"❌ Error getting order list: {data.get('error')} - {data.get('message')}"
                )
                logger.error(f"🔍 DEBUG: Full error response: {data}")
                return None

            # DEBUG: Check response structure
            response_data = data.get("response", {})
            order_list = response_data.get("order_list", [])

            logger.debug(
                f"🔍 DEBUG: Response structure - response: {bool(response_data)}, order_list: {len(order_list)}"
            )

            if not order_list:
                logger.warning(f"⚠️ WARNING: No order_list in response")
                logger.warning(f"🔍 DEBUG: Full response: {data}")
                return data  # Return empty response instead of None

            logger.info(f"✅ Retrieved {len(order_list)} orders")

            # DEBUG: Log order IDs for verification
            if order_list:
                order_ids = [order.get("order_sn", "NO_SN") for order in order_list[:3]]
                logger.debug(f"🔍 DEBUG: First 3 order IDs: {order_ids}")

            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Request failed: {e}")
            logger.error(f"🔍 DEBUG: Request URL: {url}")
            logger.error(f"🔍 DEBUG: Request params: {params}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON decode failed: {e}")
            logger.error(f"🔍 DEBUG: Response text: {response.text[:500]}...")
            return None
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
            logger.error(
                f"🔍 DEBUG: Request details - URL: {url}, Time range: {time_from}-{time_to}"
            )
            return None

    def get_order_detail(self, order_sn_list: List[str]) -> Optional[Dict[str, Any]]:
        """
        Lấy chi tiết đơn hàng từ Shopee API

        Args:
            order_sn_list: Danh sách order_sn (tối đa 50)

        Returns:
            Dictionary chứa response từ API hoặc None nếu lỗi
        """
        if not self._ensure_valid_token():
            logger.error("❌ Cannot get order detail: no valid token")
            return None

        # Batch processing để không mất dữ liệu
        if len(order_sn_list) <= 50:
            # Xử lý trực tiếp nếu ≤ 50 orders
            return self._get_order_detail_batch(order_sn_list)

        # Chia thành batches 50 orders để xử lý tất cả
        logger.info(f"📦 Splitting {len(order_sn_list)} orders into batches of 50")
        batches = [order_sn_list[i : i + 50] for i in range(0, len(order_sn_list), 50)]
        all_orders = []

        for batch_idx, batch in enumerate(batches):
            logger.info(
                f"📦 Processing batch {batch_idx + 1}/{len(batches)}: {len(batch)} orders"
            )
            batch_result = self._get_order_detail_batch(batch)
            if batch_result:
                all_orders.extend(
                    batch_result.get("response", {}).get("order_list", [])
                )
            time.sleep(0.5)  # Rate limiting giữa các batch

        logger.info(
            f"✅ Processed all {len(all_orders)} orders in {len(batches)} batches"
        )
        return {"response": {"order_list": all_orders}}

    def _get_order_detail_batch(
        self, order_sn_list: List[str]
    ) -> Optional[Dict[str, Any]]:
        """
        Gọi API get_order_detail cho một batch orders (≤ 50)

        Args:
            order_sn_list: Danh sách order_sn (tối đa 50)

        Returns:
            Response từ API hoặc None nếu lỗi
        """
        path = "/api/v2/order/get_order_detail"
        timestamp = int(time.time())
        sign = self.create_signature(path, timestamp, self.access_token, self.shop_id)

        url = f"{self.base_url}{path}"

        # Query parameters
        params = {
            "partner_id": int(self.partner_id),
            "timestamp": timestamp,
            "sign": sign,
            "access_token": self.access_token,
            "shop_id": int(self.shop_id),
            "order_sn_list": ",".join(order_sn_list),
        }

        # Request optional fields để lấy đầy đủ thông tin
        params["response_optional_fields"] = (
            "buyer_user_id,buyer_username,estimated_shipping_fee,recipient_address,actual_shipping_fee,"
            "goods_to_declare,note,note_update_time,item_list,pay_time,dropshipper,dropshipper_phone,split_up,"
            "buyer_cancel_reason,cancel_by,cancel_reason,actual_shipping_fee_confirmed,buyer_cpf_id,fulfillment_flag,"
            "pickup_done_time,package_list,shipping_carrier,payment_method,total_amount,invoice_data,"
            "order_chargeable_weight_gram,return_request_due_date,edt,payment_info"
        )

        try:
            # DEBUG: Log request details
            logger.debug(f"🔍 DEBUG: Requesting {len(order_sn_list)} orders")
            logger.debug(f"🔍 DEBUG: First 3 order IDs: {order_sn_list[:3]}")
            logger.debug(f"🔍 DEBUG: URL: {url}")

            response = requests.get(url, params=params, timeout=self.api_timeout)
            data = response.json()

            # DEBUG: Log response details
            logger.debug(f"🔍 DEBUG: Response status: {response.status_code}")
            logger.debug(
                f"🔍 DEBUG: Response data keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}"
            )

            if response.status_code != 200 or data.get("error"):
                logger.error(
                    f"❌ Error getting order detail: {data.get('error')} - {data.get('message')}"
                )
                logger.error(f"🔍 DEBUG: Full error response: {data}")
                return None

            # DEBUG: Check response structure
            response_data = data.get("response", {})
            order_list = response_data.get("order_list", [])

            logger.debug(
                f"🔍 DEBUG: Response structure - response: {bool(response_data)}, order_list: {len(order_list)}"
            )

            if not order_list:
                logger.warning(
                    f"⚠️ WARNING: No order_list in response for {len(order_sn_list)} orders"
                )
                logger.warning(f"🔍 DEBUG: Full response: {data}")
                # Check if there's an error in response
                if "error" in response_data:
                    logger.error(f"❌ Error in response: {response_data['error']}")
                return None

            logger.info(f"✅ Retrieved details for {len(order_list)} orders")

            # DEBUG: Log success details
            if len(order_list) != len(order_sn_list):
                logger.warning(
                    f"⚠️ WARNING: Requested {len(order_sn_list)} orders but got {len(order_list)} details"
                )
                logger.warning(
                    f"🔍 DEBUG: Missing orders - requested: {len(order_sn_list)}, received: {len(order_list)}"
                )

            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Request failed: {e}")
            logger.error(f"🔍 DEBUG: Request URL: {url}")
            logger.error(f"🔍 DEBUG: Request params: {params}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON decode failed: {e}")
            logger.error(f"🔍 DEBUG: Response text: {response.text[:500]}...")
            return None
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
            logger.error(
                f"🔍 DEBUG: Request details - URL: {url}, Orders: {len(order_sn_list)}"
            )
            return None

    def extract_orders_full_load(
        self, start_date: datetime, end_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Extract tất cả đơn hàng trong khoảng thời gian (Full Load)
        Chia nhỏ thành chunks 15 ngày để tuân thủ Shopee API limit

        Args:
            start_date: Ngày bắt đầu
            end_date: Ngày kết thúc

        Returns:
            List chứa tất cả orders với chi tiết đầy đủ
        """
        logger.info(
            f"🚀 Starting Shopee full load extraction from {start_date} to {end_date}"
        )

        # Shopee API giới hạn 10 ngày cho mỗi query (giảm từ 15 để tránh overload)
        max_days_per_chunk = 10
        all_orders = []

        # Chia khoảng thời gian thành chunks 10 ngày
        current_start = start_date
        chunk_number = 1

        while current_start < end_date:
            # Tính toán end date cho chunk này (không quá 10 ngày)
            chunk_end = min(
                current_start + timedelta(days=max_days_per_chunk), end_date
            )

            logger.info(
                f"📦 Processing chunk {chunk_number}: {current_start.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}"
            )

            try:
                # Extract orders cho chunk này
                chunk_orders = self._extract_orders_chunk(current_start, chunk_end)
                all_orders.extend(chunk_orders)

                logger.info(
                    f"✅ Chunk {chunk_number} completed: {len(chunk_orders)} orders"
                )

            except Exception as e:
                logger.error(f"❌ Error processing chunk {chunk_number}: {e}")
                # Tiếp tục với chunk tiếp theo thay vì fail toàn bộ

            # Chuyển sang chunk tiếp theo
            current_start = chunk_end
            chunk_number += 1

            # Rate limiting giữa các chunks
            time.sleep(1)

        logger.info(f"🎉 Full load extraction completed: {len(all_orders)} orders")
        return all_orders

    def _extract_orders_chunk(
        self, start_date: datetime, end_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Extract orders cho một chunk thời gian (tối đa 10 ngày)

        Args:
            start_date: Ngày bắt đầu chunk
            end_date: Ngày kết thúc chunk

        Returns:
            List orders trong chunk này
        """
        # Đảm bảo range không quá 10 ngày để tránh API error
        days_diff = (end_date - start_date).days
        if days_diff > 10:
            logger.warning(
                f"⚠️ Chunk range too large ({days_diff} days), limiting to 10 days"
            )
            end_date = start_date + timedelta(days=10)

        start_timestamp = int(start_date.timestamp())
        end_timestamp = int(end_date.timestamp())

        chunk_orders = []
        page_size = 100
        offset = 0

        while True:
            # Lấy danh sách order_sn cho chunk này
            order_list_response = self.get_order_list(
                time_from=start_timestamp, time_to=end_timestamp, page_size=page_size
            )

            if not order_list_response:
                logger.error("❌ Failed to get order list for chunk")
                break

            order_list = order_list_response.get("response", {}).get("order_list", [])

            if not order_list:
                logger.info("📭 No more orders found in chunk")
                break

            # Lấy order_sn để gọi get_order_detail
            order_sn_list = [
                order.get("order_sn") for order in order_list if order.get("order_sn")
            ]

            if order_sn_list:
                # Gọi get_order_detail để lấy chi tiết
                detail_response = self.get_order_detail(order_sn_list)

                if detail_response:
                    orders_detail = detail_response.get("response", {}).get(
                        "order_list", []
                    )
                    chunk_orders.extend(orders_detail)
                    logger.info(
                        f"✅ Processed {len(orders_detail)} orders in chunk (Chunk total: {len(chunk_orders)})"
                    )
                else:
                    logger.warning("⚠️ Failed to get order details, skipping batch")

            # Kiểm tra nếu có more data
            if not order_list_response.get("response", {}).get("more", False):
                break

            # Rate limiting
            time.sleep(0.5)

        return chunk_orders

    def extract_orders_incremental(
        self, minutes_back: int = 15
    ) -> List[Dict[str, Any]]:
        """
        Extract đơn hàng incremental (dữ liệu mới trong X phút gần nhất)

        Args:
            minutes_back: Số phút lookback (default 15 phút với buffer 5 phút)

        Returns:
            List chứa orders mới
        """
        logger.info(
            f"🔄 Starting Shopee incremental extraction: {minutes_back} minutes back"
        )

        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=minutes_back)

        start_timestamp = int(start_time.timestamp())
        end_timestamp = int(end_time.timestamp())

        logger.info(f"📅 Incremental range: {start_time} to {end_time}")

        # Lấy danh sách order_sn trong khoảng thời gian
        order_list_response = self.get_order_list(
            time_from=start_timestamp, time_to=end_timestamp, page_size=100
        )

        if not order_list_response:
            logger.warning("⚠️ No order list response for incremental")
            return []

        order_list = order_list_response.get("response", {}).get("order_list", [])

        if not order_list:
            logger.info("📭 No new orders found in incremental window")
            return []

        # Lấy chi tiết cho các orders mới
        order_sn_list = [
            order.get("order_sn") for order in order_list if order.get("order_sn")
        ]

        if not order_sn_list:
            logger.warning("⚠️ No valid order_sn found")
            return []

        # Process in batches of 50 (API limit)
        all_orders = []
        batch_size = 50

        for i in range(0, len(order_sn_list), batch_size):
            batch = order_sn_list[i : i + batch_size]

            detail_response = self.get_order_detail(batch)

            if detail_response:
                orders_detail = detail_response.get("response", {}).get(
                    "order_list", []
                )
                all_orders.extend(orders_detail)
                logger.info(
                    f"✅ Processed batch {i//batch_size + 1}: {len(orders_detail)} orders"
                )
            else:
                logger.warning(f"⚠️ Failed to get details for batch {i//batch_size + 1}")

            # Rate limiting
            time.sleep(0.5)

        logger.info(
            f"🎉 Incremental extraction completed: {len(all_orders)} new orders"
        )
        return all_orders

    def find_earliest_order_date(
        self, max_lookback_years: int = 2
    ) -> Optional[datetime]:
        """
        Tìm ngày đơn hàng sớm nhất có thể (auto-detect start date)

        Args:
            max_lookback_years: Số năm tối đa để lookback

        Returns:
            Datetime của đơn hàng sớm nhất hoặc None nếu không tìm thấy
        """
        logger.info(
            f"🔍 Auto-detecting earliest order date (max {max_lookback_years} years back)"
        )

        end_date = datetime.now()
        start_date = end_date - timedelta(days=max_lookback_years * 365)

        # Binary search để tìm ngày sớm nhất có data
        while (end_date - start_date).days > 1:
            mid_date = start_date + (end_date - start_date) / 2

            start_timestamp = int(start_date.timestamp())
            mid_timestamp = int(mid_date.timestamp())

            # Kiểm tra nửa đầu
            response = self.get_order_list(
                time_from=start_timestamp, time_to=mid_timestamp, page_size=1
            )

            if response and response.get("response", {}).get("order_list"):
                end_date = mid_date
            else:
                start_date = mid_date

        logger.info(f"✅ Earliest order date detected: {start_date}")
        return start_date

    def _save_tokens_to_db(self):
        """Lưu Shopee tokens vào database"""
        try:
            import pyodbc

            with pyodbc.connect(settings.pyodbc_connection_string) as conn:
                cursor = conn.cursor()

                merge_sql = """
                    MERGE etl_control.api_token_storage AS target
                    USING (SELECT ? AS platform) AS source
                    ON (target.platform = source.platform)
                    WHEN MATCHED THEN
                        UPDATE SET
                            access_token = ?,
                            refresh_token = ?,
                            expires_at = ?,
                            last_updated = GETUTCDATE()
                    WHEN NOT MATCHED THEN
                        INSERT (platform, access_token, refresh_token, expires_at)
                        VALUES (?, ?, ?, ?);
                """

                params = (
                    "shopee",  # For USING clause
                    self.access_token,
                    self.refresh_token,
                    self.token_expires_at,
                    "shopee",  # For INSERT clause
                    self.access_token,
                    self.refresh_token,
                    self.token_expires_at,
                )

                cursor.execute(merge_sql, params)
                conn.commit()
                logger.info("✅ Successfully saved Shopee tokens to database")

        except Exception as e:
            logger.error(f"❌ Failed to save Shopee tokens to database: {e}")
            # Không raise exception để không làm gián đoạn ETL process
