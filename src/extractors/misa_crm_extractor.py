#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MISA CRM Data Extractor
Tích hợp với TikTok Shop Infrastructure - Cấu trúc src/
"""

import requests
import jwt
import time
import pyodbc
from datetime import datetime, timedelta, timezone
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


class MISACRMExtractor:
    """
    MISA CRM Data Extractor - Tương tự TikTok Shop Extractor pattern
    """

    def __init__(self):
        """Khởi tạo MISA CRM Extractor"""
        self.credentials = settings.get_data_source_credentials("misa_crm")
        self.client_id = self.credentials.get("client_id")
        self.client_secret = self.credentials.get("client_secret")
        self.base_url = "https://crmconnect.misa.vn"  # URL gốc cố định theo tài liệu
        self.token_url = f"{self.base_url}/api/v2/Account"

        # Add threading lock for token refresh safety
        self._lock = threading.Lock()

        # Initialize tokens - try database first, then fallback to .env
        self._load_persistent_tokens()

        # Endpoint configuration
        self.endpoints = {
            "customers": "/api/v2/Customers",
            "sale_orders": "/api/v2/SaleOrders",
            "contacts": "/api/v2/Contacts",
            "stocks": "/api/v2/Stocks",
            "products": "/api/v2/Products",  # Giả định endpoint này tồn tại
        }

        logger.info(f"Khởi tạo MISA CRM Extractor cho {settings.company_name}")

    def _load_persistent_tokens(self):
        """Load tokens from database similar to TikTok implementation"""
        try:
            with pyodbc.connect(settings.pyodbc_connection_string) as conn:
                cursor = conn.cursor()
                query = """
                    SELECT access_token, expires_at
                    FROM etl_control.api_token_storage
                    WHERE platform = 'misa_crm'
                """
                cursor.execute(query)
                row = cursor.fetchone()

                if row:
                    logger.info("Loaded MISA CRM tokens from database.")
                    self.access_token = row.access_token
                    # Chuẩn hóa expires_at từ DB:
                    # - DB lưu DATETIME2-naive theo giờ Việt Nam (+07)
                    # - Để so sánh hợp lệ, chuyển về UTC-aware
                    if row.expires_at:
                        exp = row.expires_at
                        if isinstance(exp, datetime):
                            if exp.tzinfo is None:
                                # Gắn +07 rồi chuyển về UTC-aware
                                exp = exp.replace(tzinfo=timezone(timedelta(hours=7))).astimezone(
                                    timezone.utc
                                )
                            else:
                                exp = exp.astimezone(timezone.utc)
                            self.token_expires_at = exp
                        else:
                            self.token_expires_at = None
                    else:
                        self.token_expires_at = None
                    return

        except pyodbc.Error as e:
            logger.error(f"Database error loading MISA CRM tokens: {e}")
            pass
        except Exception as e:
            logger.error(f"Unexpected error loading MISA CRM tokens: {e}")
            pass

        # Fallback to .env file
        logger.warning("No MISA CRM token found in database. Using .env configuration.")
        self.access_token = settings.misa_crm_access_token
        self.token_expires_at = (
            self._decode_token_expiry(self.access_token) if self.access_token else None
        )

    def _save_tokens_to_db(self):
        """Save MISA CRM tokens to database like TikTok does"""
        try:
            with pyodbc.connect(settings.pyodbc_connection_string) as conn:
                cursor = conn.cursor()

                merge_sql = """
                    MERGE etl_control.api_token_storage AS target
                    USING (SELECT ? AS platform) AS source
                    ON (target.platform = source.platform)
                    WHEN MATCHED THEN
                        UPDATE SET
                            access_token = ?,
                            expires_at = ?,
                            last_updated = DATEADD(HOUR, 7, GETUTCDATE())
                    WHEN NOT MATCHED THEN
                        INSERT (platform, access_token, refresh_token, expires_at)
                        VALUES (?, ?, ?, ?);
                """

                # Lưu expires_at về DB theo DATETIME2-naive +07 để đồng bộ với các nền tảng
                expires_for_db = self.token_expires_at
                if isinstance(expires_for_db, datetime):
                    if expires_for_db.tzinfo is not None:
                        expires_for_db = (
                            expires_for_db.astimezone(timezone(timedelta(hours=7)))
                            .replace(tzinfo=None)
                        )

                params = (
                    "misa_crm",  # For USING clause
                    self.access_token,
                    expires_for_db,
                    "misa_crm",  # For INSERT clause
                    self.access_token,
                    "",  # MISA doesn't use refresh_token
                    expires_for_db,
                )

                cursor.execute(merge_sql, params)
                conn.commit()
                logger.info("Successfully persisted MISA CRM tokens to database.")

        except Exception as e:
            logger.error(f"Failed to persist MISA CRM tokens to database: {e}")

    def _decode_token_expiry(self, token: str) -> Optional[datetime]:
        """Decode JWT token để lấy thời gian hết hạn"""
        try:
            decoded = jwt.decode(token, options={"verify_signature": False})
            exp_timestamp = decoded.get("exp")

            if exp_timestamp:
                return datetime.fromtimestamp(exp_timestamp, tz=timezone.utc)
            else:
                logger.warning(
                    "Token không có thời gian hết hạn, sử dụng mặc định 1 giờ"
                )
                return datetime.now(timezone.utc) + timedelta(hours=1)

        except Exception as e:
            logger.warning(f"Không thể decode token: {e}, sử dụng mặc định 1 giờ")
            return datetime.now(timezone.utc) + timedelta(hours=1)

    def _is_token_expired(self) -> bool:
        """Kiểm tra token có hết hạn không"""
        if not self.access_token or not self.token_expires_at:
            return True

        # Chuẩn hóa thời điểm hết hạn về UTC-aware trước khi so sánh
        expiry = self.token_expires_at
        if isinstance(expiry, datetime):
            if expiry.tzinfo is None:
                # Giả định giá trị naive đến từ DB là +07-naive
                expiry = expiry.replace(tzinfo=timezone(timedelta(hours=7))).astimezone(
                    timezone.utc
                )
            else:
                expiry = expiry.astimezone(timezone.utc)
        else:
            # Nếu không phải datetime hợp lệ, coi như đã hết hạn
            return True

        buffer_time = datetime.now(timezone.utc) + timedelta(
            seconds=settings.misa_crm_token_refresh_buffer
        )
        return buffer_time >= expiry

    def get_access_token(self, force_refresh: bool = False) -> Optional[str]:
        """Lấy access token, tự động làm mới nếu cần, tuân thủ tài liệu API v2."""
        with self._lock:  # Thread safety
            if not force_refresh and self.access_token and not self._is_token_expired():
                logger.debug("Sử dụng token hiện tại vẫn còn hạn.")
                return self.access_token

            if not self.client_id or not self.client_secret:
                logger.error(
                    "MISA CRM client_id hoặc client_secret chưa được cấu hình."
                )
                return None

            logger.info("Đang yêu cầu access token mới từ MISA CRM API.")

            request_body = {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }
            headers = {"Content-Type": "application/json"}

            try:
                response = requests.post(
                    self.token_url,
                    json=request_body,
                    headers=headers,
                    timeout=settings.api_timeout,
                )
                response.raise_for_status()  # Ném lỗi cho các HTTP status code 4xx/5xx

                result = response.json()
                if result.get("success") and "data" in result and result["data"]:
                    self.access_token = result["data"]
                    self.token_expires_at = self._decode_token_expiry(self.access_token)
                    logger.info(
                        f"Lấy token MISA CRM thành công, hết hạn lúc: {self.token_expires_at}"
                    )

                    # Save to database for persistence
                    self._save_tokens_to_db()

                    return self.access_token
                else:
                    error_msg = result.get("error_message", str(result))
                    logger.error(
                        f"Lấy token MISA CRM thất bại. Phản hồi từ API: {error_msg}"
                    )
                    return None

            except requests.exceptions.RequestException as e:
                logger.error(f"Lỗi kết nối khi yêu cầu token MISA CRM: {e}")
                return None
            except Exception as e:
                logger.error(f"Lỗi không xác định khi yêu cầu token MISA CRM: {e}")
                return None

    def ensure_valid_token(self) -> bool:
        """
        Ensure we have a valid access token, refresh if necessary.
        Similar to TikTok authentication pattern.

        Returns:
            True if we have a valid token, False otherwise
        """
        # Check if token is expired or will expire soon
        if self._is_token_expired():
            logger.info(
                "MISA CRM access token is expired or will expire soon, refreshing..."
            )
            if not self.get_access_token(force_refresh=True):
                logger.error("Failed to refresh MISA CRM access token")
                return False

        # Verify token is available
        if not self.access_token:
            logger.error("MISA CRM access token not available")
            return False

        return True

    def _get_headers(self) -> Optional[Dict[str, str]]:
        """Tạo headers cho API request, bao gồm cả việc lấy token."""
        token = self.get_access_token()
        if not token:
            logger.error("Không thể tạo headers vì không có access token.")
            return None

        if not self.client_id:
            logger.error(
                "Không thể tạo headers vì MISA_CRM_CLIENT_ID chưa được cấu hình."
            )
            return None

        return {
            "Authorization": f"Bearer {token}",
            "Clientid": self.client_id,
            "Content-Type": "application/json",
        }

    def _make_request_with_retry(
        self, method: str, url: str, **kwargs
    ) -> Optional[requests.Response]:
        """Thực hiện request với logic retry và tự động làm mới token khi gặp lỗi 401."""
        for attempt in range(settings.api_retry_attempts):
            try:
                headers = self._get_headers()
                if not headers:
                    logger.error("Bỏ qua request vì không thể tạo headers.")
                    return None

                kwargs["headers"] = headers
                kwargs["timeout"] = kwargs.get("timeout", settings.api_timeout)

                response = requests.request(method, url, **kwargs)

                # Nếu thành công, trả về ngay lập tức
                if response.status_code == 200:
                    return response

                # Nếu token hết hạn (401), thử làm mới và gọi lại
                if (
                    response.status_code == 401
                    and attempt < settings.api_retry_attempts - 1
                ):
                    logger.warning(
                        f"Lần thử {attempt + 1}: Token hết hạn (401). Đang làm mới và thử lại..."
                    )
                    self.get_access_token(force_refresh=True)  # Buộc làm mới token
                    time.sleep(1)  # Chờ 1 giây trước khi thử lại
                    continue  # Chuyển sang lần thử tiếp theo

                # Nếu là lỗi khác hoặc lần thử cuối cùng, báo lỗi và thoát
                logger.error(
                    f"Lần thử {attempt + 1}: Yêu cầu thất bại với mã trạng thái {response.status_code}. Nội dung: {response.text}"
                )
                return None

            except requests.exceptions.RequestException as e:
                logger.error(f"Lần thử {attempt + 1}: Lỗi kết nối - {e}")
                if attempt >= settings.api_retry_attempts - 1:
                    return None  # Trả về None sau lần thử cuối
                time.sleep(settings.api_retry_delay * (attempt + 1))
            except Exception as e:
                logger.error(f"Lần thử {attempt + 1}: Lỗi không xác định - {e}")
                return None  # Lỗi không mong muốn, thoát ngay

        return None

    def extract_endpoint_data(
        self, endpoint_name: str, page: int = 0, page_size: int = None, **params
    ) -> Optional[Dict]:
        """
        Lấy dữ liệu từ MISA CRM endpoint

        Args:
            endpoint_name: Tên endpoint ('customers', 'sale_orders', etc.)
            page: Số trang (bắt đầu từ 0)
            page_size: Số bản ghi trên trang
            **params: Tham số bổ sung

        Returns:
            Dữ liệu response hoặc None nếu thất bại
        """
        if endpoint_name not in self.endpoints:
            logger.error(f"Endpoint không hợp lệ: {endpoint_name}")
            return None

        if page_size is None:
            page_size = settings.misa_crm_page_size

        url = f"{self.base_url}{self.endpoints[endpoint_name]}"

        # Setup parameters
        request_params = {
            "page": page,
            "pageSize": min(page_size, settings.misa_crm_max_page_size),
        }
        request_params.update(params)

        # Xử lý đặc biệt cho stocks (không có phân trang)
        if endpoint_name == "stocks":
            request_params = {}

        logger.debug(
            f"Đang yêu cầu dữ liệu {endpoint_name} (trang {page}, kích thước {page_size})"
        )

        response = self._make_request_with_retry("GET", url, params=request_params)

        if response:
            result = response.json()
            logger.debug(f"Lấy dữ liệu {endpoint_name} thành công")
            return result
        else:
            logger.error(f"Thất bại khi lấy dữ liệu {endpoint_name}")
            return None

    def extract_all_data_from_endpoint(
        self, endpoint_name: str, max_pages: int = 50000
    ) -> List[Dict]:
        """
        Lấy tất cả dữ liệu từ một endpoint, xử lý phân trang tự động.

        Args:
            endpoint_name: Tên của endpoint (ví dụ: 'customers').
            max_pages: Giới hạn số trang tối đa để tránh vòng lặp vô hạn, mặc định là 1000.

        Returns:
            Một danh sách chứa tất cả các bản ghi từ endpoint.
        """
        all_records = []
        page_index = 0
        page_size = 100  # Sử dụng page size tối đa được phép

        logger.info(
            f"Bắt đầu trích xuất toàn bộ dữ liệu từ endpoint: '{endpoint_name}'"
        )

        # Xử lý trường hợp đặc biệt cho 'stocks' không có phân trang
        if endpoint_name == "stocks":
            result = self.extract_endpoint_data(endpoint_name)
            if (
                result
                and result.get("success")
                and isinstance(result.get("data"), list)
            ):
                all_records = result["data"]
                logger.info(
                    f"Trích xuất thành công {len(all_records)} bản ghi từ endpoint 'stocks'."
                )
            else:
                logger.error(
                    f"Trích xuất từ endpoint 'stocks' thất bại hoặc không có dữ liệu."
                )
            return all_records

        # Vòng lặp xử lý phân trang cho các endpoint khác
        while page_index < max_pages:
            logger.info(
                f"Đang lấy dữ liệu từ trang {page_index} của endpoint '{endpoint_name}'..."
            )
            result = self.extract_endpoint_data(
                endpoint_name, page=page_index, page_size=page_size
            )

            if (
                not result
                or not result.get("success")
                or not isinstance(result.get("data"), list)
            ):
                logger.error(
                    f"Yêu cầu đến trang {page_index} của '{endpoint_name}' thất bại hoặc không có dữ liệu."
                )
                break

            batch_data = result["data"]
            if not batch_data:
                logger.info(
                    f"Không còn dữ liệu ở trang {page_index}. Kết thúc trích xuất cho '{endpoint_name}'."
                )
                break

            all_records.extend(batch_data)
            logger.info(
                f"Đã lấy {len(batch_data)} bản ghi từ trang {page_index}. Tổng số bản ghi hiện tại: {len(all_records)}."
            )

            # Nếu số lượng bản ghi trả về ít hơn page_size, đây là trang cuối cùng
            if len(batch_data) < page_size:
                logger.info(
                    f"Đã đến trang cuối cùng. Kết thúc trích xuất cho '{endpoint_name}'."
                )
                break

            page_index += 1
            time.sleep(
                settings.api_retry_delay / 10
            )  # Thêm một khoảng nghỉ nhỏ giữa các request

        if page_index >= max_pages:
            logger.warning(
                f"Đã đạt giới hạn {max_pages} trang. Dừng trích xuất cho '{endpoint_name}'."
            )

        logger.info(
            f"Hoàn thành trích xuất cho '{endpoint_name}'. Tổng cộng {len(all_records)} bản ghi."
        )
        return all_records

    def extract_incremental_data(
        self,
        endpoint_name: str,
        lookback_hours: int = None,
        modified_since: datetime = None,
    ) -> List[Dict]:
        """
        Lấy dữ liệu incremental (chỉ dữ liệu mới/thay đổi)

        Args:
            endpoint_name: Tên endpoint
            lookback_hours: Số giờ nhìn lại (None = sử dụng config mặc định)
            modified_since: Mốc thời gian (datetime) để lọc bản ghi có modified_date >= mốc này. Ưu tiên hơn lookback_hours nếu được cung cấp.

        Returns:
            Danh sách records đã thay đổi
        """
        # Xác định cutoff_time: ưu tiên modified_since nếu được truyền vào; nếu không, suy ra từ lookback_hours
        if modified_since is not None:
            cutoff_time = modified_since
            logger.info(
                f"Lấy dữ liệu incremental từ {endpoint_name} (cutoff theo modified_since: {cutoff_time.isoformat()})"
            )
        else:
            if lookback_hours is None:
                lookback_hours = settings.misa_crm_incremental_lookback_hours
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
            logger.info(
                f"Lấy dữ liệu incremental từ {endpoint_name} (lookback: {lookback_hours} giờ, cutoff: {cutoff_time.isoformat()})"
            )

        # Chuẩn hóa cutoff_time về UTC-aware để so sánh an toàn
        if isinstance(cutoff_time, datetime):
            if cutoff_time.tzinfo is None:
                cutoff_time = cutoff_time.replace(tzinfo=timezone.utc)
            else:
                cutoff_time = cutoff_time.astimezone(timezone.utc)

        # Lấy dữ liệu với giới hạn trang nhỏ cho incremental
        # Lý do: tránh kéo toàn bộ và treo task khi không có thay đổi lớn
        all_data = self.extract_all_data_from_endpoint(endpoint_name, max_pages=3)

        if not all_data:
            return []

        # Lọc theo mốc thời gian
        filtered_data = []

        # Đối với 4 endpoint (trừ sale_orders): lọc theo modified_date trong 15 phút gần nhất
        if endpoint_name in {"customers", "contacts", "stocks", "products"}:
            for record in all_data:
                ts = record.get("modified_date") or record.get("ModifiedDate")
                if not ts:
                    continue
                dt = self._parse_datetime_with_timezone(ts)
                if dt and dt >= cutoff_time:
                    filtered_data.append(record)
        # Đối với sale_orders: ưu tiên theo thời gian tạo/cập nhật đơn thay vì lấy hết
        elif endpoint_name == "sale_orders":
            time_fields_priority = [
                "updated_date",
                "modified_date",
                "order_updated_date",
                "order_date",
                "created_date",
            ]
            for record in all_data:
                dt = None
                for f in time_fields_priority:
                    ts = record.get(f) or record.get(f.capitalize())
                    if ts:
                        dt = self._parse_datetime_with_timezone(ts)
                        if dt:
                            break
                if dt and dt >= cutoff_time:
                    filtered_data.append(record)
        else:
            # Mặc định: giữ nguyên
            filtered_data = all_data

        logger.info(f"Lọc incremental: {len(filtered_data)}/{len(all_data)} records")
        return filtered_data

    def _parse_datetime_with_timezone(self, ts: str) -> Optional[datetime]:
        """
        Parse datetime string và chuyển về UTC để so sánh
        """
        try:
            dt = datetime.fromisoformat(str(ts))
            if dt.tzinfo is None:
                # Nếu không có timezone, coi như UTC
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                # Nếu có timezone, chuyển về UTC để so sánh
                dt = dt.astimezone(timezone.utc)
            return dt
        except Exception:
            return None

    def health_check(self) -> Dict[str, Any]:
        """
        Thực hiện health check trên MISA CRM API

        Returns:
            Kết quả health check
        """
        health_status = {
            "timestamp": datetime.now().isoformat(),
            "token_status": "unknown",
            "endpoints_status": {},
            "overall_status": "unknown",
        }

        try:
            # Kiểm tra token
            token = self.get_access_token()
            if token:
                health_status["token_status"] = "healthy"
                health_status["token_expires_at"] = (
                    self.token_expires_at.isoformat() if self.token_expires_at else None
                )
            else:
                health_status["token_status"] = "unhealthy"
                health_status["overall_status"] = "unhealthy"
                return health_status

            # Kiểm tra từng endpoint
            for endpoint_name in self.endpoints.keys():
                try:
                    result = self.extract_endpoint_data(
                        endpoint_name, page=0, page_size=1
                    )
                    if result and "data" in result:
                        health_status["endpoints_status"][endpoint_name] = "healthy"
                    else:
                        health_status["endpoints_status"][endpoint_name] = "unhealthy"
                except Exception as e:
                    health_status["endpoints_status"][
                        endpoint_name
                    ] = f"error: {str(e)}"

            # Trạng thái tổng thể
            unhealthy_endpoints = [
                k
                for k, v in health_status["endpoints_status"].items()
                if v != "healthy"
            ]
            if not unhealthy_endpoints:
                health_status["overall_status"] = "healthy"
            else:
                health_status["overall_status"] = f"degraded: {unhealthy_endpoints}"

        except Exception as e:
            health_status["overall_status"] = f"error: {str(e)}"

        logger.info(f"Health check hoàn thành: {health_status['overall_status']}")
        return health_status
