"""
Utility functions for TikTok Shop API authentication and signature generation
Based on the working notebook implementation
"""

import hmac
import hashlib
import time
import requests
import logging
import sys
import os
from typing import Dict, Any, Optional
from datetime import datetime
import threading
import pyodbc

# Add project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config.settings import settings

logger = logging.getLogger(__name__)


class TikTokAuthenticator:
    """Handle TikTok Shop API authentication and token management with automatic refresh"""

    def __init__(self):
        self.app_key = settings.tiktok_shop_app_key
        self.app_secret = settings.tiktok_shop_app_secret
        self._lock = threading.Lock()  # Thread safety for token refresh

        # API endpoints
        self.token_refresh_url = "https://auth.tiktok-shops.com/api/v2/token/refresh"
        self.base_api_url = "https://open-api.tiktokglobalshop.com"

        # Initialize tokens from persistent storage first, then fallback to .env
        self._load_persistent_tokens()

    def _load_persistent_tokens(self):
        """Load the latest tokens from the database."""
        try:
            with pyodbc.connect(settings.pyodbc_connection_string) as conn:
                cursor = conn.cursor()
                query = """
                    SELECT access_token, refresh_token, expires_at, shop_cipher
                    FROM etl_control.api_token_storage
                    WHERE platform = 'tiktok_shop'
                """
                cursor.execute(query)
                row = cursor.fetchone()

                if row:
                    logger.info("Loaded TikTok Shop tokens from database.")
                    self.access_token = row.access_token
                    self.refresh_token = row.refresh_token
                    self.access_token_expires_at = (
                        row.expires_at.timestamp() if row.expires_at else None
                    )
                    # Always use shop_cipher from .env (fixed value)
                    self.shop_cipher = settings.tiktok_shop_cipher
                    self.refresh_token_expires_at = (
                        None  # This is no longer stored in the token table
                    )
                    return

        except pyodbc.Error as e:
            logger.error(
                f"CRITICAL: pyodbc error when loading tokens from database: {e}"
            )
            logger.error(
                "This is a fatal error. The application cannot proceed without database access for tokens."
            )
            raise  # Re-raise the exception to stop the process
        except Exception as e:
            logger.error(
                f"An unexpected critical error occurred while loading tokens from database: {e}"
            )
            raise

        # If we reach here, it means DB connection was successful but no token was found.
        # This is a valid state, but we should log it and then proceed to load from .env as a fallback.
        logger.warning(
            "No TikTok Shop token found in the database. Falling back to .env file."
        )
        self.access_token = settings.tiktok_shop_access_token
        self.refresh_token = settings.tiktok_shop_refresh_token
        self.shop_cipher = settings.tiktok_shop_cipher
        self.access_token_expires_at = None
        self.refresh_token_expires_at = None

    def generate_signature(
        self, path: str, params: Dict[str, Any], body: str = ""
    ) -> str:
        """
        Generate HMAC-SHA256 signature based on the official TikTok Shop documentation.
        This version correctly handles query parameters and the request body.
        """
        # Step 1: Extract all query parameters excluding 'sign' and 'access_token'
        filtered_params = {
            k: str(v) for k, v in params.items() if k not in ["sign", "access_token"]
        }

        # Step 2: Sort parameters alphabetically by key
        sorted_keys = sorted(filtered_params.keys())

        # Step 3: Concatenate in {key}{value} format
        param_string = "".join([f"{key}{filtered_params[key]}" for key in sorted_keys])

        # Step 4: Start with request path + sorted params
        sign_string = f"{path}{param_string}"

        # Step 5: Append body if provided (for POST requests)
        if body:
            sign_string += body

        # Step 6: Wrap with app_secret
        wrapped_string = f"{self.app_secret}{sign_string}{self.app_secret}"

        # Step 7: HMAC-SHA256 encode
        signature = hmac.new(
            self.app_secret.encode("utf-8"),
            wrapped_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        logger.debug(f"Generated signature for path {path}: {signature}")
        return signature

    def is_token_expired(self) -> bool:
        """Check if access token is expired or will expire soon (within 5 minutes)"""
        if not self.access_token_expires_at:
            return True

        # Consider token expired if it expires within 5 minutes
        buffer_time = 5 * 60  # 5 minutes in seconds
        return time.time() >= (self.access_token_expires_at - buffer_time)

    def is_refresh_token_expired(self) -> bool:
        """Check if refresh token is expired"""
        if not self.refresh_token_expires_at:
            return False  # Assume valid if we don't know expiry

        return time.time() >= self.refresh_token_expires_at

    def refresh_access_token(self) -> bool:
        """
        Refresh the access token using the refresh token, following the official documentation.
        Enhanced with proper expiry tracking and thread safety.
        """
        with self._lock:  # Thread safety
            logger.info("Attempting to refresh access token...")

            # Check if refresh token is expired
            if self.is_refresh_token_expired():
                logger.error(
                    "Refresh token is expired. Manual re-authentication required."
                )
                return False

            params = {
                "app_key": self.app_key,
                "app_secret": self.app_secret,
                "refresh_token": self.refresh_token,
                "grant_type": "refresh_token",
            }

            try:
                # Per official documentation (202407), token refresh is a GET request
                response = requests.get(
                    self.token_refresh_url, params=params, timeout=30
                )
                response.raise_for_status()

                data = response.json()
                if data.get("code") == 0:
                    token_data = data.get("data", {})

                    # Update tokens
                    self.access_token = token_data.get("access_token")
                    new_refresh_token = token_data.get("refresh_token")
                    if new_refresh_token:
                        self.refresh_token = new_refresh_token

                    # Update expiry times
                    access_expires_in = token_data.get("access_token_expire_in")
                    refresh_expires_in = token_data.get("refresh_token_expire_in")

                    if access_expires_in:
                        self.access_token_expires_at = time.time() + access_expires_in
                    if refresh_expires_in:
                        self.refresh_token_expires_at = time.time() + refresh_expires_in

                    logger.info(
                        f"Successfully refreshed access token. Expires in {access_expires_in} seconds."
                    )

                    # Update settings if possible (for persistence)
                    self._update_persistent_tokens()

                    # After successfully refreshing and persisting, wait a moment for the new token to be active across TikTok's systems
                    logger.info(
                        "Waiting for 2 seconds for the new access token to become active..."
                    )
                    time.sleep(2)

                    return True
                else:
                    logger.error(
                        f"Failed to refresh token. API Error {data.get('code')}: {data.get('message')}"
                    )
                    return False

            except requests.exceptions.RequestException as e:
                logger.error(f"HTTP request failed during token refresh: {e}")
                return False
            except Exception as e:
                logger.error(f"An unexpected error occurred during token refresh: {e}")
                return False

    def _update_persistent_tokens(self):
        """Saves the latest tokens to the database using a MERGE operation (UPSERT)."""
        try:
            with pyodbc.connect(settings.pyodbc_connection_string) as conn:
                cursor = conn.cursor()

                # Convert expiry timestamp to datetime object for SQL Server
                expiry_dt = (
                    datetime.fromtimestamp(self.access_token_expires_at)
                    if self.access_token_expires_at
                    else None
                )

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
                    "tiktok_shop",  # For the USING clause
                    self.access_token,
                    self.refresh_token,
                    expiry_dt,
                    "tiktok_shop",  # For the INSERT clause
                    self.access_token,
                    self.refresh_token,
                    expiry_dt,
                )

                cursor.execute(merge_sql, params)
                conn.commit()
                logger.info(
                    "Successfully persisted new TikTok Shop tokens to the database."
                )

        except Exception as e:
            logger.error(
                f"CRITICAL: Failed to persist tokens to database: {e}. This is a fatal error."
            )
            raise

    def get_shop_cipher(self) -> Optional[str]:
        """
        Get shop cipher for the authenticated shop

        Returns:
            Shop cipher string or None if failed
        """
        try:
            url = "https://open-api.tiktokglobalshop.com/authorization/202309/shops"

            params = {
                "app_key": self.app_key,
                "timestamp": str(int(time.time())),
                "access_token": self.access_token,
                "version": "202309",
            }

            # Generate signature
            signature = self.generate_signature("/authorization/202309/shops", params)
            params["sign"] = signature

            response = requests.get(url, params=params)

            if response.status_code == 200:
                data = response.json()
                if data.get("code") == 0 and data.get("data", {}).get("shops"):
                    shop_cipher = data["data"]["shops"][0]["cipher"]
                    self.shop_cipher = shop_cipher
                    logger.info(f"Shop cipher retrieved: {shop_cipher}")
                    return shop_cipher
                else:
                    logger.error(f"Get shop cipher failed: {data.get('message')}")
                    return None
            else:
                logger.error(
                    f"Get shop cipher HTTP error: {response.status_code} - Response: {response.text}"
                )
                return None

        except Exception as e:
            logger.error(f"Exception getting shop cipher: {str(e)}")
            return None

    def ensure_valid_token(self) -> bool:
        """
        Ensure we have a valid access token, refresh if necessary.
        This method should be called before any API request.

        Returns:
            True if we have a valid token, False otherwise
        """
        # Check if token is expired or will expire soon
        if self.is_token_expired():
            logger.info("Access token is expired or will expire soon, refreshing...")
            if not self.refresh_access_token():
                logger.error("Failed to refresh access token")
                return False

        # Verify token works - shop_cipher is always available from .env
        if not self.shop_cipher:
            logger.error("Shop cipher not available in .env configuration")
            return False

        return True

    def make_authenticated_request(
        self,
        method: str,
        endpoint: str,
        params: Dict[str, Any] = None,
        body: str = "",
        headers: Dict[str, str] = None,
    ) -> Optional[requests.Response]:
        """
        Make an authenticated request to TikTok Shop API with automatic token refresh

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path (e.g., '/order/202309/orders')
            params: Query parameters
            body: Request body (for POST requests)
            headers: Additional headers

        Returns:
            Response object or None if failed
        """
        # Ensure we have a valid token
        if not self.ensure_valid_token():
            error_msg = "Authentication failed (ensure_valid_token returned False). Cannot make request."
            logger.error(error_msg)
            raise ConnectionError(error_msg)

        # Prepare parameters
        if params is None:
            params = {}

        # Add required parameters
        params.update(
            {
                "app_key": self.app_key,
                "timestamp": str(int(time.time())),
                "shop_cipher": self.shop_cipher,
                "sign_method": "hmac_sha256",
            }
        )

        # Generate signature
        signature = self.generate_signature(endpoint, params, body)
        params["sign"] = signature

        # Prepare headers
        if headers is None:
            headers = {}
        headers.update(
            {
                "x-tts-access-token": self.access_token,
                "Content-Type": "application/json",
            }
        )

        # Make request
        url = f"{self.base_api_url}{endpoint}"

        try:
            if method.upper() == "GET":
                response = requests.get(url, params=params, headers=headers, timeout=30)
            elif method.upper() == "POST":
                response = requests.post(
                    url, params=params, data=body, headers=headers, timeout=30
                )
            else:
                error_msg = f"Unsupported HTTP method: {method}"
                logger.error(error_msg)
                raise ValueError(error_msg)

            # Check for token expiry error and retry once
            if response.status_code == 401 or (
                response.status_code == 200
                and response.json().get("code") in [10002, 10003]
            ):  # Token error codes
                logger.warning("Received token error, attempting refresh and retry...")
                if self.refresh_access_token():
                    # Update headers with new token
                    headers["x-tts-access-token"] = self.access_token
                    # Regenerate signature with new timestamp
                    params["timestamp"] = str(int(time.time()))
                    signature = self.generate_signature(endpoint, params, body)
                    params["sign"] = signature

                    # Retry request
                    if method.upper() == "GET":
                        response = requests.get(
                            url, params=params, headers=headers, timeout=30
                        )
                    elif method.upper() == "POST":
                        response = requests.post(
                            url, params=params, data=body, headers=headers, timeout=30
                        )

            return response

        except Exception as e:
            logger.error(f"Request failed: {e}")
            raise ConnectionError(f"Request failed: {e}")
