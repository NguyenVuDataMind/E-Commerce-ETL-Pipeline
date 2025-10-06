#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MISA CRM Data Loader
Tích hợp với TikTok Shop Infrastructure - Cấu trúc src/
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


class MISACRMLoader:
    """
    MISA CRM Data Loader - Tương tự TikTok Shop Loader pattern
    """

    def __init__(self):
        """Khởi tạo MISA CRM Loader"""
        self.db_engine = create_engine(settings.sql_server_connection_string)

        # Table mapping - phải khớp với keys từ transformer
        self.table_mappings = {
            "customers": settings.get_misa_crm_table_full_name("misa_customers"),
            "sale_orders_flattened": settings.get_misa_crm_table_full_name(
                "misa_sale_orders_flattened"
            ),
            "contacts": settings.get_misa_crm_table_full_name("misa_contacts"),
            "stocks": settings.get_misa_crm_table_full_name("misa_stocks"),
            "products": settings.get_misa_crm_table_full_name("misa_products"),
        }

        logger.info(f"Khởi tạo MISA CRM Loader cho {settings.company_name}")
        logger.info(f"Database: {settings.sql_server_host}")

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

    def truncate_table(self, endpoint: str) -> bool:
        """
        Truncate staging table cho endpoint

        Args:
            endpoint: Tên endpoint

        Returns:
            True nếu thành công
        """
        if endpoint not in self.table_mappings:
            logger.error(f"Không tìm thấy table mapping cho endpoint: {endpoint}")
            return False

        table_full_name = self.table_mappings[endpoint]

        try:
            # FIXED: Sử dụng pyodbc connection thay vì SQLAlchemy để tránh lỗi commit
            import pyodbc

            with pyodbc.connect(settings.pyodbc_connection_string) as conn:
                cursor = conn.cursor()
                cursor.execute(f"TRUNCATE TABLE {table_full_name}")
                conn.commit()

            logger.info(f"Truncated table {table_full_name}")
            return True

        except Exception as e:
            logger.error(f"Lỗi khi truncate table {table_full_name}: {e}")
            return False

    def load_dataframe_to_staging(
        self, df: pd.DataFrame, endpoint: str, if_exists: str = "append"
    ) -> bool:
        """
        Load DataFrame vào staging table

        Args:
            df: DataFrame cần load
            endpoint: Tên endpoint
            if_exists: Hành động nếu table đã tồn tại ('append', 'replace', 'fail')

        Returns:
            True nếu thành công
        """
        if df.empty:
            logger.warning(f"DataFrame rỗng cho endpoint {endpoint}")
            return True

        if endpoint not in self.table_mappings:
            logger.error(f"Không tìm thấy table mapping cho endpoint: {endpoint}")
            return False

        table_full_name = self.table_mappings[endpoint]
        table_info = self._get_table_info(table_full_name)

        try:
            # Chuẩn hóa DataFrame theo schema và endpoint trước khi ghi
            df = self._normalize_dataframe_for_endpoint(df, endpoint, table_info)

            # Xử lý trùng khóa cho sale_orders_flattened trước khi ghi để tránh lỗi PK
            if endpoint == "sale_orders_flattened":
                # Bỏ các dòng thiếu khóa
                before_len = len(df)
                df = df.dropna(
                    subset=["order_id", "item_id"]
                )  # item_product_code có thể NULL theo schema mới
                dropped_nulls = before_len - len(df)
                if dropped_nulls > 0:
                    logger.info(
                        f"sale_orders_flattened: bỏ {dropped_nulls} dòng thiếu khóa (order_id/item_id) trước khi load"
                    )

                # Loại toàn bộ bản ghi trùng trên bộ khóa (order_id, item_id)
                dup_mask = df.duplicated(subset=["order_id", "item_id"], keep=False)
                dup_count = int(dup_mask.sum())
                if dup_count > 0:
                    logger.warning(
                        f"sale_orders_flattened: phát hiện {dup_count} dòng trùng khóa trong batch — sẽ loại bỏ toàn bộ các dòng trùng để tránh lỗi PK"
                    )
                    df = df[~dup_mask]

                # Xóa trước các bản ghi trùng trong DB để tránh xung đột khi insert
                try:
                    self._predelete_conflicting_sale_order_items(df, table_info)
                except Exception as predel_err:
                    logger.warning(
                        f"sale_orders_flattened: không thể pre-delete khóa trùng trong DB ({predel_err}), tiếp tục ghi dữ liệu còn lại"
                    )

            # FIXED: Bỏ method="multi" để tránh lỗi parameter markers với SQL Server
            batch_size = min(
                50, settings.misa_crm_etl_batch_size
            )  # Giảm batch size xuống 50

            # Load data using pandas to_sql với batch size nhỏ hơn
            df.to_sql(
                name=table_info["table"],
                con=self.db_engine,
                schema=table_info["schema"],
                if_exists=if_exists,
                index=False,
                # method="multi",  # FIXED: Bỏ method="multi" vì không tương thích với SQL Server
                chunksize=batch_size,  # FIXED: Batch size nhỏ hơn
            )

            logger.info(f"Loaded {len(df)} records to {table_full_name}")
            return True

        except Exception as e:
            logger.error(f"Lỗi khi load data vào {table_full_name}: {e}")
            # Try alternative loading method for all tables (SQLAlchemy engine issue)
            logger.info(f"Trying alternative pyodbc loading method for {endpoint}...")
            return self._load_with_pyodbc(df, table_full_name)

    def load_incremental_data(self, endpoint: str, df: pd.DataFrame) -> bool:
        """
        Load data incrementally using UPSERT (INSERT/UPDATE) logic
        Similar to TikTok Shop loader pattern

        Args:
            endpoint: MISA CRM endpoint name (customers, sale_orders, etc.)
            df: DataFrame with data to load

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if df.empty:
                logger.warning(f"DataFrame is empty for {endpoint}, nothing to load")
                return True

            logger.info(
                f"Loading {len(df)} rows incrementally for {endpoint} with UPSERT logic..."
            )

            # Prepare the data
            df_prepared = self._prepare_dataframe_for_upsert(df, endpoint)
            if df_prepared is None:
                return False

            # Use MERGE statement for proper UPSERT
            return self._upsert_records(endpoint, df_prepared)

        except Exception as e:
            logger.error(f"Error in incremental load for {endpoint}: {str(e)}")
            return False

    def _prepare_dataframe_for_upsert(
        self, df: pd.DataFrame, endpoint: str
    ) -> Optional[pd.DataFrame]:
        """
        Prepare DataFrame for UPSERT operation

        Args:
            df: Original DataFrame
            endpoint: MISA CRM endpoint name

        Returns:
            Prepared DataFrame or None if error
        """
        try:
            df_prepared = df.copy()

            # Add ETL metadata columns
            current_time = datetime.now()
            df_prepared["etl_batch_id"] = (
                f"misa_crm_{endpoint}_{current_time.strftime('%Y%m%d_%H%M%S')}"
            )
            df_prepared["etl_created_at"] = current_time
            df_prepared["etl_updated_at"] = current_time

            # Handle NaN values
            # Giữ None để ghi NULL lên DB thay vì chuỗi rỗng

            return df_prepared

        except Exception as e:
            logger.error(f"Error preparing DataFrame for {endpoint}: {str(e)}")
            return None

    def _upsert_records(self, endpoint: str, df: pd.DataFrame) -> bool:
        """
        Perform UPSERT operation using SQL MERGE statement
        Similar to TikTok Shop pattern but adapted for MISA CRM endpoints

        Args:
            endpoint: MISA CRM endpoint name
            df: Prepared DataFrame

        Returns:
            bool: True if successful, False otherwise
        """
        if endpoint not in self.table_mappings:
            logger.error(f"No table mapping found for endpoint: {endpoint}")
            return False

        table_full_name = self.table_mappings[endpoint]
        table_info = self._get_table_info(table_full_name)

        try:
            with self.db_engine.connect() as conn:
                # Create temporary table
                temp_table = (
                    f"#temp_{endpoint}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                )

                # Load data to temp table first (giảm chunksize để tránh lỗi ODBC parameter markers)
                df.to_sql(
                    name=temp_table.replace("#", ""),
                    con=conn,
                    if_exists="replace",
                    index=False,
                    method="multi",
                    chunksize=15,
                )

                # Get primary key for each endpoint
                primary_key = self._get_primary_key_for_endpoint(endpoint)

                # Perform MERGE operation
                merge_sql = self._build_merge_sql(
                    endpoint, table_info, temp_table, primary_key, df.columns.tolist()
                )

                result = conn.execute(text(merge_sql))
                rows_affected = result.rowcount

                # Drop temp table
                conn.execute(text(f"DROP TABLE {temp_table}"))

                logger.info(
                    f"UPSERT completed for {endpoint}: {rows_affected} rows affected"
                )
                return True

        except Exception as e:
            logger.error(f"Error in _upsert_records for {endpoint}: {str(e)}")
            return False

    def _get_primary_key_for_endpoint(self, endpoint: str) -> str:
        """
        Get primary key column name for each MISA CRM endpoint

        Args:
            endpoint: MISA CRM endpoint name

        Returns:
            Primary key column name
        """
        primary_keys = {
            "customers": "customer_id",
            "sale_orders_flattened": "order_id",
            "contacts": "contact_id",
            "stocks": "stock_id",
            "products": "product_id",
        }

        return primary_keys.get(endpoint, "id")  # Default fallback

    def _get_update_condition_for_endpoint(self, endpoint: str) -> Optional[str]:
        """
        Get the update condition for the MERGE statement for a given endpoint.
        This is used to only update rows that have actually changed.
        Compares source and target columns based on `ModifiedOn` timestamp.

        Args:
            endpoint: MISA CRM endpoint name

        Returns:
            Update condition string or None
        """
        # Using ModifiedOn as the primary indicator of change
        conditions = {
            "customers": "target.ModifiedOn < source.ModifiedOn",
            "sale_orders_flattened": "target.ModifiedOn < source.ModifiedOn",
            "contacts": "target.ModifiedOn < source.ModifiedOn",
            "stocks": "target.ModifiedOn < source.ModifiedOn",
            "products": "target.ModifiedOn < source.ModifiedOn",
        }
        return conditions.get(endpoint)

    def _build_merge_sql(
        self,
        endpoint: str,
        table_info: Dict,
        temp_table: str,
        primary_key: str,
        columns: List[str],
        update_condition: Optional[str] = None,
    ) -> str:
        """
        Build SQL MERGE statement for UPSERT operation

        Args:
            endpoint: MISA CRM endpoint name
            table_info: Table schema and name info
            temp_table: Temporary table name
            primary_key: Primary key column name
            columns: List of DataFrame columns

        Returns:
            SQL MERGE statement
        """
        schema = table_info["schema"]
        table = table_info["table"]

        # Filter out ETL metadata columns for matching conditions
        data_columns = [col for col in columns if not col.startswith("etl_")]

        # Build UPDATE SET clause
        update_set = []
        for col in data_columns:
            if col != primary_key:  # Don't update primary key
                update_set.append(f"target.{col} = source.{col}")

        # Add ETL metadata update
        update_set.append("target.etl_updated_at = GETDATE()")

        # Build INSERT columns and values
        insert_columns = ", ".join(columns)
        insert_values = ", ".join([f"source.{col}" for col in columns])

        merge_sql = f"""
        MERGE [{schema}].[{table}] AS target
        USING {temp_table} AS source
        ON target.{primary_key} = source.{primary_key}

        WHEN MATCHED THEN
            UPDATE SET
                {', '.join(update_set)}

        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values});
        """

        return merge_sql

    def _load_with_pyodbc(self, df: pd.DataFrame, table_full_name: str) -> bool:
        """
        Alternative loading method using pyodbc for composite key tables
        """
        try:
            import pyodbc

            # Create connection string for pyodbc
            connection_string = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={settings.sql_server_host},{settings.sql_server_port};"
                f"DATABASE={settings.sql_server_database};"
                f"UID={settings.sql_server_username};"
                f"PWD={settings.sql_server_password};"
                f"TrustServerCertificate=yes"
            )

            connection = pyodbc.connect(connection_string)
            cursor = connection.cursor()

            # Get table info
            table_info = self._get_table_info(table_full_name)
            schema = table_info["schema"]
            table = table_info["table"]

            # Get table columns (excluding computed columns)
            cursor.execute(
                f"""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema}'
                AND TABLE_NAME = '{table}'
                AND COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA+'.'+TABLE_NAME), COLUMN_NAME, 'IsComputed') = 0
                ORDER BY ORDINAL_POSITION
            """
            )

            db_columns = [row.COLUMN_NAME for row in cursor.fetchall()]

            # Match DataFrame columns with database columns
            matching_columns = [col for col in db_columns if col in df.columns]

            # DEBUG: Log column mismatch details
            missing_in_df = [col for col in db_columns if col not in df.columns]
            extra_in_df = [col for col in df.columns if col not in db_columns]

            if missing_in_df:
                logger.warning(f"Columns missing in DataFrame: {missing_in_df}")
            if extra_in_df:
                logger.warning(f"Extra columns in DataFrame: {extra_in_df}")

            logger.info(
                f"Matched {len(matching_columns)} columns out of {len(db_columns)} database columns"
            )
            # FIXED: Sắp xếp DataFrame columns theo thứ tự database để tránh lỗi column order

            if matching_columns:
                df_ordered = df[matching_columns]
                logger.info(f"Reordered DataFrame columns to match database order")
            else:
                df_ordered = df

            if not matching_columns:
                logger.error(
                    f"No matching columns found between DataFrame and {table_full_name}"
                )
                logger.error(f"DataFrame columns: {sorted(df.columns.tolist())}")
                logger.error(f"Database columns: {sorted(db_columns)}")
                return False

            # Prepare insert statement
            placeholders = ", ".join(["?" for _ in matching_columns])
            insert_sql = f"INSERT INTO {schema}.{table} ({', '.join(matching_columns)}) VALUES ({placeholders})"

            # Insert data in batches - FIXED: Giảm batch size để tránh parameter limit
            batch_size = min(
                100, len(df)
            )  # Giảm batch size xuống 100 để tránh parameter limit
            total_inserted = 0

            for i in range(0, len(df), batch_size):
                batch_df = df_ordered.iloc[i : i + batch_size]
                batch_data = []

                for _, row in batch_df.iterrows():
                    row_data = []
                    for col in matching_columns:
                        value = row[col] if col in row else None
                        # FIXED: Handle NaN values và data type conversion
                        if pd.isna(value):
                            row_data.append(None)
                        else:
                            # Convert data types để tránh type mismatch
                            converted_value = self._convert_value_for_sql(value)
                            row_data.append(converted_value)
                    batch_data.append(row_data)

                # Execute batch insert
                cursor.executemany(insert_sql, batch_data)
                connection.commit()
                total_inserted += len(batch_data)

                logger.info(
                    f"   Inserted batch {i//batch_size + 1}: {len(batch_data)} rows"
                )

            cursor.close()
            connection.close()

            logger.info(
                f"Successfully loaded {total_inserted} records to {table_full_name} using pyodbc"
            )
            return True

        except Exception as e:
            logger.error(f"pyodbc loading failed for {table_full_name}: {e}")
            return False

    def _convert_value_for_sql(self, value):
        """
        Convert value tối giản để tương thích SQL Server data types.
        Sau khi đã normalize DataFrame, hàm này chỉ xử lý các trường hợp phổ biến.
        """
        if value is None or pd.isna(value):
            return None

        # Datetime
        if isinstance(value, (pd.Timestamp, datetime)):
            if hasattr(value, "tz") and value.tz is not None:
                value = (
                    value.tz_convert(None) if hasattr(value, "tz_convert") else value
                )
            try:
                return value.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return str(value)

        # Integer/Float/Bool/Str giữ nguyên dạng phù hợp
        if isinstance(value, (int, np.integer)):
            return int(value)

        if isinstance(value, (float, np.floating)):
            return float(value)

        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            return value

        # Mặc định: stringify
        return str(value)

    def _normalize_datetime_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Chuẩn hóa các cột datetime: parse ISO/epoch, bỏ timezone, thay NaT bằng None.
        Áp dụng an toàn cho mọi DataFrame (không thay đổi cột không phải datetime).
        """
        df_norm = df.copy()
        for col in df_norm.columns:
            s = df_norm[col]
            # Bỏ qua nếu là các kiểu không liên quan
            if pd.api.types.is_datetime64_any_dtype(s):
                # Loại bỏ timezone (nếu có) và thay NaT bằng None
                try:
                    s2 = s.dt.tz_convert(None)
                except Exception:
                    s2 = s
                df_norm[col] = s2.where(pd.notna(s2), None)
            else:
                # Nếu là numeric có thể là epoch (ms/s) hoặc string ISO
                if pd.api.types.is_numeric_dtype(s):
                    s_float = s.astype("float64")
                    if s_float.dropna().gt(1e12).any():
                        dt = pd.to_datetime(
                            s_float, unit="ms", errors="coerce", utc=True
                        )
                    elif s_float.dropna().gt(1e9).any():
                        dt = pd.to_datetime(
                            s_float, unit="s", errors="coerce", utc=True
                        )
                    else:
                        continue
                    dt = dt.dt.tz_convert(None)
                    df_norm[col] = dt.where(pd.notna(dt), None)
                elif pd.api.types.is_string_dtype(s):
                    dt = pd.to_datetime(s, errors="coerce", utc=True)
                    if dt.notna().any():
                        dt = dt.dt.tz_convert(None)
                        df_norm[col] = dt.where(pd.notna(dt), None)
        return df_norm

    def _normalize_dataframe_for_endpoint(
        self, df: pd.DataFrame, endpoint: str, table_info: Dict[str, Any]
    ) -> pd.DataFrame:
        """
        - Lọc/điều chỉnh cột theo schema đích
        - Chuẩn hóa dữ liệu THEO KIỂU CỘT trong schema:
          + DATETIME/DATETIME2: parse ISO/epoch → tz-naive, NaT→None
          + DECIMAL/NUMERIC/INT: ép số `to_numeric(errors='coerce')`
          + BIT: map về True/False hoặc None
          + NVARCHAR: có thể thay "" → None để sạch
        """
        df_norm = df.copy()

        # Lấy danh sách cột & kiểu dữ liệu từ DB
        try:
            with self.db_engine.connect() as conn:
                cols = conn.execute(
                    text(
                        """
                        SELECT COLUMN_NAME, DATA_TYPE
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table
                        ORDER BY ORDINAL_POSITION
                        """
                    ),
                    {"schema": table_info["schema"], "table": table_info["table"]},
                ).fetchall()
                db_columns = [r[0] for r in cols]
                column_types = {r[0]: str(r[1]).lower() for r in cols}
        except Exception:
            db_columns = df_norm.columns.tolist()
            column_types = {c: "nvarchar" for c in db_columns}

        # Nếu endpoint là customers và schema có account_code nhưng nguồn không có → bỏ cột này
        if (
            endpoint == "customers"
            and "account_code" in db_columns
            and "account_code" not in df_norm.columns
        ):
            db_columns = [c for c in db_columns if c != "account_code"]

        # Giữ lại cột có trong DB
        keep_cols = [c for c in db_columns if c in df_norm.columns]
        if keep_cols:
            df_norm = df_norm[keep_cols]

        # Chuẩn hóa theo kiểu cột trong schema
        df_norm = self._normalize_by_schema_types(df_norm, column_types)

        return df_norm

    def _predelete_conflicting_sale_order_items(
        self, df: pd.DataFrame, table_info: Dict[str, Any]
    ) -> None:
        """
        Xóa trước các bản ghi trong DB có khóa (order_id, item_id) trùng với batch hiện tại
        để tránh lỗi trùng khóa khi insert. Hàm an toàn: nếu batch rỗng sẽ bỏ qua.
        """
        if df.empty:
            return

        # Lấy cặp khóa duy nhất trong batch
        keys_df = df[["order_id", "item_id"]].dropna().drop_duplicates()
        if keys_df.empty:
            return

        schema = table_info["schema"]
        table = table_info["table"]

        # Tạo danh sách giá trị cho câu lệnh DELETE IN (VALUES ...)
        tuples = list(keys_df.itertuples(index=False, name=None))
        values_clause = ", ".join([f"({int(oid)}, {int(iid)})" for oid, iid in tuples])

        sql = f"""
        DELETE tgt
        FROM [{schema}].[{table}] AS tgt
        JOIN (VALUES {values_clause}) AS src(order_id, item_id)
          ON tgt.order_id = src.order_id AND tgt.item_id = src.item_id
        """

        with self.db_engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()

    def _normalize_by_schema_types(
        self, df: pd.DataFrame, column_types: Dict[str, str]
    ) -> pd.DataFrame:
        df_norm = df.copy()

        datetime_types = {"datetime", "datetime2", "smalldatetime", "date"}
        numeric_types = {
            "decimal",
            "numeric",
            "float",
            "real",
            "int",
            "bigint",
            "smallint",
            "tinyint",
        }

        for col in df_norm.columns:
            dtype = column_types.get(col, "nvarchar")
            s = df_norm[col]

            # DATETIME
            if dtype in datetime_types:
                if pd.api.types.is_datetime64_any_dtype(s):
                    try:
                        s2 = s.dt.tz_convert(None)
                    except Exception:
                        s2 = s
                    df_norm[col] = s2.where(pd.notna(s2), None)
                else:
                    if pd.api.types.is_numeric_dtype(s):
                        s_float = s.astype("float64")
                        if s_float.dropna().gt(1e12).any():
                            dt = pd.to_datetime(
                                s_float, unit="ms", errors="coerce", utc=True
                            )
                        elif s_float.dropna().gt(1e9).any():
                            dt = pd.to_datetime(
                                s_float, unit="s", errors="coerce", utc=True
                            )
                        else:
                            dt = pd.to_datetime(s_float, errors="coerce", utc=True)
                    else:
                        dt = pd.to_datetime(s, errors="coerce", utc=True)
                    dt = dt.dt.tz_convert(None)
                    df_norm[col] = dt.where(pd.notna(dt), None)
                continue

            # NUMERIC
            if dtype in numeric_types:
                if pd.api.types.is_string_dtype(s):
                    s_clean = (
                        s.str.replace("%", "", regex=False)
                        .str.replace(",", "", regex=False)
                        .str.strip()
                    )
                else:
                    s_clean = s
                df_norm[col] = pd.to_numeric(s_clean, errors="coerce")
                continue

            # BIT
            if dtype == "bit":
                if pd.api.types.is_bool_dtype(s):
                    df_norm[col] = s
                else:

                    def _to_bool(v):
                        if pd.isna(v):
                            return None
                        if isinstance(v, bool):
                            return v
                        vs = str(v).strip().lower()
                        if vs in {"1", "true", "yes", "on"}:
                            return True
                        if vs in {"0", "false", "no", "off"}:
                            return False
                        return None

                    df_norm[col] = s.map(_to_bool)
                continue

            # NVARCHAR: thay chuỗi rỗng thành NULL để sạch dữ liệu
            if pd.api.types.is_string_dtype(s):
                df_norm[col] = s.replace("", None)

        return df_norm

    def load_all_data_to_staging(
        self, transformed_data: Dict[str, pd.DataFrame], truncate_first: bool = False
    ) -> Dict[str, int]:
        """
        Load tất cả transformed data vào staging tables

        Args:
            transformed_data: Dict với key là endpoint name, value là DataFrame
            truncate_first: Có truncate tables trước khi load không

        Returns:
            Dict với số records đã load cho mỗi endpoint
        """
        logger.info("Bắt đầu load tất cả data vào staging tables...")

        loaded_counts = {}

        for endpoint, df in transformed_data.items():
            if df.empty:
                logger.warning(f"DataFrame rỗng cho {endpoint}, bỏ qua")
                loaded_counts[endpoint] = 0
                continue

            try:
                # Truncate table nếu được yêu cầu
                if truncate_first:
                    self.truncate_table(endpoint)

                # Load data
                success = self.load_dataframe_to_staging(
                    df, endpoint, if_exists="append"
                )

                if success:
                    loaded_counts[endpoint] = len(df)
                    logger.info(f"{endpoint}: {len(df)} records loaded")
                else:
                    loaded_counts[endpoint] = 0
                    logger.error(f"{endpoint}: Load thất bại")

            except Exception as e:
                logger.error(f"Exception khi load {endpoint}: {e}")
                loaded_counts[endpoint] = 0

        total_loaded = sum(loaded_counts.values())
        failed_endpoints = [
            endpoint
            for endpoint, count in loaded_counts.items()
            if count == 0 and not transformed_data[endpoint].empty
        ]

        if failed_endpoints:
            error_msg = f"Load thất bại cho các endpoints: {failed_endpoints}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

        logger.info(f"Load hoàn thành: {total_loaded} tổng records")
        return loaded_counts

    def validate_loaded_data(self, loaded_counts: Dict[str, int]) -> Dict[str, Any]:
        """
        Validate dữ liệu đã load vào staging tables

        Args:
            loaded_counts: Dict với số records đã load

        Returns:
            Dict với validation results
        """
        logger.info("Đang validate dữ liệu đã load...")

        validation_results = {
            "total_expected_records": sum(loaded_counts.values()),
            "total_actual_records": 0,
            "table_validations": {},
            "validation_passed": True,
        }

        for endpoint, expected_count in loaded_counts.items():
            if endpoint not in self.table_mappings:
                continue

            table_full_name = self.table_mappings[endpoint]

            try:
                with self.db_engine.connect() as conn:
                    # Count records in table
                    result = conn.execute(
                        text(f"SELECT COUNT(*) FROM {table_full_name}")
                    )
                    actual_count = result.fetchone()[0]

                    # Check latest ETL batch
                    result = conn.execute(
                        text(f"SELECT MAX(etl_created_at) FROM {table_full_name}")
                    )
                    latest_etl_time = result.fetchone()[0]

                    table_validation = {
                        "expected_count": expected_count,
                        "actual_count": actual_count,
                        "count_match": actual_count
                        >= expected_count,  # Allow for existing data
                        "latest_etl_time": latest_etl_time,
                        "has_recent_data": latest_etl_time
                        and (datetime.now() - latest_etl_time).total_seconds()
                        < 3600,  # Within 1 hour
                    }

                    validation_results["table_validations"][endpoint] = table_validation
                    validation_results["total_actual_records"] += actual_count

                    if (
                        not table_validation["count_match"]
                        or not table_validation["has_recent_data"]
                    ):
                        validation_results["validation_passed"] = False

                    logger.info(
                        f"📊 {endpoint}: Expected {expected_count}, Actual {actual_count}, Latest ETL: {latest_etl_time}"
                    )

            except Exception as e:
                logger.error(f"Lỗi khi validate {endpoint}: {e}")
                validation_results["validation_passed"] = False
                validation_results["table_validations"][endpoint] = {"error": str(e)}

        logger.info(
            f"Validation tổng thể: {'PASSED' if validation_results['validation_passed'] else 'FAILED'}"
        )

        return validation_results

    def get_staging_data_summary(self) -> Dict[str, Any]:
        """
        Lấy tóm tắt dữ liệu trong staging tables

        Returns:
            Dict với thông tin tóm tắt
        """
        logger.info("Đang lấy tóm tắt dữ liệu staging...")

        summary = {
            "timestamp": datetime.now().isoformat(),
            "tables": {},
            "total_records": 0,
        }

        for endpoint, table_full_name in self.table_mappings.items():
            try:
                with self.db_engine.connect() as conn:
                    # Basic counts
                    result = conn.execute(
                        text(f"SELECT COUNT(*) FROM {table_full_name}")
                    )
                    total_count = result.fetchone()[0]

                    # Latest ETL info
                    result = conn.execute(
                        text(
                            f"""
                        SELECT
                            MAX(etl_created_at) as latest_etl,
                            COUNT(DISTINCT etl_batch_id) as batch_count
                        FROM {table_full_name}
                    """
                        )
                    )
                    etl_info = result.fetchone()

                    # Recent data (last 24 hours)
                    result = conn.execute(
                        text(
                            f"""
                        SELECT COUNT(*)
                        FROM {table_full_name}
                        WHERE etl_created_at >= DATEADD(day, -1, GETDATE())
                    """
                        )
                    )
                    recent_count = result.fetchone()[0]

                    table_summary = {
                        "total_records": total_count,
                        "recent_records_24h": recent_count,
                        "latest_etl_time": etl_info[0],
                        "total_batches": etl_info[1],
                    }

                    summary["tables"][endpoint] = table_summary
                    summary["total_records"] += total_count

                    logger.info(
                        f"📊 {endpoint}: {total_count} records, {recent_count} recent"
                    )

            except Exception as e:
                logger.error(f"Lỗi khi lấy summary cho {endpoint}: {e}")
                summary["tables"][endpoint] = {"error": str(e)}

        logger.info(f"📊 Tổng records trong staging: {summary['total_records']}")

        return summary

    def cleanup_old_data(self, retention_days: int = None) -> Dict[str, int]:
        """
        Cleanup dữ liệu cũ trong staging tables

        Args:
            retention_days: Số ngày giữ lại dữ liệu (None = sử dụng config)

        Returns:
            Dict với số records đã xóa
        """
        if retention_days is None:
            retention_days = settings.misa_crm_data_retention_days

        logger.info(f"Đang cleanup dữ liệu cũ hơn {retention_days} ngày...")

        deleted_counts = {}

        for endpoint, table_full_name in self.table_mappings.items():
            try:
                with self.db_engine.connect() as conn:
                    # Delete old data
                    result = conn.execute(
                        text(
                            f"""
                        DELETE FROM {table_full_name}
                        WHERE etl_created_at < DATEADD(day, -{retention_days}, GETDATE())
                    """
                        )
                    )

                    deleted_count = result.rowcount
                    deleted_counts[endpoint] = deleted_count

                    conn.commit()

                    if deleted_count > 0:
                        logger.info(f"🗑️ {endpoint}: Đã xóa {deleted_count} records cũ")
                    else:
                        logger.info(f"{endpoint}: Không có dữ liệu cũ cần xóa")

            except Exception as e:
                logger.error(f"Lỗi khi cleanup {endpoint}: {e}")
                deleted_counts[endpoint] = 0

        total_deleted = sum(deleted_counts.values())
        logger.info(f"🗑️ Cleanup hoàn thành: {total_deleted} tổng records đã xóa")

        return deleted_counts

    def test_database_connection(self) -> bool:
        """
        Test database connection

        Returns:
            True nếu connection thành công
        """
        try:
            with self.db_engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                test_value = result.fetchone()[0]

                if test_value == 1:
                    logger.info("Database connection test thành công")
                    return True
                else:
                    logger.error("Database connection test thất bại")
                    return False

        except Exception as e:
            logger.error(f"Database connection error: {e}")
            return False
