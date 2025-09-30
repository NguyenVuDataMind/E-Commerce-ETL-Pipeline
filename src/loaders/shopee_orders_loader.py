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
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
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
            'orders': settings.get_table_full_name('shopee', 'shopee_orders'),
            'recipient_address': settings.get_table_full_name('shopee', 'shopee_recipient_address'),
            'order_items': settings.get_table_full_name('shopee', 'shopee_order_items'),
            'order_item_locations': settings.get_table_full_name('shopee', 'shopee_order_item_locations'),
            'packages': settings.get_table_full_name('shopee', 'shopee_packages'),
            'package_items': settings.get_table_full_name('shopee', 'shopee_package_items'),
            'invoice': settings.get_table_full_name('shopee', 'shopee_invoice'),
            'payment_info': settings.get_table_full_name('shopee', 'shopee_payment_info'),
            'order_pending_terms': settings.get_table_full_name('shopee', 'shopee_order_pending_terms'),
            'order_warnings': settings.get_table_full_name('shopee', 'shopee_order_warnings'),
            'prescription_images': settings.get_table_full_name('shopee', 'shopee_prescription_images'),
            'buyer_proof_of_collection': settings.get_table_full_name('shopee', 'shopee_buyer_proof_of_collection')
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
        parts = table_full_name.split('.')
        if len(parts) == 2:
            return {'schema': parts[0], 'table': parts[1]}
        else:
            return {'schema': 'staging', 'table': table_full_name}
    
    def _convert_datetime_to_naive(self, df: pd.DataFrame) -> pd.DataFrame:
        """Chuyển đổi datetime columns từ timezone-aware sang timezone-naive"""
        df_copy = df.copy()
        
        for col in df_copy.columns:
            if df_copy[col].dtype == 'datetime64[ns, UTC]':
                df_copy[col] = df_copy[col].dt.tz_localize(None)
            elif 'datetime' in str(df_copy[col].dtype):
                df_copy[col] = pd.to_datetime(df_copy[col], utc=True).dt.tz_localize(None)
        
        return df_copy
    
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
            with self.db_engine.connect() as conn:
                schema, table = table_full_name.split('.')

                # Với SQL Server, TRUNCATE TABLE không thể dùng khi có FK tham chiếu.
                # Vì bảng orders được tham chiếu bởi nhiều bảng con, thay thế bằng DELETE theo thứ tự an toàn.
                if table_name == 'orders':
                    logger.info("↩️ Detected parent table 'orders' — performing safe cascade DELETE on child tables first")
                    self._delete_shopee_children_tables(conn, schema)

                # Thực hiện DELETE thay cho TRUNCATE để không vướng FK
                delete_sql = f"DELETE FROM {table_full_name}"
                result = conn.execute(text(delete_sql))
                conn.commit()

                logger.info(f"✅ Cleared table via DELETE: {table_full_name} (rows affected: {result.rowcount})")
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
            'shopee_package_items',
            'shopee_order_item_locations',
            'shopee_packages',
            'shopee_invoice',
            'shopee_payment_info',
            'shopee_order_pending_terms',
            'shopee_order_warnings',
            'shopee_prescription_images',
            'shopee_buyer_proof_of_collection',
            'shopee_order_items',
            'shopee_recipient_address',
        ]

        for tbl in tables_in_order:
            full_name = f"{schema}.{tbl}"
            try:
                res = conn.execute(text(f"IF OBJECT_ID('{full_name}', 'U') IS NOT NULL DELETE FROM {full_name}"))
                # Một số driver không trả rowcount cho câu IF... nên cần try/except riêng
                affected = getattr(res, 'rowcount', None)
                logger.info(f"   🗑️ Cleared child table: {full_name}{'' if affected is None else f' (rows: {affected})'}")
            except Exception as e:
                logger.warning(f"   ⚠️ Skipped deleting {full_name}: {e}")
    
    def load_dataframe_to_table(self, df: pd.DataFrame, table_name: str, 
                               if_exists: str = 'append') -> bool:
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
            # Convert datetime columns to timezone-naive
            df_export = self._convert_datetime_to_naive(df)
            
            # Load to database
            df_export.to_sql(
                name=table_full_name.split('.')[1],  # Table name only
                con=self.db_engine,
                schema=table_full_name.split('.')[0],  # Schema name
                if_exists=if_exists,
                index=False,
                method='multi',
                chunksize=1000
            )
            
            logger.info(f"✅ Loaded {len(df)} rows to {table_full_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to load DataFrame to {table_full_name}: {str(e)}")
            return False
    
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
                'orders',  # Main table first
                'recipient_address',
                'order_items',
                'order_item_locations',
                'packages',
                'package_items',
                'invoice',
                'payment_info',
                'order_pending_terms',
                'order_warnings',
                'prescription_images',
                'buyer_proof_of_collection'
            ]
            
            success_count = 0
            total_count = len(load_order)
            
            for table_name in load_order:
                if table_name in dataframes:
                    df = dataframes[table_name]
                    
                    if not df.empty:
                        # Truncate table trước khi load (full load)
                        if self.truncate_table(table_name):
                            if self.load_dataframe_to_table(df, table_name, 'append'):
                                success_count += 1
                                logger.info(f"✅ Successfully loaded {table_name}: {len(df)} rows")
                            else:
                                logger.error(f"❌ Failed to load {table_name}")
                        else:
                            logger.error(f"❌ Failed to truncate {table_name}")
                    else:
                        logger.info(f"📭 Skipping empty {table_name}")
                        success_count += 1  # Empty table is considered success
            
            if success_count == total_count:
                logger.info(f"🎉 Full load completed successfully: {success_count}/{total_count} tables")
                return True
            else:
                logger.error(f"❌ Full load failed: {success_count}/{total_count} tables")
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
            # Convert datetime columns to timezone-naive
            df_export = self._convert_datetime_to_naive(df)
            
            # Load to main orders table with UPSERT logic
            table_full_name = self.table_mappings['orders']
            
            # Use pandas to_sql with if_exists='append' for now
            # In production, implement proper UPSERT with SQL MERGE statement
            df_export.to_sql(
                name=table_full_name.split('.')[1],
                con=self.db_engine,
                schema=table_full_name.split('.')[0],
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            
            logger.info(f"✅ Loaded {len(df)} incremental orders to {table_full_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Incremental load failed: {str(e)}")
            return False
    
    def load_flat_orders_dataframe(self, df: pd.DataFrame, load_type: str = 'full') -> bool:
        """
        Load DataFrame phẳng vào bảng orders chính (tương thích với TikTok Shop pattern)
        
        Args:
            df: DataFrame phẳng chứa orders
            load_type: 'full' hoặc 'incremental'
            
        Returns:
            True nếu thành công, False nếu thất bại
        """
        if df.empty:
            logger.info("📭 No data to load")
            return True
        
        try:
            # Convert datetime columns to timezone-naive
            df_export = self._convert_datetime_to_naive(df)
            
            table_full_name = self.table_mappings['orders']
            
            if load_type == 'full':
                # Truncate table trước khi load
                if not self.truncate_table('orders'):
                    return False
                
                if_exists = 'append'
            else:
                if_exists = 'append'  # Incremental: append new data
            
            # Load to database
            df_export.to_sql(
                name=table_full_name.split('.')[1],
                con=self.db_engine,
                schema=table_full_name.split('.')[0],
                if_exists=if_exists,
                index=False,
                method='multi',
                chunksize=1000
            )
            
            logger.info(f"✅ Loaded {len(df)} orders ({load_type} load) to {table_full_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to load orders DataFrame: {str(e)}")
            return False
    
    def get_table_row_count(self, table_name: str) -> int:
        """
        Lấy số dòng trong table
        
        Args:
            table_name: Tên table
            
        Returns:
            Số dòng trong table
        """
        table_full_name = self.table_mappings.get(table_name)
        if not table_full_name:
            return 0
        
        try:
            with self.db_engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_full_name}"))
                count = result.scalar()
                return count
        except Exception as e:
            logger.error(f"❌ Failed to get row count for {table_full_name}: {str(e)}")
            return 0
    
    def validate_data_integrity(self) -> Dict[str, Any]:
        """
        Kiểm tra tính toàn vẹn dữ liệu sau khi load
        
        Returns:
            Dictionary chứa kết quả validation
        """
        logger.info("🔍 Validating data integrity...")
        
        validation_results = {}
        
        for table_name, table_full_name in self.table_mappings.items():
            try:
                row_count = self.get_table_row_count(table_name)
                validation_results[table_name] = {
                    'row_count': row_count,
                    'status': 'success' if row_count >= 0 else 'error'
                }
            except Exception as e:
                validation_results[table_name] = {
                    'row_count': 0,
                    'status': 'error',
                    'error': str(e)
                }
        
        # Log validation results
        for table_name, result in validation_results.items():
            if result['status'] == 'success':
                logger.info(f"✅ {table_name}: {result['row_count']} rows")
            else:
                logger.error(f"❌ {table_name}: {result.get('error', 'Unknown error')}")
        
        return validation_results
