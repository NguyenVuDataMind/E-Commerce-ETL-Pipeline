"""
Database utilities for SQL Server connections and operations
"""

import logging
import sqlalchemy as sa
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from typing import Optional, Dict, Any, List
import pandas as pd
import sys
import os

# Add project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config.settings import settings

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manage SQL Server database connections and operations"""

    def __init__(self):
        self.connection_string = settings.sql_server_connection_string
        self.engine: Optional[sa.Engine] = None
        self.SessionLocal: Optional[sessionmaker] = None

    def initialize(self) -> bool:
        """
        Initialize database connection

        Returns:
            True if successful, False otherwise
        """
        try:
            self.engine = create_engine(
                self.connection_string,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False,
            )

            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            self.SessionLocal = sessionmaker(
                autocommit=False, autoflush=False, bind=self.engine
            )

            logger.info("Database connection initialized successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize database connection: {str(e)}")
            return False

    @contextmanager
    def get_connection(self):
        """
        Get database connection as context manager
        """
        if not self.engine:
            if not self.initialize():
                raise RuntimeError("Failed to initialize database connection")

        connection = self.engine.connect()
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def get_session(self):
        """
        Get database session as context manager
        """
        if not self.SessionLocal:
            if not self.initialize():
                raise RuntimeError("Failed to initialize database connection")

        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def execute_sql_file(self, file_path: str) -> bool:
        """
        Execute SQL statements from a file

        Args:
            file_path: Path to SQL file

        Returns:
            True if successful, False otherwise
        """
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                sql_content = file.read()

            # Split by GO statements
            statements = [
                stmt.strip() for stmt in sql_content.split("GO") if stmt.strip()
            ]

            with self.get_connection() as conn:
                for statement in statements:
                    if statement:
                        with conn.begin():  # Start a transaction that auto-commits
                            conn.execute(text(statement))

            logger.info(f"Successfully executed SQL file: {file_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to execute SQL file {file_path}: {str(e)}")
            return False

    def create_staging_schema(self) -> bool:
        """
        Create staging schema if it doesn't exist

        Returns:
            True if successful, False otherwise
        """
        try:
            sql = f"""
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{settings.staging_schema}')
            BEGIN
                EXEC('CREATE SCHEMA {settings.staging_schema}');
            END;
            """

            with self.get_connection() as conn:
                with conn.begin():  # Start a transaction that auto-commits
                    conn.execute(text(sql))

            logger.info(f"Staging schema '{settings.staging_schema}' ensured")
            return True

        except Exception as e:
            logger.error(f"Failed to create staging schema: {str(e)}")
            return False

    def table_exists(self, table_name: str, schema: str = None) -> bool:
        """
        Check if table exists

        Args:
            table_name: Name of the table
            schema: Schema name (optional)

        Returns:
            True if table exists, False otherwise
        """
        try:
            schema = schema or settings.staging_schema

            sql = """
            SELECT COUNT(*) as count
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = :schema 
            AND TABLE_NAME = :table_name
            """

            with self.get_connection() as conn:
                result = conn.execute(
                    text(sql), {"schema": schema, "table_name": table_name}
                ).fetchone()

                return result.count > 0

        except Exception as e:
            logger.error(f"Error checking table existence: {str(e)}")
            return False

    def truncate_table(self, table_name: str, schema: str = None) -> bool:
        """
        Truncate table

        Args:
            table_name: Name of the table
            schema: Schema name (optional)

        Returns:
            True if successful, False otherwise
        """
        try:
            schema = schema or settings.staging_schema
            full_table_name = f"[{schema}].[{table_name}]"

            sql = f"TRUNCATE TABLE {full_table_name}"

            with self.get_connection() as conn:
                with conn.begin():  # Start a transaction that auto-commits
                    conn.execute(text(sql))

            logger.info(f"Table {full_table_name} truncated successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to truncate table {full_table_name}: {str(e)}")
            return False

    def insert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = None,
        if_exists: str = "append",
    ) -> bool:
        """
        Insert DataFrame into database table

        Args:
            df: DataFrame to insert
            table_name: Target table name
            schema: Schema name (optional)
            if_exists: What to do if table exists ('append', 'replace', 'fail')

        Returns:
            True if successful, False otherwise
        """
        try:
            schema = schema or settings.staging_schema

            if not self.engine:
                if not self.initialize():
                    return False

            df.to_sql(
                table_name,
                self.engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                method="multi",
            )

            logger.info(
                f"Successfully inserted {len(df)} rows into {schema}.{table_name}"
            )
            return True

        except Exception as e:
            logger.error(
                f"Failed to insert DataFrame into {schema}.{table_name}: {str(e)}"
            )
            return False


# Global database manager instance
db_manager = DatabaseManager()
