#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Production Configuration for ETL Pipeline
Cấu hình production cho hệ thống ETL incremental 15 phút/lần
"""

import os
from datetime import timedelta
from typing import Dict, Any

class ProductionConfig:
    """Production configuration settings"""
    
    # ETL Schedule Settings
    INCREMENTAL_SCHEDULE_MINUTES = 15
    MAX_ACTIVE_RUNS = 1
    CATCHUP = False
    
    # Retry Configuration
    RETRIES = 3
    RETRY_DELAY_MINUTES = 5
    EXECUTION_TIMEOUT_MINUTES = 12  # Slightly less than 15 min interval
    
    # Performance Settings
    MISA_MAX_PAGES_PER_CYCLE = 2  # Limit for incremental
    TIKTOK_DAYS_BACK_BUFFER = 1   # 1 day buffer for incremental
    BATCH_SIZE = 1000
    
    # Monitoring Settings
    ENABLE_DETAILED_LOGGING = True
    LOG_LEVEL = "INFO"
    ENABLE_PERFORMANCE_METRICS = True
    ENABLE_DATA_QUALITY_CHECKS = True
    
    # Alert Thresholds
    MAX_EXECUTION_TIME_MINUTES = 10
    MIN_RECORDS_THRESHOLD = 0  # Alert if no records for 3 consecutive runs
    MAX_ERROR_RATE_PERCENT = 20  # Alert if error rate > 20%
    
    # Database Settings
    CONNECTION_POOL_SIZE = 5
    CONNECTION_TIMEOUT_SECONDS = 30
    QUERY_TIMEOUT_SECONDS = 300
    
    # Email Alert Settings
    ENABLE_EMAIL_ALERTS = True
    ALERT_EMAIL_RECIPIENTS = [
        "admin@facolos.com",
        "etl-team@facolos.com"
    ]
    
    # Slack Alert Settings (Optional)
    ENABLE_SLACK_ALERTS = False
    SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
    
    # Health Check Settings
    HEALTH_CHECK_ENABLED = True
    HEALTH_CHECK_INTERVAL_MINUTES = 5
    
    @classmethod
    def get_airflow_dag_config(cls) -> Dict[str, Any]:
        """Get Airflow DAG configuration"""
        return {
            'schedule_interval': timedelta(minutes=cls.INCREMENTAL_SCHEDULE_MINUTES),
            'max_active_runs': cls.MAX_ACTIVE_RUNS,
            'catchup': cls.CATCHUP,
            'default_args': {
                'retries': cls.RETRIES,
                'retry_delay': timedelta(minutes=cls.RETRY_DELAY_MINUTES),
                'execution_timeout': timedelta(minutes=cls.EXECUTION_TIMEOUT_MINUTES),
                'email_on_failure': cls.ENABLE_EMAIL_ALERTS,
                'email_on_retry': False,
                'email': cls.ALERT_EMAIL_RECIPIENTS
            }
        }
    
    @classmethod
    def get_performance_thresholds(cls) -> Dict[str, Any]:
        """Get performance monitoring thresholds"""
        return {
            'max_execution_time': cls.MAX_EXECUTION_TIME_MINUTES * 60,  # seconds
            'min_records_threshold': cls.MIN_RECORDS_THRESHOLD,
            'max_error_rate': cls.MAX_ERROR_RATE_PERCENT / 100,
            'connection_timeout': cls.CONNECTION_TIMEOUT_SECONDS,
            'query_timeout': cls.QUERY_TIMEOUT_SECONDS
        }

# Production Environment Variables
PRODUCTION_ENV = {
    'ENVIRONMENT': 'production',
    'LOG_LEVEL': ProductionConfig.LOG_LEVEL,
    'ENABLE_MONITORING': str(ProductionConfig.ENABLE_PERFORMANCE_METRICS),
    'ENABLE_ALERTS': str(ProductionConfig.ENABLE_EMAIL_ALERTS),
    'ETL_SCHEDULE_MINUTES': str(ProductionConfig.INCREMENTAL_SCHEDULE_MINUTES)
}
