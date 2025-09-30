# Hệ Thống ETL Enterprise Multi-Platform
│ Dữ liệu tập trung từ TikTok Shop, MISA ## ✨ Tính Năng

- **✅ Full Load ETL**: Lấy toàn bộ dữ liệu historical từ 3 platforms
- **⚡ Incremental ETL**: Tự động cập nhật mỗi 15 phút
- **🔄 Parallel Processing**: 3 platforms xử lý đồng thời
- **📈 Batch Updates**: Tối ưu hóa hiệu suất với batch processing
## 📈 Performanceheck


### Common Issues
- **Token expired**: Check credentials và refresh tokens
- **DB connection**: Verify SQL Server container status
- **API errors**: Review rate limits và error logsuplicate
- **🏢 Enterprise Ready**: Docker + Airflow production-grade
- **🔧 Scalable**: Sẵn sàng tích hợp thêm platforms
│ Full Load tự động + Incremental Updates mỗi 15 phút cho các nền tảng thương mại điện tử
### Staging Tables (Current)

#### TikTok Shop
1. **`staging.tiktok_shop_order_detail`**: (115+ columns)
   - Order details với flatt## 🔧 Platform Features

- **TikTok Shop**: Order flattening, App Key auth, token refresh
- **MISA CRM**: Multi-entity support, OAuth2, incremental tracking  
- **Shopee**: 12 normalized tables, auto token refresh, batch API calls Meta## 📈 Performance & Scaling

- **🔥 Parallel Processing**: 3 platforms (TikTok Shop + MISA CRM + Shopee) đồng thời
- **💾 Memory Management**: Streaming ETL để xử lý datasets lớn
- **⚡ Incremental Updates**: Chỉ xử lý data mới/thay đổi trong 15 phút window
- **🔄 Fault Tolerance**: Auto-retry và error recovery cho từng platform
- **📉 Monitoring**: Comprehensive logging và metrics
- **🎯 Shopee Optimization**: 
  - API batch processing (50 orders per call)
  - Auto token refresh với database persistence
  - Binary search cho earliest date detection
  - Memory-efficient DataFrame processingTL timestamps, batch tracking

#### MISA CRM
2. **`staging.misa_customers`**: (77+ columns)  
   - Customer master data từ MISA CRM
   - Contact info, addresses, business metrics

## ⚙️ Configuration

**Schedule**: 
- Full Load: Manual trigger (1 lần đầu)
- Incremental: Mỗi 10 phút tự động

**Performance Settings**:
- Batch size: 1000 records
- API timeout: 30 seconds
- Retry attempts: 3 times
- Shopee API: 50 orders per batch

### ERD Design (Shopee)
```
shopee_orders (order_sn PK)
├── shopee_recipient_address (order_sn PK/FK)
├── shopee_order_items (order_sn, order_item_id PK)
│   └── shopee_order_item_locations (order_sn, order_item_id, location_id PK)
├── shopee_packages (order_sn, package_number PK)
│   └── shopee_package_items (order_sn, package_number, order_item_id PK)
├── shopee_invoice (order_sn PK/FK)
├── shopee_payment_info (order_sn, transaction_id PK)
├── shopee_order_pending_terms (order_sn, term PK)
├── shopee_order_warnings (order_sn, warning PK)
├── shopee_prescription_images (order_sn, image_url PK)
└── shopee_buyer_proof_of_collection (order_sn, image_url PK)
``` Dự Án

**✅ HOÀN THÀNH**: TikTok Shop + MISA CRM + Shopee  
**🔄 PRODUCTION**: Full Load + Incremental ETL tự động cho 3 platforms  
**🔮 TƯƠNG LAI**: Mở rộng thêm các nền tảng khác (Lazada, Sendo, etc.) 

## 🏗️ Kiến Trúc

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Data Sources    │───▶│  ETL Process     │───▶│ SQL Server     │
│ TikTok Shop     │    │ (Airflow DAG)    │    │ (Staging DB)   │
│ MISA CRM        │    │                  │    │                │
│ + Shopee (T2)   │    │ Auto-Schedule    │    │ Data Warehouse │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                ↓
                         Docker Containers
```

## ✨ Tính Năng

- **✅ Full Load ETL**: Lấy toàn bộ dữ liệu historical từ TikTok Shop (từ 1/7/2024) + MISA CRM
- **⚡ Incremental ETL**: Tự động cập nhật dữ liệu mới mỗi 10 phút cho cả 2 nền tảng
- **🔄 Parallel Processing**: TikTok Shop và MISA CRM xử lý đồng thời
- **📈 Real-time Updates**: UPSERT logic để tránh dữ liệu duplicate  
- **🎯 Data Quality**: Validation, error handling và logging toàn diện
- **🏢 Enterprise Ready**: Docker + Apache Airflow cho production environment
- **🔧 Scalable**: Kiến trúc sẵn sàng tích hợp các nền tảng mới

## 📁 Cấu Trúc Dự Án

```
facolos-data-pipelines/
├── dags/                           # Airflow DAGs
│   ├── full_load_etl_dag.py        # Full Load: 1 lần duy nhất cho 3 platforms
│   ├── incremental_etl_dag.py      # Incremental: mỗi 10 phút cho 3 platforms
│   └── test_etl_limited_data.py    # Testing DAG
├── src/                           # Mã nguồn ETL
│   ├── extractors/                # API Data Extractors
│   │   ├── tiktok_shop_extractor.py
│   │   ├── misa_crm_extractor.py
│   │   └── shopee_orders_extractor.py   # ✅ SHOPEE COMPLETED
│   ├── transformers/              # Data Transform
│   │   ├── tiktok_shop_transformer.py
│   │   ├── misa_crm_transformer.py
│   │   └── shopee_orders_transformer.py # ✅ SHOPEE COMPLETED
│   ├── loaders/                   # Database Loaders
│   │   ├── tiktok_shop_staging_loader.py
│   │   ├── misa_crm_loader.py
│   │   └── shopee_orders_loader.py      # ✅ SHOPEE COMPLETED
│   └── utils/                     # Shared Utilities
│       ├── auth.py               # Multi-platform Authentication
│       ├── database.py           # SQL Server connections
│       ├── logging.py            # Enterprise logging
│       ├── etl_logging.py        # ETL-specific logging
│       └── quiet_logger.py       # Quiet logging for cleanup
├── config/                       # Application Settings  
│   ├── settings.py              # Multi-platform configurations
│   └── production.py             # Production overrides
├── sql/                          # Database Scripts
│   ├── 00_master_setup.sql      # Database + Tables creation (includes Shopee)
│   └── entrypoint.sh            # DB initialization
├── docs/                         # Documentation
│   ├── SHOPEE_INTEGRATION_GUIDE.md      # ✅ SHOPEE DOCS
│   ├── shopee_orders.ipynb             # ✅ SHOPEE DEVELOPMENT
│   ├── misa_crm_api.ipynb              # MISA CRM development
│   ├── tiktok_shop_api.ipynb           # TikTok Shop development
│   └── shopee_orders_data/             # Sample data files
├── logs/                         # Log files
├── docker-compose.yml            # Multi-container orchestration  
├── requirements.txt              # Python dependencies
└── README.md                    # File này
```

## 🔧 Cài Đặt & Triển Khai

### 1. Prerequisites
- Docker + Docker Compose 
- ℹ️ **TikTok Shop API**: App Key, Secret, Access Token, Refresh Token, Shop Cipher
- ℹ️ **MISA CRM API**: Client ID, Client Secret, Access Token
- ℹ️ **Shopee API**: Partner ID, Partner Key, Shop ID, Access Token, Refresh Token
- ℹ️ **SQL Server**: Database connection credentials
- 💾 **Minimum**: 4GB RAM cho containers

### 2. Environment Setup

```bash
# Clone repository
git clone <repository-url>
cd facolos-data-pipelines

# Cấu hình credentials
cp .env.example .env
nano .env  # Chỉnh sửa API credentials
```

**File `.env` template:**
```env
# TikTok Shop API Credentials  
TIKTOK_APP_KEY=your_app_key
TIKTOK_APP_SECRET=your_app_secret_here
TIKTOK_ACCESS_TOKEN=your_access_token_here  
TIKTOK_REFRESH_TOKEN=your_refresh_token_here
TIKTOK_SHOP_CIPHER=your_shop_cipher_here

# MISA CRM API Credentials
MISA_CRM_CLIENT_ID=your_client_id
MISA_CRM_CLIENT_SECRET=your_client_secret_here
MISA_CRM_ACCESS_TOKEN=your_access_token_here

# Shopee API Credentials (REQUIRED)
SHOPEE_PARTNER_ID=your_partner_id
SHOPEE_PARTNER_KEY=your_partner_key
SHOPEE_SHOP_ID=your_shop_id
SHOPEE_REDIRECT_URI=https://yourapp.com/callback

# Shopee tokens (lần đầu cần điền; lần sau sẽ đọc DB và auto refresh)
SHOPEE_ACCESS_TOKEN=your_first_run_access_token
SHOPEE_REFRESH_TOKEN=your_first_run_refresh_token

# Database Configurations
SQL_SERVER_PASSWORD=your_secure_password
SQL_SERVER_DATABASE=Facolos_Staging

# Shopee ETL Settings (optional)
SHOPEE_TOKEN_REFRESH_BUFFER=300
SHOPEE_ETL_BATCH_SIZE=1000
SHOPEE_INCREMENTAL_LOOKBACK_MINUTES=15
```

### 3. Launch Application

```bash
# Khởi động toàn bộ system
docker-compose up -d

# Kiểm tra containers
docker-compose ps  
docker-compose logs -f airflow-webserver
```

### 4. Truy Cập Dashboard

- **🖥️ Airflow Web UI**: http://localhost:8080
  - Username: `admin`
  - Password: Xem trong docker-compose.yml
- **💾 Database**: SQL Server trên port 1433 (development connection)

## 🚀 Sử Dụng ETL Pipeline

### Full Load (Lần đầu - 1 lần)

1. **Trigger Full Load DAG:**
   - Vào Airflow UI → DAGs → `full_load_etl_dag`
   - Click "Trigger DAG" để chạy một lần duy nhất
   - Quá trình sẽ lấy TẤT CẢ dữ liệu historical từ:
     - **TikTok Shop**: Từ 1/7/2024 đến hiện tại
     - **MISA CRM**: Tất cả customers, products, orders
     - **Shopee**: Auto-detect earliest order date (tối đa 2 năm) hoặc từ ngày cấu hình

2. **Monitoring:**
   - Theo dõi progress trên Airflow UI
   - Check data trong SQL Server tables
   - Logs chi tiết trong `/logs/` folder

### Incremental Updates (15 phút/lần)

Theo mặc định sẽ **TỰ ĐỘNG CHẠY** mỗi 15 phút:

- ✅ **TikTok Shop**: Dữ liệu đơn hàng mới/cập nhật trong 15 phút gần nhất  
- ✅ **MISA CRM**: Khách hàng, đơn hàng, sản phẩm, kho và liên hệ được cập nhật
- ✅ **Shopee**: Đơn hàng mới trong 15 phút gần nhất với auto token refresh
- 🔄 **UPSERT Logic**: Không tạo duplicate, chỉ update dữ liệu có thay đổi

### Manual Execution (Testing)

```bash
# Test kết nối tất cả APIs
python test_connections.py

# Test từng platform riêng lẻ
python -c "from src.extractors.misa_crm_extractor import MISACRMExtractor; MISACRMExtractor().health_check()"
python -c "from src.extractors.tiktok_shop_extractor import TikTokShopOrderExtractor; TikTokShopOrderExtractor().test_api_connection()"
python -c "from src.extractors.shopee_orders_extractor import ShopeeOrderExtractor; extractor = ShopeeOrderExtractor(); print('Shopee ready:', bool(extractor.access_token))"
```

## 📊 Database Schema

### Staging Tables (19 bảng)

#### TikTok Shop (1 bảng)
- **`staging.tiktok_shop_order_detail`**: Order details với flattened line items (115+ columns)

#### MISA CRM (5 bảng)
- **`staging.misa_customers`**: Customer master data (77+ columns)
- **`staging.misa_sale_orders_flattened`**: Flattened order items 
- **`staging.misa_contacts`**: Contact person data
- **`staging.misa_stocks`**: Stock/warehouse data
- **`staging.misa_products`**: Product catalog

#### Shopee Platform (12 bảng normalized)
- **`staging.shopee_orders`**: Main orders table
- **`staging.shopee_recipient_address`**: Delivery addresses
- **`staging.shopee_order_items`**: Order line items
- **`staging.shopee_order_item_locations`**: Item locations
- **`staging.shopee_packages`**: Package information
- **`staging.shopee_package_items`**: Items in packages
- **`staging.shopee_invoice`**: Invoice details
- **`staging.shopee_payment_info`**: Payment transactions
- **`staging.shopee_order_pending_terms`**: Pending terms
- **`staging.shopee_order_warnings`**: Order warnings
- **`staging.shopee_prescription_images`**: Prescription images
- **`staging.shopee_buyer_proof_of_collection`**: Collection proof

#### ETL Control (1 bảng)
- **`etl_control.api_token_storage`**: API token management


## ⚙️ Configuration

**Schedule**: 
- Full Load: Manual trigger (1 lần đầu)
- Incremental: Mỗi 10 phút tự động


## 📈 Performance

- **Parallel Processing**: 3 platforms đồng thời
- **Incremental Updates**: Chỉ data mới trong 15 phút
- **Memory Efficient**: Streaming + batch processing
- **Fault Tolerance**: Auto-retry + error recovery

## 💼 Business Value

✅ **Multi-Platform Data**: TikTok Shop + MISA CRM + Shopee  
✅ **Real-time Sync**: Cập nhật mỗi 10 phút  
✅ **Analytics Ready**: Normalized schema  
✅ **Fully Automated**: Zero manual intervention  
✅ **Production Grade**: Docker + Airflow

### Airflow UI Monitoring
- **Logs**: Real-time log viewing per task cho cả 3 platforms
- **Task Duration**: Performance metrics 
- **Failure Alerts**: Automatic task retry logic
- **Manual Triggers**: On-demand execution

