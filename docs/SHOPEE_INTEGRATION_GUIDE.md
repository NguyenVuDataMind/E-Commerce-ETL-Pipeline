# Hướng dẫn tích hợp Shopee vào Facolos Enterprise ETL System

## Tổng quan

Tài liệu này hướng dẫn cách tích hợp Shopee Platform vào hệ thống ETL hiện tại của Facolos Enterprise, bổ sung vào TikTok Shop và MISA CRM đã có sẵn.

## Kiến trúc tích hợp

### 1. Cấu trúc thư mục
```
src/
├── extractors/
│   └── shopee_orders_extractor.py      # Trích xuất dữ liệu từ Shopee API
├── transformers/
│   └── shopee_orders_transformer.py    # Chuyển đổi JSON thành DataFrame
└── loaders/
    └── shopee_orders_loader.py         # Load dữ liệu vào staging database

dags/
├── full_load_etl_dag.py               # Full load cho tất cả platforms
└── incremental_etl_dag.py             # Incremental load mỗi 10 phút

sql/staging/
└── create_shopee_orders_tables.sql    # Script tạo bảng staging
```

### 2. Luồng xử lý dữ liệu

#### Full Load (Lần đầu)
1. **Extract**: Lấy tất cả dữ liệu lịch sử từ Shopee API
2. **Transform**: Chuyển đổi JSON thành các DataFrame theo ERD
3. **Load**: Load vào staging database với 12 bảng

#### Incremental Load (Mỗi 10 phút)
1. **Extract**: Lấy dữ liệu mới trong 15 phút gần nhất
2. **Transform**: Chuyển đổi thành DataFrame phẳng
3. **Load**: UPSERT vào bảng orders chính

## Cấu hình

### 1. Environment Variables

Thêm các biến môi trường sau vào file `.env`:

```bash
# Shopee Platform Credentials
SHOPEE_PARTNER_ID=your_partner_id
SHOPEE_PARTNER_KEY=your_partner_key
SHOPEE_SHOP_ID=your_shop_id
SHOPEE_REDIRECT_URI=https://your-domain.com/
SHOPEE_ACCESS_TOKEN=your_access_token
SHOPEE_REFRESH_TOKEN=your_refresh_token

# Shopee ETL Settings
SHOPEE_TOKEN_REFRESH_BUFFER=300
SHOPEE_ETL_BATCH_SIZE=1000
SHOPEE_INCREMENTAL_LOOKBACK_MINUTES=15
```

### 2. Database Setup

Chạy script SQL để tạo bảng staging:

```sql
-- Chạy file này trong SQL Server Management Studio
sql/staging/create_shopee_orders_tables.sql
```

Script sẽ tạo 12 bảng trong schema `staging`:
- `orders` (bảng chính)
- `recipient_address`
- `order_items`
- `order_item_locations`
- `packages`
- `package_items`
- `invoice`
- `payment_info`
- `order_pending_terms`
- `order_warnings`
- `prescription_images`
- `buyer_proof_of_collection`

## Sử dụng

### 1. Lấy Access Token

Trước khi chạy ETL, cần lấy access token từ Shopee:

```python
from src.extractors.shopee_orders_extractor import ShopeeOrderExtractor

extractor = ShopeeOrderExtractor()

# Lấy authorization code từ URL (thực hiện thủ công)
auth_code = "your_authorization_code"

# Lấy access token
token_response = extractor.get_access_token_from_code(auth_code)
if token_response:
    print(f"Access Token: {token_response['access_token']}")
    print(f"Refresh Token: {token_response['refresh_token']}")
```

### 2. Chạy Full Load

```bash
# Chạy DAG trong Airflow UI hoặc CLI
airflow dags trigger full_load_etl_dag
```

### 3. Chạy Incremental Load

Incremental DAG sẽ tự động chạy mỗi 10 phút:

```bash
# Kiểm tra trạng thái DAG
airflow dags state incremental_etl_dag
```

## Cấu trúc dữ liệu

### 1. ERD (Entity Relationship Diagram)

```
orders (order_sn PK)
├─ recipient_address (order_sn PK/FK)
├─ order_items (order_sn, order_item_id PK)
│   └─ order_item_locations (order_sn, order_item_id, location_id PK)
├─ packages (order_sn, package_number PK)
│   └─ package_items (order_sn, package_number, order_item_id PK)
├─ invoice (order_sn PK/FK)
├─ payment_info (order_sn, transaction_id PK)
├─ order_pending_terms (order_sn, term PK)
├─ order_warnings (order_sn, warning PK)
├─ prescription_images (order_sn, image_url PK)
└─ buyer_proof_of_collection (order_sn, image_url PK)
```

### 2. Mapping JSON → Database

| JSON Field | Database Table | Column |
|------------|----------------|---------|
| `order_sn` | `orders` | `order_sn` (PK) |
| `recipient_address` | `recipient_address` | Tất cả fields |
| `item_list[]` | `order_items` | Mỗi item một row |
| `package_list[]` | `packages` | Mỗi package một row |
| `invoice_data` | `invoice` | Tất cả fields |
| `payment_info[]` | `payment_info` | Mỗi payment một row |

## Monitoring và Troubleshooting

### 1. Logs

Kiểm tra logs trong Airflow UI:
- Task logs cho từng bước ETL
- Error logs nếu có lỗi

### 2. Database Queries

Kiểm tra dữ liệu đã load:

```sql
-- Kiểm tra số lượng orders
SELECT COUNT(*) FROM staging.orders;

-- Kiểm tra orders mới nhất
SELECT TOP 10 order_sn, create_time, order_status 
FROM staging.orders 
ORDER BY create_time DESC;

-- Kiểm tra incremental load
SELECT COUNT(*) FROM staging.orders 
WHERE etl_created_at >= DATEADD(minute, -20, GETUTCDATE());
```

### 3. Common Issues

#### Lỗi Authentication
```
❌ Failed to get access token: invalid_grant
```
**Giải pháp**: Refresh token đã hết hạn, cần lấy authorization code mới

#### Lỗi API Rate Limit
```
❌ Request failed: 429 Too Many Requests
```
**Giải pháp**: Tăng delay giữa các API calls

#### Lỗi Database Connection
```
❌ Failed to load DataFrame: Login failed for user
```
**Giải pháp**: Kiểm tra connection string và credentials

## Performance Tuning

### 1. Batch Size
- **API Batch Size**: 50 orders per `get_order_detail` call
- **ETL Batch Size**: 1000 records per database insert
- **Incremental Lookback**: 15 minutes (buffer 5 phút)

### 2. Memory Optimization
- Sử dụng streaming cho large datasets
- Convert datetime columns để tiết kiệm memory
- Clean up DataFrames sau khi sử dụng

### 3. Database Indexing
- Index trên `update_time` cho incremental queries
- Index trên `order_sn` cho foreign key lookups
- Index trên `etl_created_at` cho monitoring

## API Reference

### ShopeeOrderExtractor

```python
# Full load
orders = extractor.extract_orders_full_load(start_date, end_date)

# Incremental load
orders = extractor.extract_orders_incremental(minutes_back=15)

# Auto-detect earliest date
earliest = extractor.find_earliest_order_date(max_lookback_years=2)
```

### ShopeeOrderTransformer

```python
# Transform to ERD DataFrames
dataframes = transformer.transform_orders_to_dataframes(orders_list)

# Transform to flat DataFrame
df = transformer.transform_orders_to_flat_dataframe(orders_list)
```

### ShopeeOrderLoader

```python
# Full load
success = loader.load_orders_full_load(dataframes)

# Incremental load
success = loader.load_orders_incremental(df)

# Load flat DataFrame
success = loader.load_flat_orders_dataframe(df, load_type='full')
```

## Kết luận

Shopee đã được tích hợp thành công vào Facolos Enterprise ETL System với:

✅ **Full Load**: Lấy tất cả dữ liệu lịch sử  
✅ **Incremental Load**: Cập nhật dữ liệu mới mỗi 10 phút  
✅ **ERD Design**: 12 bảng normalized theo thiết kế quan hệ  
✅ **Error Handling**: Xử lý lỗi và retry logic  
✅ **Monitoring**: Logs và validation  
✅ **Performance**: Batch processing và memory optimization  

Hệ thống hiện tại hỗ trợ 3 platforms: **TikTok Shop**, **MISA CRM**, và **Shopee** với kiến trúc ETL nhất quán và có thể mở rộng.
