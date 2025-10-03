# ETL Pipeline Test Scripts

Thư mục này chứa các script test để kiểm tra ETL pipeline.

## 📋 Các Script Test

### 1. `test_dag_functionality.py`
**Mục đích:** Test chức năng cơ bản của DAGs mà không cần API keys thực
**Sử dụng:** Chạy trong CI/CD pipeline
**Kiểm tra:**
- ✅ Import các modules
- ✅ Khởi tạo classes
- ✅ Logic transformation với sample data
- ✅ Cấu trúc DAG
- ✅ Configuration files

```bash
python scripts/test_dag_functionality.py
```

### 2. `test_data_extraction.py`
**Mục đích:** Test thực tế việc lấy dữ liệu từ API (cần API keys)
**Sử dụng:** Chạy local để kiểm tra API connections
**Kiểm tra:**
- ✅ MISA CRM API connection
- ✅ TikTok Shop API connection
- ✅ Shopee API connection
- ✅ Database connection
- ✅ Data transformation

```bash
python scripts/test_data_extraction.py
```

### 3. `test_real_data_extraction.py`
**Mục đích:** Test toàn bộ ETL pipeline với dữ liệu thực
**Sử dụng:** Chạy local để kiểm tra end-to-end pipeline
**Kiểm tra:**
- ✅ MISA CRM full extraction
- ✅ TikTok Shop full extraction
- ✅ Shopee orders full extraction
- ✅ Full ETL pipeline

```bash
python scripts/test_real_data_extraction.py
```

### 4. `test_token_and_database.py`
**Mục đích:** Test chuyên biệt cho token refresh và database loading
**Sử dụng:** Chạy local để kiểm tra thực tế
**Kiểm tra:**
- ✅ Token refresh thực tế
- ✅ Database connection thực tế
- ✅ Database loading logic
- ✅ Full ETL pipeline với database

```bash
python scripts/test_token_and_database.py
```

## 🔧 Cách sử dụng

### Test cơ bản (không cần API keys)
```bash
# Test chức năng DAGs
python scripts/test_dag_functionality.py
```

### Test với API keys thực
```bash
# Đảm bảo đã cấu hình API keys trong .env
python scripts/test_data_extraction.py

# Test toàn bộ pipeline
python scripts/test_real_data_extraction.py

# Test token refresh và database
python scripts/test_token_and_database.py
```

## 📊 Kết quả Test

### ✅ PASS - Test thành công
- Tất cả modules import được
- Classes khởi tạo thành công
- Logic transformation hoạt động
- API connections thành công
- Dữ liệu được lấy về

### ❌ FAIL - Test thất bại
- Lỗi import modules
- Lỗi khởi tạo classes
- Lỗi logic transformation
- Lỗi API connections
- Không lấy được dữ liệu

### ⚠️ WARNING - Cảnh báo
- API keys chưa được cấu hình
- Kết nối API chậm
- Dữ liệu trả về ít hơn mong đợi

## 🚀 CI/CD Integration

Script `test_dag_functionality.py` được tích hợp vào CI/CD pipeline và chạy tự động khi:
- Push code lên main branch
- Tạo Pull Request
- Merge code

## 🔍 Troubleshooting

### Lỗi Import
```bash
# Kiểm tra Python path
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Cài đặt dependencies
pip install -r requirements.txt
```

### Lỗi API Connection
```bash
# Kiểm tra file .env
cat .env

# Kiểm tra API keys
python -c "from config.settings import settings; print(settings.misa_api_url)"
```

### Lỗi Database Connection
```bash
# Kiểm tra SQL Server
docker-compose ps sqlserver

# Kiểm tra connection string
python -c "from src.utils.database import DatabaseManager; print(DatabaseManager().get_connection_string())"
```

## 📝 Logs

Tất cả script test đều ghi logs chi tiết:
- Timestamp
- Log level (INFO, WARNING, ERROR)
- Test results
- Error details

Logs được hiển thị trên console và có thể redirect vào file:
```bash
python scripts/test_dag_functionality.py > test_results.log 2>&1
```
