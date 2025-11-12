## Hướng dẫn chạy (ngắn gọn)

### 2) Khởi động hệ thống bằng Docker

```bash
docker-compose up -d
docker-compose ps
# (tuỳ chọn) theo dõi log webserver
docker-compose logs -f airflow-webserver
```

Truy cập Airflow UI: http://localhost:8080
- Username: admin
- Password: facolos2024 (mặc định trong docker-compose.yml)

### 3) Chạy DAGs qua UI
1) Mở Airflow UI → tìm DAG cần chạy
2) Bật (Unpause) DAG nếu đang tắt
3) Nhấn “Trigger DAG” để chạy

### 4) Chạy DAGs qua CLI (tuỳ chọn)

```bash
# Liệt kê DAGs
docker-compose exec airflow-webserver airflow dags list

# Full load (chạy 1 lần đầu)
docker-compose exec airflow-webserver airflow dags unpause full_load_etl_dag
docker-compose exec airflow-webserver airflow dags trigger full_load_etl_dag

# Incremental (lịch mặc định 15 phút)
docker-compose exec airflow-webserver airflow dags unpause incremental_etl_dag
# (tuỳ chọn) chạy ngay một lần
docker-compose exec airflow-webserver airflow dags trigger incremental_etl_dag
```

### 5) Dừng hệ thống

```bash
docker-compose down
# (tuỳ chọn) xoá volumes dữ liệu
docker-compose down -v
```


