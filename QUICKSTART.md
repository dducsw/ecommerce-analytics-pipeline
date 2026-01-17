# Quick Start Guide

## Khởi động nhanh

### 1. Build và khởi động services

```bash
# Build images
docker-compose build

# Khởi tạo Airflow
docker-compose up airflow-init

# Khởi động tất cả services
docker-compose up -d
```

### 2. Truy cập các services

- **Airflow UI**: http://localhost:8080
  - Username: `airflow`
  - Password: `airflow`

### 3. Kiểm tra trạng thái

```bash
# Xem tất cả containers
docker-compose ps

# Xem logs
docker-compose logs -f
```

### 4. Test dbt connection

```bash
docker-compose exec dbt dbt debug
```

## Các lệnh thường dùng

### Docker Compose

```bash
# Khởi động
docker-compose up -d

# Dừng
docker-compose down

# Xem logs
docker-compose logs -f [service-name]

# Restart service
docker-compose restart [service-name]

# Rebuild image
docker-compose build [service-name]
```

### DBT Commands

```bash
# Vào dbt container
docker-compose exec dbt bash

# Chạy models
docker-compose exec dbt dbt run

# Chạy tests
docker-compose exec dbt dbt test

# Compile models
docker-compose exec dbt dbt compile
```

### Database Commands

```bash
# Connect to PostgreSQL
docker-compose exec postgres psql -U airflow -d airflow

# List databases
\l

# List tables
\dt

# Describe table
\d table_name

# Exit
\q
```

## Troubleshooting

### Reset toàn bộ hệ thống

```bash
docker-compose down -v
docker-compose up airflow-init
docker-compose up -d
```

### Xem logs chi tiết

```bash
# Airflow scheduler
docker-compose logs -f airflow-scheduler

# Airflow worker
docker-compose logs -f airflow-worker

# PostgreSQL
docker-compose logs -f postgres
```
