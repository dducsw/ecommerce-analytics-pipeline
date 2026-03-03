# Postgres to BigQuery Data Loader

Script để load dữ liệu từ Postgres sang BigQuery.

## Setup

1. **Cài đặt dependencies:**
```bash
pip install -r scripts/requirements.txt
```

2. **Cấu hình:**

Mở file `scripts/load_to_bigquery.py` và chỉnh sửa:

```python
# BigQuery configuration
GCP_KEY_PATH = 'credentials/gcp-key.json'  # Path to your service account key
BQ_PROJECT_ID = 'your-project-id'          # Your GCP project ID
BQ_DATASET_ID = 'ecommerce_analytics'      # Your BigQuery dataset name
```

3. **Đảm bảo Postgres đang chạy:**
```bash
docker compose up -d postgres
```

## Usage

### Load bảng users (lần đầu tiên):

```bash
# Tạo dataset và load bảng users
python scripts/load_to_bigquery.py --table users --create-dataset
```

### Load các bảng khác:

```bash
# Load orders
python scripts/load_to_bigquery.py --table orders

# Load order_items
python scripts/load_to_bigquery.py --table order_items

# Load products
python scripts/load_to_bigquery.py --table products

# Load events
python scripts/load_to_bigquery.py --table events

# Load inventory_items
python scripts/load_to_bigquery.py --table inventory_items

# Load distribution_centers
python scripts/load_to_bigquery.py --table distribution_centers
```

### Load modes:

```bash
# WRITE_TRUNCATE: Xóa dữ liệu cũ và ghi mới (mặc định)
python scripts/load_to_bigquery.py --table users --mode WRITE_TRUNCATE

# WRITE_APPEND: Thêm vào dữ liệu hiện có
python scripts/load_to_bigquery.py --table users --mode WRITE_APPEND

# WRITE_EMPTY: Chỉ ghi nếu bảng trống (fail nếu có dữ liệu)
python scripts/load_to_bigquery.py --table users --mode WRITE_EMPTY
```

## Bảng được hỗ trợ

Script hỗ trợ load các bảng staging sau:
- `users` → `stg_users`
- `orders` → `stg_orders`
- `order_items` → `stg_order_items`
- `products` → `stg_products`
- `events` → `stg_events`
- `inventory_items` → `stg_inventory_items`
- `distribution_centers` → `stg_distribution_centers`

## Load tất cả bảng

Tạo script batch để load tất cả:

**Windows (PowerShell):**
```powershell
# load_all.ps1
$tables = @("users", "orders", "order_items", "products", "events", "inventory_items", "distribution_centers")

foreach ($table in $tables) {
    Write-Host "Loading $table..." -ForegroundColor Green
    python scripts/load_to_bigquery.py --table $table
}
```

**Linux/Mac (Bash):**
```bash
# load_all.sh
#!/bin/bash
tables=("users" "orders" "order_items" "products" "events" "inventory_items" "distribution_centers")

for table in "${tables[@]}"; do
    echo "Loading $table..."
    python scripts/load_to_bigquery.py --table "$table"
done
```

## Troubleshooting

### Lỗi kết nối Postgres:
- Kiểm tra Postgres container đang chạy: `docker compose ps`
- Kiểm tra credentials trong `POSTGRES_CONFIG`

### Lỗi BigQuery authentication:
- Kiểm tra file `credentials/gcp-key.json` tồn tại
- Kiểm tra service account có quyền BigQuery Data Editor

### Lỗi dataset không tồn tại:
- Thêm flag `--create-dataset` khi chạy lần đầu

### Lỗi schema:
- Script tự động detect schema, nhưng có thể cần adjust data types
- Kiểm tra dữ liệu null/NaN trong Postgres

## Notes

- Script sử dụng pandas để đọc và load dữ liệu
- BigQuery auto-detect schema từ DataFrame
- Mặc định write mode là `WRITE_TRUNCATE` (replace data)
- Dữ liệu được đọc từ staging views trong Postgres
