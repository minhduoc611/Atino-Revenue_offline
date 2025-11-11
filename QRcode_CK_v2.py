import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time

LARK_CONFIG = {
    "app_id": "cli_a8620f964a38d02f",
    "app_secret": "G3FdlSvmTAXZYX8SBZtfpckHUiWUCO4h",
    "base_token": "GI8Ubcp0BaTn9PsY1xbl5zMagJb",
    "table_id": "tblB5MS7TOcNX1Hi"
}

# Constants
MAX_WORKERS = 10  # Số luồng download đồng thời
BATCH_SIZE = 500  # Số records update mỗi batch
DOWNLOAD_TIMEOUT = 15  # Timeout cho mỗi download (seconds)
MAX_RETRIES = 3  # Số lần retry khi download fail

# Thread-safe counter và lock
progress_lock = Lock()
progress_data = {"processed": 0, "success": 0, "failed": 0}


def get_lark_tenant_access_token(app_id, app_secret):
    """Lấy access token từ Lark API"""
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": app_id, "app_secret": app_secret}

    try:
        response = requests.post(url, json=payload, headers={"Content-Type": "application/json"})
        response.raise_for_status()
        result = response.json()
        return result.get("tenant_access_token") if result.get("code") == 0 else None
    except Exception as e:
        print(f"Lỗi lấy token: {e}")
        return None


def get_all_records_from_lark(base_token, table_id, access_token):
    """Lấy tất cả records từ Lark table với pagination"""
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base_token}/tables/{table_id}/records"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    all_records = []
    page_token = None

    try:
        while True:
            params = {"page_size": 500}
            if page_token:
                params["page_token"] = page_token

            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            result = response.json()

            if result.get("code") == 0:
                all_records.extend(result.get("data", {}).get("items", []))
                if not result.get("data", {}).get("has_more", False):
                    break
                page_token = result.get("data", {}).get("page_token")
            else:
                print(f"Lỗi lấy records: {result.get('msg')}")
                return []

        print(f"Đã lấy {len(all_records)} records")
        return all_records
    except Exception as e:
        print(f"Lỗi kết nối: {e}")
        return []


def download_and_upload_qr_with_retry(base_token, access_token, qr_url, filename, max_retries=MAX_RETRIES):
    """Download QR và upload lên Lark với retry logic"""
    for attempt in range(max_retries):
        try:
            # Download QR code
            response = requests.get(qr_url, timeout=DOWNLOAD_TIMEOUT)
            if response.status_code != 200:
                if attempt < max_retries - 1:
                    time.sleep(1 * (attempt + 1))  # Exponential backoff
                    continue
                return None

            # Upload to Lark Drive
            url = f"https://open.larksuite.com/open-apis/drive/v1/medias/upload_all"
            headers = {"Authorization": f"Bearer {access_token}"}

            files = {'file': (filename, response.content, 'image/png')}
            data = {
                'file_name': filename,
                'parent_type': 'bitable_image',
                'parent_node': base_token,
                'size': str(len(response.content))
            }

            upload_response = requests.post(url, headers=headers, files=files, data=data)
            result = upload_response.json()

            if result.get("code") == 0:
                return result.get("data", {}).get("file_token")
            elif attempt < max_retries - 1:
                time.sleep(1 * (attempt + 1))
                continue
            else:
                return None

        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                print(f"  Timeout {filename}, retry {attempt + 1}/{max_retries}")
                time.sleep(1 * (attempt + 1))
                continue
            else:
                print(f"  Timeout {filename} sau {max_retries} lần thử")
                return None
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(1 * (attempt + 1))
                continue
            else:
                print(f"  Lỗi {filename}: {e}")
                return None

    return None


def process_single_record(record, base_token, access_token, total_records):
    """Xử lý 1 record - được gọi bởi ThreadPoolExecutor"""
    record_id = record.get("record_id")
    fields = record.get("fields", {})
    qr_link_field = fields.get("Link QR")

    # Skip nếu không hợp lệ (BỎ CHECK QR code đã tồn tại)
    if not qr_link_field or not record_id:
        return None

    # Parse QR link
    qr_link = None
    if isinstance(qr_link_field, list) and len(qr_link_field) > 0:
        qr_link = qr_link_field[0].get("text")
    elif isinstance(qr_link_field, str):
        qr_link = qr_link_field

    if not qr_link:
        return None

    # Tạo filename
    depot_id = fields.get("Mã cửa hàng", "unknown")
    depot_name = fields.get("Tên cửa hàng", "unknown")
    date_timestamp = fields.get("Ngày")

    date_str = datetime.fromtimestamp(date_timestamp / 1000).strftime("%Y-%m-%d") if date_timestamp else "unknown"
    safe_depot_name = depot_name.replace("/", "-").replace("\\", "-").replace(" ", "_")[:30]
    filename = f"{depot_id}_{safe_depot_name}_{date_str}.png"

    # Download và upload
    file_token = download_and_upload_qr_with_retry(base_token, access_token, qr_link, filename)

    # Update progress (thread-safe)
    with progress_lock:
        progress_data["processed"] += 1
        if file_token:
            progress_data["success"] += 1
            status = "✓"
        else:
            progress_data["failed"] += 1
            status = "✗"

        # In progress
        print(f"  [{progress_data['processed']}/{total_records}] {status} {filename}")

    if file_token:
        return {
            "record_id": record_id,
            "fields": {"QR code": [{"file_token": file_token}]}
        }

    return None


def process_records_concurrently(base_token, access_token, records):
    """Xử lý nhiều records đồng thời bằng ThreadPoolExecutor"""
    # Filter records cần xử lý (BỎ CHECK QR code đã tồn tại)
    records_to_process = []
    for record in records:
        fields = record.get("fields", {})
        qr_link_field = fields.get("Link QR")
        record_id = record.get("record_id")

        # Chỉ cần có Link QR và record_id là xử lý
        if qr_link_field and record_id:
            records_to_process.append(record)

    if not records_to_process:
        print("Không có record cần xử lý")
        return []

    print(f"\nBắt đầu xử lý {len(records_to_process)} records với {MAX_WORKERS} workers\n")

    # Reset progress
    global progress_data
    progress_data = {"processed": 0, "success": 0, "failed": 0}

    records_to_update = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        future_to_record = {
            executor.submit(process_single_record, record, base_token, access_token, len(records_to_process)): record
            for record in records_to_process
        }

        # Collect results as they complete
        for future in as_completed(future_to_record):
            try:
                result = future.result()
                if result:
                    records_to_update.append(result)
            except Exception as e:
                print(f"  Lỗi khi xử lý record: {e}")

    print(f"\n{'='*60}")
    print(f"Hoàn thành download: {progress_data['success']} thành công, {progress_data['failed']} thất bại")
    print(f"{'='*60}\n")

    return records_to_update


def update_lark_records_batch(base_token, table_id, access_token, records_to_update):
    """Update records lên Lark theo batch"""
    if not records_to_update:
        print("Không có record cần update")
        return 0

    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base_token}/tables/{table_id}/records/batch_update"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    total_updated = 0
    total_batches = (len(records_to_update) + BATCH_SIZE - 1) // BATCH_SIZE

    print(f"Bắt đầu update {len(records_to_update)} records ({total_batches} batches)\n")

    for i in range(0, len(records_to_update), BATCH_SIZE):
        batch = records_to_update[i:i+BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1

        try:
            response = requests.post(url, json={"records": batch}, headers=headers)
            response.raise_for_status()
            result = response.json()

            if result.get("code") == 0:
                updated_count = len(result.get("data", {}).get("records", []))
                total_updated += updated_count
                print(f"  Batch {batch_num}/{total_batches}: ✓ Updated {updated_count} records")
            else:
                print(f"  Batch {batch_num}/{total_batches}: ✗ {result.get('msg')}")
        except Exception as e:
            print(f"  Batch {batch_num}/{total_batches}: ✗ Lỗi: {e}")

    return total_updated


def main():
    """Main function"""
    start_time = time.time()

    print("\n" + "="*60)
    print("BẮT ĐẦU XỬ LÝ QR CODE (CONCURRENT VERSION)")
    print("="*60 + "\n")

    # Lấy access token
    print("Đang lấy access token...")
    access_token = get_lark_tenant_access_token(LARK_CONFIG['app_id'], LARK_CONFIG['app_secret'])
    if not access_token:
        print("✗ Không thể lấy access token")
        return
    print("✓ Đã lấy access token\n")

    # Lấy tất cả records
    print("Đang lấy records từ Lark...")
    records = get_all_records_from_lark(
        LARK_CONFIG['base_token'],
        LARK_CONFIG['table_id'],
        access_token
    )

    if not records:
        print("✗ Không có records")
        return
    print(f"✓ Đã lấy {len(records)} records\n")

    # Xử lý records đồng thời
    records_to_update = process_records_concurrently(
        LARK_CONFIG['base_token'],
        access_token,
        records
    )

    # Update lên Lark
    if records_to_update:
        total_updated = update_lark_records_batch(
            LARK_CONFIG['base_token'],
            LARK_CONFIG['table_id'],
            access_token,
            records_to_update
        )
    else:
        total_updated = 0

    # Summary
    elapsed_time = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"HOÀN THÀNH")
    print(f"{'='*60}")
    print(f"  Tổng records đã update: {total_updated}")
    print(f"  Thời gian xử lý: {elapsed_time:.2f}s")
    print(f"  Tốc độ trung bình: {total_updated/elapsed_time:.2f} records/s" if elapsed_time > 0 else "  N/A")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()