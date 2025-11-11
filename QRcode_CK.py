import requests
from datetime import datetime

LARK_CONFIG = {
    "app_id": "cli_a8620f964a38d02f",
    "app_secret": "G3FdlSvmTAXZYX8SBZtfpckHUiWUCO4h",
    "base_token": "GI8Ubcp0BaTn9PsY1xbl5zMagJb",
    "table_id": "tblB5MS7TOcNX1Hi"
}

def get_lark_tenant_access_token(app_id, app_secret):
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

def download_and_upload_qr(base_token, access_token, qr_url, filename):
    try:
        response = requests.get(qr_url, timeout=10)
        if response.status_code != 200:
            return None
        
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
        
        return result.get("data", {}).get("file_token") if result.get("code") == 0 else None
    except Exception as e:
        print(f"Lỗi xử lý QR: {e}")
        return None

def update_qr_code_for_records(base_token, table_id, access_token, records):
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base_token}/tables/{table_id}/records/batch_update"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    
    records_to_update = []
    
    for record in records:
        record_id = record.get("record_id")
        fields = record.get("fields", {})
        qr_link_field = fields.get("Link QR")
        
        if not qr_link_field or not record_id or fields.get("QR code"):
            continue
        
        qr_link = None
        if isinstance(qr_link_field, list) and len(qr_link_field) > 0:
            qr_link = qr_link_field[0].get("text")
        elif isinstance(qr_link_field, str):
            qr_link = qr_link_field
        
        if not qr_link:
            continue
        
        depot_id = fields.get("Mã cửa hàng", "unknown")
        depot_name = fields.get("Tên cửa hàng", "unknown")
        date_timestamp = fields.get("Ngày")
        
        date_str = datetime.fromtimestamp(date_timestamp / 1000).strftime("%Y-%m-%d") if date_timestamp else "unknown"
        safe_depot_name = depot_name.replace("/", "-").replace("\\", "-").replace(" ", "_")[:30]
        filename = f"{depot_id}_{safe_depot_name}_{date_str}.png"
        
        print(f"  {filename}")
        
        file_token = download_and_upload_qr(base_token, access_token, qr_link, filename)
        
        if file_token:
            records_to_update.append({
                "record_id": record_id,
                "fields": {"QR code": [{"file_token": file_token}]}
            })
    
    if not records_to_update:
        print("Không có record cần update")
        return 0
    
    total_updated = 0
    for i in range(0, len(records_to_update), 500):
        batch = records_to_update[i:i+500]
        try:
            response = requests.post(url, json={"records": batch}, headers=headers)
            response.raise_for_status()
            result = response.json()
            
            if result.get("code") == 0:
                total_updated += len(result.get("data", {}).get("records", []))
                print(f"  Batch {i//500 + 1}: OK")
            else:
                print(f"Batch {i//500 + 1}: {result.get('msg')}")
        except Exception as e:
            print(f"Lỗi batch {i//500 + 1}: {e}")
    
    return total_updated

def main():
    print("\nBắt đầu xử lý QR code\n")
    
    access_token = get_lark_tenant_access_token(LARK_CONFIG['app_id'], LARK_CONFIG['app_secret'])
    if not access_token:
        return
    
    records = get_all_records_from_lark(
        LARK_CONFIG['base_token'],
        LARK_CONFIG['table_id'],
        access_token
    )
    
    if not records:
        return
    
    total_updated = update_qr_code_for_records(
        LARK_CONFIG['base_token'],
        LARK_CONFIG['table_id'],
        access_token,
        records
    )
    
    print(f"\nHoàn thành: {total_updated} records")

if __name__ == "__main__":
    main()

