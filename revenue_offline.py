from google.cloud import bigquery
import pandas as pd
import requests
from datetime import datetime, timedelta
import os

PROJECT_ID = "atino-vietnam"
DATASET_ID = "P_and_L"
TABLE_ID = "Bills_revenue"

LARK_CONFIG = {
    "app_id": os.getenv("LARK_APP_ID", "cli_a8620f964a38d02f"),
    "app_secret": os.getenv("LARK_APP_SECRET", "G3FdlSvmTAXZYX8SBZtfpckHUiWUCO4h"),
    "base_token": os.getenv("LARK_BASE_TOKEN", "GI8Ubcp0BaTn9PsY1xbl5zMagJb"),
    "table_id": os.getenv("LARK_TABLE_ID", "tblB5MS7TOcNX1Hi")
}

def connect_bigquery():
    try:
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        print(f"Lỗi kết nối BigQuery: {e}")
        return None

def get_revenue_data(client, target_date):
    """Query dữ liệu với logic CTE mới"""
    query = f"""
    WITH CTE AS (
      SELECT 
        CASE
          WHEN b.mode = '1' AND b.depotid IN (
            "142681", "209838", "94204", "218273", "209951", "79564", "214102", "197220", 
            "52449", "6526", "216457", "217598", "218022", "204337", "54227", "210003", 
            "216868", "210175", "207514", "170506", "9538", "177163", "170505", "213201", 
            "213200", "218047", "169822", "222250"
          ) THEN "88888888"
          WHEN b.mode = '1' AND b.depotid = "128940" THEN "102701"
          ELSE b.depotid
        END AS depotid,
        b.date,
        b.type,
        b.mode,
        CASE 
          WHEN ROW_NUMBER() OVER(PARTITION BY b.id ORDER BY i.id) = 1
          THEN b.money
        END AS money,
        CASE 
          WHEN ROW_NUMBER() OVER(PARTITION BY b.id ORDER BY i.id) = 1
          THEN b.returnFee
        END AS returnFee,
        i.avgCost,
        i.quantity,
        IFNULL(i.avgCost, 0) * IFNULL(i.quantity, 0) AS total_cost
      FROM `{PROJECT_ID}.nhanhVN.FACT_bills` AS b
      LEFT JOIN `{PROJECT_ID}.nhanhVN.FACT_imexs` AS i
        ON b.id = i.billid
      WHERE b.date = "{target_date}"
    )
    SELECT 
      depotid,
      date,
      type,
      mode,
      SUM(money) AS total_money,
      SUM(returnFee) AS total_returnfee,
      SUM(total_cost) AS total_cost
    FROM CTE
    GROUP BY 
      depotid,
      date,
      type,
      mode
    """
    
    try:
        df = client.query(query).to_dataframe()
        print(f"  Query trả về {len(df)} dòng")
        if not df.empty:
            print(f"  Columns: {df.columns.tolist()}")
        return df
    except Exception as e:
        print(f"Lỗi query ngày {target_date}: {e}")
        return None

def calculate_daily_revenue(df):
    """Tính doanh thu theo depotId, chỉ lấy mode = '2'"""
    # Filter mode = '2'
    df_filtered = df[df['mode'] == '2'].copy()
    
    if df_filtered.empty:
        print("  Không có dữ liệu với mode = '2'")
        return pd.DataFrame(columns=['depotId', 'money_type1', 'money_type2', 
                                     'returnfee_type1', 'total_cost', 'daily_revenue'])
    
    # Convert data types
    df_filtered['type'] = df_filtered['type'].astype(int)
    df_filtered['total_money'] = pd.to_numeric(df_filtered['total_money'], errors='coerce').fillna(0)
    df_filtered['total_returnfee'] = pd.to_numeric(df_filtered['total_returnfee'], errors='coerce').fillna(0)
    df_filtered['total_cost'] = pd.to_numeric(df_filtered['total_cost'], errors='coerce').fillna(0)
    
    # Tách type 1 và type 2
    type1_df = df_filtered[df_filtered['type'] == 1][['depotid', 'total_money', 'total_returnfee']].copy()
    type1_df.columns = ['depotId', 'money_type1', 'returnfee_type1']
    
    type2_df = df_filtered[df_filtered['type'] == 2][['depotid', 'total_money', 'total_returnfee', 'total_cost']].copy()
    type2_df.columns = ['depotId', 'money_type2', 'returnfee_type2', 'total_cost']
    
    # Merge hai loại
    result_df = pd.merge(
        type2_df[['depotId', 'money_type2', 'total_cost']],
        type1_df[['depotId', 'money_type1', 'returnfee_type1']],
        on='depotId',
        how='outer'
    ).fillna(0)
    
    # Tính doanh thu
    result_df['daily_revenue'] = (
        result_df['money_type2'] - 
        result_df['money_type1'] + 
        result_df['returnfee_type1']
    )
    
    # Làm tròn các cột số
    for col in ['daily_revenue', 'money_type1', 'money_type2', 'returnfee_type1', 'total_cost']:
        result_df[col] = result_df[col].round(0).astype(int)
    
    return result_df

def get_lark_tenant_access_token(app_id, app_secret):
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": app_id, "app_secret": app_secret}
    
    try:
        response = requests.post(url, json=payload, headers={"Content-Type": "application/json"})
        response.raise_for_status()
        result = response.json()
        return result.get("tenant_access_token") if result.get("code") == 0 else None
    except Exception as e:
        print(f"Lỗi kết nối Lark API: {e}")
        return None

def get_existing_records(base_token, table_id, access_token, target_date):
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base_token}/tables/{table_id}/records"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    timestamp = int(datetime.strptime(target_date, "%Y-%m-%d").timestamp() * 1000)
    
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
                return {}
        
        record_dict = {}
        for record in all_records:
            fields = record.get("fields", {})
            if fields.get("Ngày") == timestamp:
                depot_id = fields.get("Mã cửa hàng")
                record_id = record.get("record_id")
                if depot_id and record_id:
                    record_dict[depot_id] = record_id
        
        return record_dict
    
    except Exception as e:
        print(f"Lỗi lấy records: {e}")
        return {}

def update_lark_records(base_token, table_id, access_token, records_to_update):
    if not records_to_update:
        return 0
    
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base_token}/tables/{table_id}/records/batch_update"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    total_updated = 0
    
    for i in range(0, len(records_to_update), 500):
        batch = records_to_update[i:i+500]
        try:
            response = requests.post(url, json={"records": batch}, headers=headers)
            response.raise_for_status()
            result = response.json()
            
            if result.get("code") == 0:
                total_updated += len(result.get("data", {}).get("records", []))
            else:
                print(f"Lỗi update records: {result.get('msg')}")
                return total_updated
        except Exception as e:
            print(f"Lỗi kết nối Lark API: {e}")
            return total_updated
    
    return total_updated

def create_lark_records(base_token, table_id, access_token, records_to_create):
    if not records_to_create:
        return 0
    
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base_token}/tables/{table_id}/records/batch_create"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    total_created = 0
    
    for i in range(0, len(records_to_create), 500):
        batch = records_to_create[i:i+500]
        try:
            response = requests.post(url, json={"records": batch}, headers=headers)
            response.raise_for_status()
            result = response.json()
            
            if result.get("code") == 0:
                total_created += len(result.get("data", {}).get("records", []))
            else:
                print(f"Lỗi create records: {result.get('msg')}")
                return total_created
        except Exception as e:
            print(f"Lỗi kết nối Lark API: {e}")
            return total_created
    
    return total_created

def upsert_data_for_date(base_token, table_id, access_token, df, target_date):
    print(f"Xử lý ngày {target_date}...")
    
    existing_records = get_existing_records(base_token, table_id, access_token, target_date)
    timestamp = int(datetime.strptime(target_date, "%Y-%m-%d").timestamp() * 1000)
    
    records_to_update = []
    records_to_create = []
    
    for _, row in df.iterrows():
        depot_id = str(row['depotId'])
        fields = {
            "Ngày": timestamp,
            "Mã cửa hàng": depot_id,
            "Doanh thu Type 1": int(row['money_type1']),
            "Doanh thu Type 2": int(row['money_type2']),
            "Phí hoàn trả Type 1": int(row['returnfee_type1']),
            "Chi phí": int(row['total_cost']),
            "Doanh thu": int(row['daily_revenue'])
        }
        
        if depot_id in existing_records:
            records_to_update.append({"record_id": existing_records[depot_id], "fields": fields})
        else:
            records_to_create.append({"fields": fields})
    
    updated_count = 0
    if records_to_update:
        print(f"  Update {len(records_to_update)} records...")
        updated_count = update_lark_records(base_token, table_id, access_token, records_to_update)
        print(f"  Đã update {updated_count} records")
    
    created_count = 0
    if records_to_create:
        print(f"  Create {len(records_to_create)} records...")
        created_count = create_lark_records(base_token, table_id, access_token, records_to_create)
        print(f"  Đã create {created_count} records")
    
    return (updated_count + created_count) > 0

def main():
    print("\nBắt đầu cập nhật 2 ngày gần nhất\n")
    
    client = connect_bigquery()
    if not client:
        return
    
    access_token = get_lark_tenant_access_token(LARK_CONFIG['app_id'], LARK_CONFIG['app_secret'])
    if not access_token:
        print("Không thể lấy access token")
        return
    
    today = datetime.now()
    dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(0, 2)]
    
    success_count = 0
    fail_count = 0
    
    for target_date in dates:
        print(f"\n{'='*60}")
        print(f"Đang xử lý: {target_date}")
        print('='*60)
        
        df = get_revenue_data(client, target_date)
        
        if df is None or df.empty:
            print(f"  ❌ Không có dữ liệu\n")
            fail_count += 1
            continue
        
        result_df = calculate_daily_revenue(df)
        
        if result_df.empty:
            print(f"  ❌ Không có dữ liệu mode=2\n")
            fail_count += 1
            continue
        
        success = upsert_data_for_date(
            LARK_CONFIG['base_token'],
            LARK_CONFIG['table_id'],
            access_token,
            result_df,
            target_date
        )
        
        if success:
            success_count += 1
            print(f"  ✅ Tổng doanh thu: {result_df['daily_revenue'].sum():,} VNĐ")
            print(f"  💰 Tổng chi phí: {result_df['total_cost'].sum():,} VNĐ")
        else:
            fail_count += 1
            print(f"  ❌ Cập nhật thất bại")
    
    print("\n" + "="*60)
    print(f"KẾT QUẢ: {success_count} ngày thành công", end="")
    if fail_count > 0:
        print(f", {fail_count} ngày thất bại")
    else:
        print()
    print("="*60 + "\n")

if __name__ == "__main__":
    main()
