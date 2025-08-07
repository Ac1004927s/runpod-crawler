import os
import pandas as pd
from datetime import datetime, timedelta

# 模擬：這裡請改成你的實際資料下載邏輯
def download_data_for_date(date_str):
    print(f"📥 模擬下載資料: {date_str}")
    # 假設資料是個簡單 DataFrame
    return pd.DataFrame({
        "date": [date_str] * 3,
        "value": [10, 20, 30]
    })

# 設定日期範圍（可自訂）
start_date = "20250801"
end_date = "20250807"

# 轉換為 datetime 物件
start_dt = datetime.strptime(start_date, "%Y%m%d")
end_dt = datetime.strptime(end_date, "%Y%m%d")

# 跑每一天
curr_dt = start_dt
while curr_dt <= end_dt:
    date_str = curr_dt.strftime("%Y%m%d")
    output_path = f"/workspace/{date_str}.csv"

    if os.path.exists(output_path):
        print(f"✅ 已存在 {output_path}，略過")
    else:
        print(f"🚀 處理 {date_str} ...")
        df = download_data_for_date(date_str)
        df.to_csv(output_path, index=False)
        print(f"✅ 儲存完成: {output_path}")

    curr_dt += timedelta(days=1)

print("🎉 所有日期處理完畢")
