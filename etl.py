import json
import pandas as pd
import os
import zipfile

# 取出 cur.zip 資料，導入成 dataframe
data_path = "./data/cur.zip"
df = pd.read_csv(data_path, low_memory=False)  # 設定 low_memory=False 因為會有資料型別不同的警告

# 讀取 fix.json 資料
fix_json_path = "./data/fix.json"
with open(fix_json_path, 'r') as f:
    fix_data = json.load(f)

# 利用 UsageAccountId 來 group by
groups = df.groupby('lineItem/UsageAccountId')

for usage_account_id, group in groups:
    # 判斷滿足條件的欄位位置
    mask = (group['product/ProductName'] == "Amazon CloudFront") & (group['lineItem/LineItemType'] == "Usage")
    rows_to_update = group[mask].index

    if rows_to_update.empty:
        continue

    updated_rate = None
    for entry in fix_data:
        if int(entry['lineItem/UsageAccountId']) == usage_account_id:
            updated_rate = entry['lineItem/UnblendedRate']
            break

    # 取代原本資料
    if updated_rate is not None:
        group.loc[rows_to_update, 'lineItem/UnblendedRate'] = updated_rate
        group.loc[rows_to_update, 'lineItem/LineItemDescription'] = f"${updated_rate} per GB data transfer out (Europe)"
        group['lineItem/UnblendedCost'] = group['lineItem/UsageAmount'] * group['lineItem/UnblendedRate']

    # 創建 ouput 資料夾
    output_folder = "./output"
    os.makedirs(output_folder, exist_ok=True)

    # 存成 csv 檔
    output_file = os.path.join(output_folder, f"{usage_account_id}.csv")
    group.to_csv(output_file, index=False)

    # 將 csv 檔轉成 zip
    zip_file = os.path.join(output_folder, f"{usage_account_id}.zip")
    with zipfile.ZipFile(zip_file, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(output_file, os.path.basename(output_file))

    # 移除 csv 檔，只保留 zip 檔
    os.remove(output_file)

print("Data processing complete.")
