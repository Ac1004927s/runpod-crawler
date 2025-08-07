import os
import time
import gzip
import shutil
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from tqdm import tqdm
from urllib.parse import urljoin
from datetime import datetime
import urllib3
import argparse

# 忽略 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 加入 argparse 解析命令列參數
parser = argparse.ArgumentParser()
parser.add_argument('--start', type=str, required=True, help='Start date in YYYYMMDD')
parser.add_argument('--end', type=str, required=True, help='End date in YYYYMMDD')
args = parser.parse_args()

# 設定下載範圍與路徑
BASE_URL = 'https://tisvcloud.freeway.gov.tw/history/motc20/VD/'
# START_DATE = datetime.strptime("20250627", "%Y%m%d")
# END_DATE = datetime.strptime("20250627", "%Y%m%d")
START_DATE = datetime.strptime(args.start, "%Y%m%d")
END_DATE = datetime.strptime(args.end, "%Y%m%d")

BASE_SAVE_DIR = os.path.join('data', 'dynamic_VD')

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def get_all_dates():
    try:
        resp = requests.get(BASE_URL, verify=False)
        soup = BeautifulSoup(resp.text, 'html.parser')
        links = soup.find_all('a')
        date_folders = [link.get('href') for link in links if link.get('href') and link.get('href').strip('/').isdigit()]
        filtered = []

        for folder in date_folders:
            try:
                date_str = folder.strip('/')
                date_obj = datetime.strptime(date_str, "%Y%m%d")
                if START_DATE <= date_obj <= END_DATE:
                    filtered.append(folder)
            except:
                continue
        return filtered
    except Exception as e:
        print(f"❌ 無法連線至主站點：{e}")
        return []

def download_files_for_date(date_folder):
    date_url = urljoin(BASE_URL, date_folder)
    try:
        resp = requests.get(date_url, verify=False)
        soup = BeautifulSoup(resp.text, 'html.parser')
        links = soup.find_all('a')

        # 只下載 VDLive_ 開頭的檔案
        file_links = [
            urljoin(date_url, link.get('href'))
            for link in links
            if link.get('href') and link.get('href').endswith('.xml.gz') and link.get('href').startswith('VDLive_')
        ]

        save_dir = os.path.join(BASE_SAVE_DIR, date_folder.strip('/'))
        ensure_dir(save_dir)

        for file_url in file_links:
            filename_gz = file_url.split('/')[-1]
            filepath_gz = os.path.join(save_dir, filename_gz)
            filepath_xml = filepath_gz[:-3]

            if not os.path.exists(filepath_xml):
                try:
                    with requests.get(file_url, stream=True, verify=False) as r:
                        r.raise_for_status()
                        with open(filepath_gz, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=8192):
                                f.write(chunk)

                    with gzip.open(filepath_gz, 'rb') as f_in, open(filepath_xml, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)

                    os.remove(filepath_gz)
                    time.sleep(0.1)

                except Exception as e:
                    print(f"❌ 下載失敗：{file_url}，錯誤：{e}")

    except Exception as e:
        print(f"❌ 無法讀取頁面：{date_url}，錯誤：{e}")

def parse_xml_to_df(xml_file):
    try:
        ns = {'ns': 'http://traffic.transportdata.tw/standard/traffic/schema/'}
        tree = ET.parse(xml_file)
        root = tree.getroot()
        records = []

        for vdlive in root.findall('.//ns:VDLive', ns):
            vd_id = vdlive.find('ns:VDID', ns).text if vdlive.find('ns:VDID', ns) is not None else ''
            collect_time = vdlive.find('ns:DataCollectTime', ns).text if vdlive.find('ns:DataCollectTime', ns) is not None else ''

            for linkflow in vdlive.findall('.//ns:LinkFlow', ns):
                link_id = linkflow.find('ns:LinkID', ns).text if linkflow.find('ns:LinkID', ns) is not None else ''

                for lane in linkflow.findall('.//ns:Lane', ns):
                    lane_id = lane.find('ns:LaneID', ns).text or ''
                    lane_type = lane.find('ns:LaneType', ns).text or ''
                    speed = lane.find('ns:Speed', ns).text or ''
                    occupancy = lane.find('ns:Occupancy', ns).text or ''

                    vehicle_data = {'S': {'Volume': '', 'Speed': ''},
                                    'L': {'Volume': '', 'Speed': ''},
                                    'T': {'Volume': '', 'Speed': ''}}

                    for vehicle in lane.findall('.//ns:Vehicle', ns):
                        vtype = vehicle.find('ns:VehicleType', ns).text
                        vol = vehicle.find('ns:Volume', ns).text
                        spd = vehicle.find('ns:Speed', ns).text
                        vehicle_data[vtype] = {'Volume': vol, 'Speed': spd}

                    record = {
                        'VDID': vd_id,
                        'LinkID': link_id,
                        'LaneID': lane_id,
                        'LaneType': lane_type,
                        'LaneSpeed': speed,
                        'Occupancy': occupancy,
                        'Vehicle_S_Volume': vehicle_data['S']['Volume'],
                        'Vehicle_S_Speed': vehicle_data['S']['Speed'],
                        'Vehicle_L_Volume': vehicle_data['L']['Volume'],
                        'Vehicle_L_Speed': vehicle_data['L']['Speed'],
                        'Vehicle_T_Volume': vehicle_data['T']['Volume'],
                        'Vehicle_T_Speed': vehicle_data['T']['Speed'],
                        'DataCollectTime': collect_time
                    }

                    records.append(record)

        return pd.DataFrame(records)

    except Exception as e:
        print(f"❌ 解析失敗：{xml_file}，錯誤：{e}")
        return pd.DataFrame()

def process_day_to_csv(date_folder):
    folder_path = os.path.join(BASE_SAVE_DIR, date_folder.strip('/'))
    all_xml_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.startswith('VDLive_') and f.endswith('.xml')]

    all_dfs = []
    for xml_file in all_xml_files:
        df = parse_xml_to_df(xml_file)
        if not df.empty:
            all_dfs.append(df)

    if all_dfs:
        day_df = pd.concat(all_dfs, ignore_index=True)
        csv_path = os.path.join(folder_path, f"{date_folder.strip('/')}.csv")
        day_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"✅ 已儲存：{csv_path}，共 {len(day_df)} 筆資料")

        # 刪除該日所有 XML 檔案
        for f in all_xml_files:
            os.remove(f)
        return day_df
    else:
        print(f"⚠ 無有效資料：{date_folder}")
        return pd.DataFrame()

def merge_all_csvs_to_one():
    all_data = []
    for date_folder in os.listdir(BASE_SAVE_DIR):
        day_path = os.path.join(BASE_SAVE_DIR, date_folder, f"{date_folder}.csv")
        if os.path.exists(day_path):
            df = pd.read_csv(day_path)
            all_data.append(df)
    if all_data:
        all_df = pd.concat(all_data, ignore_index=True)
        output_path = os.path.join(BASE_SAVE_DIR, 'VD_all_data.csv')
        all_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"\n📦 所有資料已合併為：{output_path}，共 {len(all_df)} 筆資料")

def main():
    all_dates = get_all_dates()
    print(f"📅 共找到 {len(all_dates)} 個符合的日期，開始下載與處理...")
    for date_folder in tqdm(all_dates):
        download_files_for_date(date_folder)
        process_day_to_csv(date_folder)
    merge_all_csvs_to_one()

if __name__ == '__main__':
    main()
