# 使用官方 Python 映像檔
FROM python:3.11-slim

# 設定工作目錄
WORKDIR /app

# 複製本機所有檔案到容器內
COPY . .

# 安裝 Python 套件
RUN pip install --no-cache-dir -r requirements.txt

# 啟動主程式
CMD ["python", "dw_data.py"]
