import runpod
import subprocess

def handler(job):
    print(f"✅ Received job input: {job['input']}")
    
    # 從 RunPod job input 拿參數，並給預設值
    start_date = job['input'].get('start_date', '20250701')
    end_date = job['input'].get('end_date', '20250701')

    try:
        subprocess.run([
            "python3", "download_translate_data.py",
            "--start", start_date,
            "--end", end_date
        ], check=True)
        return {"status": "success", "start_date": start_date, "end_date": end_date}
    except subprocess.CalledProcessError as e:
        return {"status": "failed", "error": str(e)}

runpod.serverless.start({"handler": handler})
