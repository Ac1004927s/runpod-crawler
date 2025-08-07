
import runpod
import subprocess

def handler(job):
    print(f"✅ Received job input: {job['input']}")
    try:
        subprocess.run(["python3", "dw_data.py"], check=True)
        return {"status": "success"}
    except subprocess.CalledProcessError as e:
        return {"status": "failed", "error": str(e)}

runpod.serverless.start({"handler": handler})
