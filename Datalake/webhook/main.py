from fastapi import FastAPI, Request
import subprocess
import logging
from urllib.parse import unquote
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("WEBHOOK-GATEKEEPER")
#khởi tạo api
app = FastAPI()
BUCKET = "crawldata"

#Global lock chống chạy song song
etl_lock = threading.Lock()

#Hàm chạy ETL
def run_etl_logic(file_path: str):
    logger.info(f"🚀 ETL START: {file_path}")

    result = subprocess.run(
        ["python", "/etc/nav_etl.py", file_path],
        capture_output=True,
        text=True
    )

    if result.stdout:
        logger.info(result.stdout)

    if result.stderr:
        logger.error(result.stderr)

    logger.info("✅ ETL FINISHED")

#Endpoint xử lý event
@app.post("/minio-event")
async def handle_event(request: Request):
    data = await request.json()

    try:
        record = data["Records"][0]
        obj_key = unquote(record["s3"]["object"]["key"])
    except Exception:
        logger.warning("Invalid event format")
        return {"status": "invalid"}

    # chỉ xử lý folder data_nav
    if not obj_key.startswith("data_nav/"):
        return {"status": "ignored"}

    # chỉ xử lý csv / parquet
    if not obj_key.endswith((".csv", ".parquet")):
        logger.info("Unsupported file type")
        return {"status": "ignored"}

    full_path = f"{BUCKET}/{obj_key}"
    logger.info(f"📥 File received: {full_path}")

    # 🚫 Nếu đang chạy ETL khác → từ chối
    if not etl_lock.acquire(blocking=False):
        logger.warning("⚠ ETL already running. Rejecting file.")
        return {"status": "busy"}

    try:
        run_etl_logic(full_path)
    finally:
        etl_lock.release()

    return {"status": "processed"}