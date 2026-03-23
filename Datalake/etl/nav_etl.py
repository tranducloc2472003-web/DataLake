import pandas as pd
import s3fs
import trino
import sys
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("NAV_ETL")

# ==============================
# CONFIG
# ==============================

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio_access_key"
MINIO_SECRET_KEY = "minio_secret_key"

BUCKET = "crawldata"
STAGING_PREFIX = "warehouse/staging/nav"

TRINO_HOST = "trino"
TRINO_PORT = 8080
TRINO_USER = "admin"

STAGING_TABLE = "hive.default.nav_staging"
CURRENT_TABLE = "iceberg.ods.nav_current"
HISTORY_TABLE = "iceberg.ods.nav_history"

# ==============================

specific_file = sys.argv[1] if len(sys.argv) > 1 else None

if not specific_file:
    logger.info("No file passed")
    sys.exit(0)

logger.info(f"Loading: {specific_file}")

# ==============================
# READ FILE FROM MINIO
# ==============================

if specific_file.endswith(".csv"):
    df = pd.read_csv(
        f"s3://{specific_file}",
        storage_options={
            "key": MINIO_ACCESS_KEY,
            "secret": MINIO_SECRET_KEY,
            "client_kwargs": {"endpoint_url": MINIO_ENDPOINT},
        },
    )
else:
    df = pd.read_parquet(
        f"s3://{specific_file}",
        storage_options={
            "key": MINIO_ACCESS_KEY,
            "secret": MINIO_SECRET_KEY,
            "client_kwargs": {"endpoint_url": MINIO_ENDPOINT},
        },
    )

logger.info(f"Input rows: {len(df)}")

# ==============================
# CLEAN DATA TYPES
# ==============================

df["nav_date"] = pd.to_datetime(df["nav_date"]).dt.date
df["import_date"] = pd.to_datetime(df["import_date"]).dt.date

# ==============================
# WRITE TO STAGING (PARQUET)
# ==============================

fs = s3fs.S3FileSystem(
    key=MINIO_ACCESS_KEY,
    secret=MINIO_SECRET_KEY,
    client_kwargs={"endpoint_url": MINIO_ENDPOINT},
)

staging_path = f"s3://{BUCKET}/{STAGING_PREFIX}/data.parquet"

logger.info("Cleaning staging folder")
try:
    fs.rm(f"{BUCKET}/{STAGING_PREFIX}", recursive=True)
except:
    pass

df.to_parquet(
    staging_path,
    engine="pyarrow",
    storage_options={
        "key": MINIO_ACCESS_KEY,
        "secret": MINIO_SECRET_KEY,
        "client_kwargs": {"endpoint_url": MINIO_ENDPOINT},
    },
    index=False,
)

logger.info("Staging write completed")

# ==============================
# CONNECT TRINO
# ==============================

conn = trino.dbapi.connect(
    host=TRINO_HOST,
    port=TRINO_PORT,
    user=TRINO_USER,
    catalog="iceberg",
    schema="ods",
)

cur = conn.cursor()

# ==============================
# STEP 1 – ARCHIVE CHANGED ROWS
# (Deduplicate in SQL)
# ==============================

cur.execute(f"""
INSERT INTO {HISTORY_TABLE}
SELECT
    c.nav_date,
    c.nav_value,
    c.currency,
    c.fund_name,
    c.website,
    c.import_date,
    c.updated_at,
    current_timestamp AS archived_at
FROM {CURRENT_TABLE} c
JOIN (
    SELECT
        nav_date,
        fund_name,
        website,
        MAX(nav_value) AS nav_value,
        MAX(currency) AS currency
    FROM {STAGING_TABLE}
    GROUP BY nav_date, fund_name, website
) s
ON (
    c.nav_date = s.nav_date
    AND c.fund_name = s.fund_name
    AND c.website = s.website
)
WHERE
       c.nav_value IS DISTINCT FROM s.nav_value
    OR c.currency IS DISTINCT FROM s.currency
""")

logger.info("STEP 1 DONE")

# ==============================
# STEP 2 – MERGE (IDEMPOTENT)
# ==============================

cur.execute(f"""
MERGE INTO {CURRENT_TABLE} t
USING (
    SELECT
        nav_date,
        fund_name,
        website,
        MAX(nav_value) AS nav_value,
        MAX(currency) AS currency,
        MAX(import_date) AS import_date
    FROM {STAGING_TABLE}
    GROUP BY nav_date, fund_name, website
) s
ON (
    t.nav_date = s.nav_date
    AND t.fund_name = s.fund_name
    AND t.website = s.website
)

WHEN MATCHED AND (
       t.nav_value IS DISTINCT FROM s.nav_value
    OR t.currency IS DISTINCT FROM s.currency
)
THEN UPDATE SET
    nav_value = s.nav_value,
    currency = s.currency,
    import_date = s.import_date,
    updated_at = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    nav_date,
    nav_value,
    currency,
    fund_name,
    website,
    import_date,
    updated_at
)
VALUES (
    s.nav_date,
    s.nav_value,
    s.currency,
    s.fund_name,
    s.website,
    s.import_date,
    current_timestamp
)
""")

logger.info("STEP 2 DONE")

conn.close()
logger.info("SCD4 Completed – Idempotent Mode")