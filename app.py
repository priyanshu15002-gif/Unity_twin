import os
import math
import asyncpg
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
from dotenv import load_dotenv

# Load .env if present
load_dotenv()

DATABASE_URL = os.getenv("NEON_DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("NEON_DATABASE_URL is not set. Put it in .env or your environment.")

app = FastAPI(title="Assets API", version="0.1.0")

# Allow Unity Editor/WebGL & local tools in dev (tighten later)
ALLOWED_ORIGINS = [
    "http://localhost",
    "http://localhost:3000",
    "http://localhost:5173",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:5500",  # file server
    # add your Unity WebGL hosting origin/domain when known
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

pool: asyncpg.Pool | None = None

# ---------- Models ----------
class KpiSummary(BaseModel):
    baseline_kwh: float
    actual_kwh: float
    saved_kwh: float
    offline_count: int

class AssetDTO(BaseModel):
    asset_id: str
    asset_type: str
    building: str
    floor: str
    zone: str
    status: str
    fault_reoccurrence_level: str
    energy_consumed_per_hour: float
    planned_active_hours: float
    actual_active_hours: float

class AnomalyDTO(BaseModel):
    asset_id: str
    asset_type: str
    building: str
    floor: str
    zone: str
    actual_kwh: float
    avg_kwh: float
    std_kwh: float
    zscore: float

# ---------- DB Pool ----------
@app.on_event("startup")
async def on_startup():
    global pool
    pool = await asyncpg.create_pool(
        dsn=DATABASE_URL,
        min_size=1,
        max_size=5,  # raise if needed
        timeout=10,
    )
    # quick sanity check
    async with pool.acquire() as conn:
        await conn.execute("SELECT 1;")

@app.on_event("shutdown")
async def on_shutdown():
    if pool:
        await pool.close()

# ---------- Health ----------
@app.get("/healthz")
async def healthz():
    return {"status": "ok"}

# ---------- KPIs ----------
@app.get("/kpi/summary", response_model=KpiSummary)
async def kpi_summary():
    sql = """
    SELECT
      COALESCE(SUM(planned_active_hours * energy_consumed_per_hour), 0) AS baseline_kwh,
      COALESCE(SUM(actual_active_hours  * energy_consumed_per_hour), 0) AS actual_kwh,
      COALESCE(SUM(planned_active_hours * energy_consumed_per_hour)
             - SUM(actual_active_hours  * energy_consumed_per_hour), 0) AS saved_kwh,
      COALESCE(SUM(CASE WHEN status = 'OFFLINE' THEN 1 ELSE 0 END), 0)  AS offline_count
    FROM assets;
    """
    async with pool.acquire() as conn:  # type: ignore
        row = await conn.fetchrow(sql)
        return dict(row)

# ---------- Assets ----------
@app.get("/assets", response_model=List[AssetDTO])
async def list_assets(
    building: Optional[str] = Query(None, description="Exact building match, e.g., 'Building A'"),
    floor: Optional[str] = Query(None, description="Exact floor match, e.g., 'F3'"),
    asset_type: Optional[str] = Query(None, description="Filter by asset type, e.g., 'HVAC'"),
    status: Optional[str] = Query(None, description="ONLINE/OFFLINE"),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    sql = """
    SELECT asset_id, asset_type, building, floor, zone, status,
           fault_reoccurrence_level, energy_consumed_per_hour,
           planned_active_hours, actual_active_hours
    FROM assets
    WHERE ($1::text IS NULL OR building = $1)
      AND ($2::text IS NULL OR floor    = $2)
      AND ($3::text IS NULL OR asset_type = $3)
      AND ($4::text IS NULL OR status   = $4)
    ORDER BY asset_type, building, floor, asset_id
    LIMIT $5 OFFSET $6;
    """
    async with pool.acquire() as conn:  # type: ignore
        rows = await conn.fetch(sql, building, floor, asset_type, status, limit, offset)
        return [dict(r) for r in rows]

# ---------- Insights: Anomalies ----------
@app.get("/insights/anomalies", response_model=List[AnomalyDTO])
async def anomalies(z: float = Query(2.0, ge=0.0, le=10.0)):
    """
    Simple peer-based anomaly:
    Compute z-score of (actual_kwh) vs peers of same asset_type, return |z| >= z.
    """
    sql = """
    WITH peer AS (
      SELECT asset_type,
             AVG(actual_active_hours * energy_consumed_per_hour) AS avg_kwh,
             STDDEV_POP(actual_active_hours * energy_consumed_per_hour) AS std_kwh
      FROM assets
      GROUP BY asset_type
    )
    SELECT a.asset_id, a.asset_type, a.building, a.floor, a.zone,
           (a.actual_active_hours * a.energy_consumed_per_hour) AS actual_kwh,
           p.avg_kwh, p.std_kwh,
           CASE WHEN p.std_kwh > 0
                THEN ((a.actual_active_hours * a.energy_consumed_per_hour) - p.avg_kwh) / p.std_kwh
                ELSE 0 END AS zscore
    FROM assets a
    JOIN peer p USING (asset_type)
    WHERE ABS(
      CASE WHEN p.std_kwh > 0
           THEN ((a.actual_active_hours * a.energy_consumed_per_hour) - p.avg_kwh) / p.std_kwh
           ELSE 0 END) >= $1
    ORDER BY ABS(
      CASE WHEN p.std_kwh > 0
           THEN ((a.actual_active_hours * a.energy_consumed_per_hour) - p.avg_kwh) / p.std_kwh
           ELSE 0 END) DESC
    LIMIT 50;
    """
    async with pool.acquire() as conn:  # type: ignore
        rows = await conn.fetch(sql, z)
        return [dict(r) for r in rows]
