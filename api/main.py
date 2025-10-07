from fastapi import FastAPI, Query
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient, ASCENDING, DESCENDING
from dotenv import load_dotenv
import os, datetime as dt
import httpx
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler


load_dotenv(dotenv_path=".env")
mongo_uri = str(os.getenv("MONGODB_URI_READONLY"))
print(mongo_uri)
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()
client = MongoClient(mongo_uri)
db = client["transit_project"]

scheduler = AsyncIOScheduler(
    job_defaults={
        "coalesce": True,
        "misfire_grace_time": 120,
        "max_instances": 1,
    }
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not scheduler.running:
        # schedule jobs
        scheduler.add_job(
            alert_freshness,
            "interval",
            minutes=3,
            kwargs={"minutes_threshold": 15},
            misfire_grace_time=60,
        )
        scheduler.add_job(
            alert_gap,
            "interval",
            minutes=5,
            kwargs={"minutes": 120, "z": 3.0, "factor": 2.0, "top_n_pairs": 5},
            misfire_grace_time=120,
        )
        scheduler.start()
    app.state.scheduler = scheduler
    try:
        yield
    finally:
        # shutdown
        if scheduler and scheduler.running:
            scheduler.shutdown(wait=False)


app = FastAPI(title="TTC Streaming API", version="1.0.0", lifespan=lifespan)


@app.get("/")
def root():
    return {"ok": True}


@app.get("/health")
def get_health(thresh_min: int = 10):
    """
    Pipeline freshness check. Checks how long ago was the latest departure we ingested.
    Returns a boolean, the computed lag in minutes, and, a small snippet of the latest record.

    Args:
        thresh_min (int, optional): Staleness threshold, if the record is older than this, endpoint declares the system unhealthy. Defaults to 10.
    """
    latest = db.departures.find_one(
        sort=[("dep_ts", -1)],
        projection={"_id": 0, "dep_ts": 1, "station_uri": 1, "route_uri": 1},
    )
    now = datetime.now(timezone.utc)
    lag_min = (
        (
            now - datetime.fromtimestamp(float(latest["dep_ts"]), tz=timezone.utc)
        ).total_seconds()
        / 60
        if latest
        else 9999
    )
    healthy = lag_min <= thresh_min
    return {"healthy": healthy, "lag_min": round(lag_min, 1), "latest": latest}


@app.get("/metrics/headways/summary")
def headway_summary(
    station: Optional[str] = None, route: Optional[str] = None, minutes: int = 180
):
    """
    Quick summary stats of headways over a recent window. It shows count, average, p95, and
    a simple gap_rate(share of headways >= 2x average)

    Args:
        station (Optional[str], optional): restrict the window to a specific station. Defaults to None.
        route (Optional[str], optional): restricts the window to a specific route. Defaults to None.
        minutes (int, optional): the look-back window length. Defaults to 180.

    Returns:
        Dict: Summary Stats listed above.
    """
    since_dt = datetime.now(timezone.utc) - dt.timedelta(days=1)
    since = int(since_dt.timestamp())
    print(since)

    q = {"dep_ts": {"$gte": since}}
    if station:
        q["station_uri"] = station
    if route:
        q["route_uri"] = route

    rows = list(db.departures_metrics.find(q, {"_id": 0, "headway_mins": 1}))
    vals = [r["headway_mins"] for r in rows if r.get("headway_mins") is not None]
    if not vals:
        return {"count": 0, "avg": None, "p95": None, "gap_rate": None}

    vals.sort()
    p95 = vals[int(0.95 * (len(vals) - 1))]
    gap_rate = sum(v >= 2 * max(1e-9, sum(vals) / len(vals)) for v in vals) / len(vals)

    return {
        "count": len(vals),
        "avg": sum(vals) / len(vals),
        "p95": p95,
        "gap_rate": round(gap_rate, 3),
    }


async def notify_slack(text: str, blocks: Optional[List[Dict[str, Any]]] = None):
    """Send a notification message to Slack via webhook.

    Args:
        text: The message text to send. Used as fallback text and notification preview.
        blocks: Optional list of Slack Block Kit blocks for rich message formatting.

    Returns:
        dict: A dictionary with 'sent' status and optional 'reason' if not sent.
              Returns {"sent": False, "reason": "no webhook configured"} if webhook URL is missing.
              Returns {"sent": True} on successful delivery.

    Raises:
        httpx.HTTPStatusError: If the Slack API returns an error status code.
    """
    if not SLACK_WEBHOOK_URL:
        return {"sent": False, "reason": "no webhook configured"}
    payload: Dict[str, Any] = {"text": text}
    if blocks:
        payload["blocks"] = blocks
    async with httpx.AsyncClient(timeout=10) as client_http:
        r = await client_http.post(SLACK_WEBHOOK_URL, json=payload)
        r.raise_for_status()
    return {"sent": True}


# --- small helpers ---
def _now_utc():
    return datetime.now(timezone.utc)


def _ensure_list(v: Optional[Union[str, List[str]]]) -> Optional[List[str]]:
    if v is None:
        return None
    if isinstance(v, list):
        return [s for s in v if s]
    if isinstance(v, str):
        return [s for s in v.split(",") if s]


# --- freshness alert ---
@app.get("/alerts/freshness")
async def alert_freshness(minutes_threshold: int = Query(15, ge=1, le=240)):
    """Check data freshness and alert if departures are stale.

    Queries the most recent departure from the database and calculates how long ago
    it occurred. If the lag exceeds the specified threshold, sends a Slack alert.

    Args:
        minutes_threshold: Maximum acceptable age in minutes for the newest departure.
                          Must be between 1 and 240. Defaults to 15 minutes.

    Returns:
        dict: If data is fresh, returns {"ok": True, "lag_min": <rounded lag>}.
              If data is stale, returns the Slack notification response.
              If no departures exist, sends a warning notification.

    Notes:
        - Handles both timestamp (int/float) and datetime departure values.
        - Sends Slack notification only when data is stale or missing."""
    latest = db.departures.find_one(
        {},
        sort=[("dep_ts", DESCENDING)],
        projection={"_id": 0, "dep_ts": 1, "station_uri": 1, "route_uri": 1},
    )
    if not latest:
        return await notify_slack(":warning: No departures in collection.")

    dep = latest["dep_ts"]
    dep_dt = (
        datetime.fromtimestamp(int(dep), tz=timezone.utc)
        if isinstance(dep, (int, float))
        else dep
    )
    lag_min = (_now_utc() - dep_dt).total_seconds() / 60.0
    ok = lag_min <= minutes_threshold

    text = (
        f"*Freshness*: newest dep `{dep_dt.isoformat()}` "
        f"({lag_min:.1f} min ago) — threshold {minutes_threshold} → "
        + ("*OK* ✅" if ok else "*STALE* 🚨")
    )
    if ok:
        return {"ok": True, "lag_min": round(lag_min, 1)}
    return await notify_slack(text)


# --- headway/gap alert ---
@app.get("/alerts/gap")
async def alert_gap(
    station: Optional[Union[str, List[str]]] = Query(None),
    route: Optional[Union[str, List[str]]] = Query(None),
    minutes: int = Query(120, ge=5, le=1440),
    z: float = Query(3.0, ge=2.0, le=5.0),
    factor: float = Query(2.0, ge=1.2, le=5.0),
    top_n_pairs: int = Query(5, ge=1, le=20),
):
    """
    Detect headway (time between departures) anomalies.

    Args:
        station: Station URI(s) to filter by
        route: Route URI(s) to filter by
        minutes: Time window to look back (5-1440 minutes)
        z: Z-score threshold for statistical anomaly detection (2.0-5.0)
        factor: Multiplier of typical headway to consider as anomaly (1.2-5.0)
        top_n_pairs: Number of top station-route pairs to analyze if no filters provided
    """

    since_sec = int((_now_utc() - timedelta(minutes=minutes)).timestamp())

    stations = _ensure_list(station)
    routes = _ensure_list(route)

    q: Dict[str, Any] = {"dep_ts": {"$gte": since_sec}}
    if stations:
        q["station_uri"] = {"$in": stations}
    if routes:
        q["route_uri"] = {"$in": routes}
    if not stations and not routes:
        pairs = list(
            db.departures.aggregate(
                [
                    {"$match": {"dep_ts": {"$gte": since_sec}}},
                    {
                        "$group": {
                            "_id": {"s": "$station_uri", "r": "$route_uri"},
                            "cnt": {"$sum": 1},
                        }
                    },
                    {"$sort": {"cnt": -1}},
                    {"$limit": top_n_pairs},
                ]
            )
        )
        if pairs:
            q["station_uri"] = {"$in": [p["_id"]["s"] for p in pairs]}
            q["route_uri"] = {"$in": [p["_id"]["r"] for p in pairs]}

    rows = list(
        db.departures_metrics.find(
            q,
            {
                "_id": 0,
                "dep_ts": 1,
                "station_uri": 1,
                "route_uri": 1,
                "headway_min": 1,
                "roll_mean": 1,
                "roll_std": 1,
                "z": 1,
            },
        ).sort([("dep_ts", 1)])
    )

    alerts = []
    for r in rows:
        hw = r.get("headway_min")
        typ = r.get("roll_mean") or r.get("median_headway")
        zv = r.get("z")
        if hw is None or typ is None:
            continue
        if (zv is not None and abs(zv) >= z) or (typ and hw >= factor * typ):
            t = r["dep_ts"]
            t_iso = (
                datetime.fromtimestamp(int(t), tz=timezone.utc).isoformat()
                if isinstance(t, (int, float))
                else str(t)
            )
            alerts.append(
                f"• `{t_iso}` — {r.get('station_uri')} / {r.get('route_uri')}: {hw:.1f}m (typ {typ:.1f}m, z={zv:.1f if zv is not None else float('nan')})"
            )

    if not alerts:
        return {"ok": True, "alerts": 0}

    text = f"🔔 *Headway anomalies* last {minutes}m — {len(alerts)} hits"
    # keep message compact (last 10)
    await notify_slack(text + "\n" + "\n".join(alerts[-10:]))
    return {"ok": False, "alerts": len(alerts)}


@app.get("/alerts/scheduler")
def scheduler_info():
    """
    Get information about the background scheduler and its jobs.

        Retrieves the current state of the APScheduler instance, including whether
        it's running and details about all scheduled jobs.

    Returns:
            dict: A dictionary containing:
                - running (bool): Whether the scheduler is currently running.
                - jobs (list): List of job dictionaries, each containing:
                    - id (str): Unique job identifier.
                    - trigger (str): String representation of the job's trigger.
                    - next_run_time (str|None): ISO format datetime of next execution, or None.
                - note (str): Only present if scheduler is not attached to app state.

        Notes:
            - Returns {"running": False, "jobs": [], "note": "scheduler not attached"}
              if no scheduler is found in app.state.
    """
    s = getattr(app.state, "scheduler", None)
    if not s:
        return {"running": False, "jobs": [], "note": "scheduler not attached"}
    jobs = []
    for j in s.get_jobs():
        jobs.append(
            {
                "id": j.id,
                "trigger": str(j.trigger),
                "next_run_time": (
                    j.next_run_time.isoformat() if j.next_run_time else None
                ),
            }
        )
    return {"running": s.running, "jobs": jobs}


# Recreate the app with lifespan enabled
app.router.lifespan_context = lifespan
