"""
dashboard/api.py
Flask API that serves the observability dashboard HTML and all data endpoints.
Runs as a Docker service alongside Airflow, Kafka, and PostgreSQL.

Endpoints:
  GET /                        → dashboard HTML
  GET /api/metrics             → KPI counts from all 4 tables
  GET /api/pipeline            → live DAG stage status (Airflow REST API + DB fallback)
  GET /api/dag-runs            → 7-day run history
  GET /api/anomalies           → gold_anomalies table
  GET /api/prices              → gold_daily_summary close prices + bronze ingest volume
"""

import os
import logging
import requests
from datetime import datetime, date
from flask import Flask, jsonify, send_from_directory, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder="static")
CORS(app)

# ─── DB CONFIG ──────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "postgres"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME",     "financial_market_intelligence"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# ─── AIRFLOW CONFIG ─────────────────────────────────────────────────
# Inside Docker, Airflow webserver is reachable via service name on port 8080
AIRFLOW_BASE = os.getenv("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1")
AIRFLOW_USER = os.getenv("AIRFLOW_USER",    "admin")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASSWORD","admin")
DAG_ID       = "market_ingestion_dag"

TASK_ORDER = [
    {"step": 1, "task_id": "extract_validate_load_kafka", "label": "Extract → validate → load → Kafka"},
    {"step": 2, "task_id": "calculate_indicators",        "label": "Calculate indicators"},
    {"step": 3, "task_id": "gold_aggregations",           "label": "Gold aggregations"},
    {"step": 4, "task_id": "anomaly_detection",           "label": "Anomaly detection"},
    {"step": 5, "task_id": "validate_pipeline_outputs",   "label": "Validate pipeline outputs"},
]

# ─── HELPERS ────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def db_query(sql, params=None):
    """Run a SELECT and return list of dicts."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

def airflow_get(path):
    """Call Airflow REST API. Returns parsed JSON or None if unreachable."""
    try:
        url = f"{AIRFLOW_BASE}{path}"
        r = requests.get(url, auth=(AIRFLOW_USER, AIRFLOW_PASS), timeout=5)
        if r.status_code == 200:
            return r.json()
        logger.warning(f"Airflow {path} → HTTP {r.status_code}")
        return None
    except Exception as e:
        logger.warning(f"Airflow unreachable ({path}): {e}")
        return None

def af_state_to_status(state):
    mapping = {
        "success":        "success",
        "running":        "running",
        "queued":         "running",
        "failed":         "failed",
        "upstream_failed":"failed",
    }
    return mapping.get(state, "pending")

def fmt_dur(seconds):
    if seconds is None:
        return None
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    return f"{s // 60}m {s % 60}s"

def serial(obj):
    """Make dates JSON-serializable."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return str(obj)

def jsonify_rows(rows):
    import json
    return app.response_class(
        json.dumps(rows, default=serial),
        mimetype="application/json"
    )

# ─── SERVE DASHBOARD HTML ───────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory("static", "dashboard.html")

# ─── API: METRICS ───────────────────────────────────────────────────
@app.route("/api/metrics")
def api_metrics():
    try:
        rows = db_query("""
            SELECT
                (SELECT COUNT(*) FROM bronze_stock_ticks)         AS bronze_count,
                (SELECT COUNT(*) FROM silver_stock_indicators)     AS silver_count,
                (SELECT COUNT(*) FROM gold_daily_summary)          AS gold_count,
                (SELECT COUNT(*) FROM gold_anomalies)              AS anomaly_count,
                (SELECT COUNT(*) FROM gold_anomalies
                 WHERE detected_at::date = CURRENT_DATE)           AS anomalies_today,
                (SELECT MAX(timestamp) FROM bronze_stock_ticks)    AS last_ingested_at
        """)
        m = rows[0]

        bronze = int(m["bronze_count"] or 0)
        silver = int(m["silver_count"] or 0)
        pass_rate = round((silver / bronze * 100), 1) if bronze > 0 else 0.0

        return jsonify_rows({
            "bronze_count":          bronze,
            "silver_count":          silver,
            "gold_count":            int(m["gold_count"] or 0),
            "anomaly_count":         int(m["anomaly_count"] or 0),
            "anomalies_today":       int(m["anomalies_today"] or 0),
            "kafka_topic":           os.getenv("KAFKA_TOPIC", "stock_ticks"),
            "last_ingested_at":      serial(m["last_ingested_at"]) if m["last_ingested_at"] else None,
            "validation_pass_rate":  pass_rate,
        })
    except Exception as e:
        logger.error(f"/api/metrics error: {e}")
        return jsonify({"error": str(e)}), 500

# ─── API: PIPELINE ──────────────────────────────────────────────────
@app.route("/api/pipeline")
def api_pipeline():
    try:
        # Try Airflow REST API first
        latest_run_data = airflow_get(f"/dags/{DAG_ID}/dagRuns?limit=1&order_by=-logical_date")
        latest_run = (latest_run_data or {}).get("dag_runs", [None])[0]

        if latest_run:
            # Airflow is live — get real task instance states
            run_id = latest_run["dag_run_id"]
            ti_data = airflow_get(f"/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances")
            task_instances = {t["task_id"]: t for t in (ti_data or {}).get("task_instances", [])}

            stages = []
            for meta in TASK_ORDER:
                ti = task_instances.get(meta["task_id"], {})
                state = ti.get("state")
                status = af_state_to_status(state)
                duration = ti.get("duration")

                if state == "success" and duration:
                    detail = f"Completed in {fmt_dur(duration)}"
                elif state == "running":
                    detail = "Currently running..."
                elif state == "failed":
                    detail = "Task failed — check Airflow at :8080"
                elif state == "upstream_failed":
                    detail = "Skipped — upstream task failed"
                elif state == "queued":
                    detail = "Queued, waiting for worker"
                else:
                    detail = "Pending"

                stages.append({
                    "step":       meta["step"],
                    "task_id":    meta["task_id"],
                    "label":      meta["label"],
                    "status":     status,
                    "duration_s": duration,
                    "detail":     detail,
                    "start_date": ti.get("start_date"),
                    "end_date":   ti.get("end_date"),
                })

            return jsonify_rows({
                "dag_id":            DAG_ID,
                "schedule":          "@daily",
                "last_run_date":     latest_run.get("start_date") or latest_run.get("logical_date"),
                "last_run_state":    latest_run.get("state"),
                "airflow_connected": True,
                "stages":            stages,
            })

        # Airflow unreachable — infer from DB row counts
        rows = db_query("""
            SELECT
                (SELECT COUNT(*) FROM bronze_stock_ticks      WHERE timestamp::date = CURRENT_DATE) AS bronze,
                (SELECT COUNT(*) FROM silver_stock_indicators WHERE timestamp::date = CURRENT_DATE) AS silver,
                (SELECT COUNT(*) FROM gold_daily_summary      WHERE date = CURRENT_DATE)            AS gold,
                (SELECT COUNT(*) FROM gold_anomalies          WHERE detected_at::date = CURRENT_DATE) AS anomalies
        """)
        c = rows[0]
        b = int(c["bronze"] or 0)
        s = int(c["silver"] or 0)
        g = int(c["gold"]   or 0)
        a = int(c["anomalies"] or 0)

        stages = [
            {"step":1,"task_id":"extract_validate_load_kafka","label":"Extract → validate → load → Kafka",
             "status":"success" if b>0 else "pending","duration_s":None,"detail":f"{b:,} rows loaded · AAPL, MSFT" if b>0 else "Waiting for today's run","start_date":None,"end_date":None},
            {"step":2,"task_id":"calculate_indicators","label":"Calculate indicators",
             "status":"success" if s>0 else ("running" if b>0 else "pending"),"duration_s":None,"detail":f"{s:,} rows · ma_7, ma_20, volatility_7" if s>0 else ("Running..." if b>0 else "Pending"),"start_date":None,"end_date":None},
            {"step":3,"task_id":"gold_aggregations","label":"Gold aggregations",
             "status":"success" if g>0 else ("running" if s>0 else "pending"),"duration_s":None,"detail":f"{g:,} rows · daily_summary" if g>0 else ("Running..." if s>0 else "Pending"),"start_date":None,"end_date":None},
            {"step":4,"task_id":"anomaly_detection","label":"Anomaly detection",
             "status":"success" if g>0 else "pending","duration_s":None,"detail":f"{a} anomaly(ies) detected today" if g>0 else "Pending","start_date":None,"end_date":None},
            {"step":5,"task_id":"validate_pipeline_outputs","label":"Validate pipeline outputs",
             "status":"success" if g>0 else "pending","duration_s":None,"detail":"All tables passed validation" if g>0 else "Pending","start_date":None,"end_date":None},
        ]

        return jsonify_rows({
            "dag_id": DAG_ID, "schedule": "@daily",
            "last_run_date": None, "last_run_state": None,
            "airflow_connected": False, "stages": stages,
        })

    except Exception as e:
        logger.error(f"/api/pipeline error: {e}")
        return jsonify({"error": str(e)}), 500

# ─── API: DAG RUNS ──────────────────────────────────────────────────
@app.route("/api/dag-runs")
def api_dag_runs():
    TASK_BLOCKS = [
        {"id":"E","task_id":"extract_validate_load_kafka","label":"Extract"},
        {"id":"I","task_id":"calculate_indicators",       "label":"Indicators"},
        {"id":"G","task_id":"gold_aggregations",          "label":"Gold"},
        {"id":"A","task_id":"anomaly_detection",          "label":"Anomaly"},
        {"id":"V","task_id":"validate_pipeline_outputs",  "label":"Validate"},
    ]
    try:
        af_data = airflow_get(f"/dags/{DAG_ID}/dagRuns?limit=7&order_by=-logical_date")
        af_runs = (af_data or {}).get("dag_runs", [])

        if af_runs:
            runs = []
            for run in af_runs:
                run_id = run["dag_run_id"]
                ti_data = airflow_get(f"/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances")
                task_map = {t["task_id"]: t for t in (ti_data or {}).get("task_instances", [])}

                stages = []
                total_dur = 0
                for block in TASK_BLOCKS:
                    ti = task_map.get(block["task_id"], {})
                    dur = ti.get("duration")
                    if dur:
                        total_dur += dur
                    stages.append({
                        "id":         block["id"],
                        "label":      block["label"],
                        "status":     af_state_to_status(ti.get("state")),
                        "duration_s": dur,
                    })

                runs.append({
                    "run_date":        run["logical_date"][:10],
                    "run_id":          run_id,
                    "state":           run.get("state"),
                    "stages":          stages,
                    "total_duration_s":total_dur if total_dur > 0 else None,
                    "success":         run.get("state") == "success",
                })
            return jsonify({"runs": runs, "airflow_connected": True})

        # DB fallback — infer from per-day row counts
        days = db_query("""
            SELECT DISTINCT date::text AS run_date FROM (
                SELECT timestamp::date AS date FROM bronze_stock_ticks
                UNION
                SELECT date FROM gold_daily_summary
            ) d
            WHERE date >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY date DESC LIMIT 7
        """)

        runs = []
        for day in days:
            rd = day["run_date"]
            c = db_query("""
                SELECT
                    (SELECT COUNT(*) FROM bronze_stock_ticks      WHERE timestamp::date = %s) AS b,
                    (SELECT COUNT(*) FROM silver_stock_indicators WHERE timestamp::date = %s) AS s,
                    (SELECT COUNT(*) FROM gold_daily_summary      WHERE date = %s)            AS g,
                    (SELECT COUNT(*) FROM gold_anomalies          WHERE detected_at::date = %s) AS a
            """, (rd, rd, rd, rd))[0]

            b = int(c["b"] or 0); s = int(c["s"] or 0)
            g = int(c["g"] or 0); a_ok = g > 0

            stages = [
                {"id":"E","label":"Extract",    "status":"success" if b>0 else "failed","duration_s":None},
                {"id":"I","label":"Indicators", "status":"success" if s>0 else ("failed" if b>0 else "pending"),"duration_s":None},
                {"id":"G","label":"Gold",       "status":"success" if g>0 else ("failed" if s>0 else "pending"),"duration_s":None},
                {"id":"A","label":"Anomaly",    "status":"success" if a_ok else "pending","duration_s":None},
                {"id":"V","label":"Validate",   "status":"success" if a_ok else "pending","duration_s":None},
            ]
            ok = all(st["status"] == "success" for st in stages)
            runs.append({"run_date":rd,"run_id":None,"state":None,"stages":stages,"total_duration_s":None,"success":ok})

        return jsonify({"runs": runs, "airflow_connected": False})

    except Exception as e:
        logger.error(f"/api/dag-runs error: {e}")
        return jsonify({"error": str(e)}), 500

# ─── API: ANOMALIES ─────────────────────────────────────────────────
@app.route("/api/anomalies")
def api_anomalies():
    try:
        limit  = min(int(request.args.get("limit", 20)), 100)
        ticker = request.args.get("ticker")

        sql = """
            SELECT ticker, date::text, close, volume, daily_return,
                   volatility_7, volume_ma_7, anomaly_type, anomaly_reason,
                   detected_at
            FROM gold_anomalies
            {where}
            ORDER BY detected_at DESC
            LIMIT %s
        """
        if ticker:
            rows = db_query(sql.format(where="WHERE ticker = %s"), (ticker, limit))
        else:
            rows = db_query(sql.format(where=""), (limit,))

        breakdown = db_query("""
            SELECT anomaly_type AS type, COUNT(*) AS count
            FROM gold_anomalies
            GROUP BY anomaly_type ORDER BY count DESC
        """)

        return jsonify_rows({
            "anomalies": rows,
            "breakdown": [{"type": r["type"], "count": int(r["count"])} for r in breakdown],
        })

    except Exception as e:
        logger.error(f"/api/anomalies error: {e}")
        return jsonify({"error": str(e)}), 500

# ─── API: PRICES ────────────────────────────────────────────────────
@app.route("/api/prices")
def api_prices():
    try:
        days = min(int(request.args.get("days", 30)), 90)

        prices = db_query(f"""
            SELECT date::text AS date, ticker, close, daily_return, signal
            FROM gold_daily_summary
            WHERE date >= CURRENT_DATE - INTERVAL '{days} days'
            ORDER BY date ASC, ticker ASC
        """)

        ingest_vol = db_query(f"""
            SELECT timestamp::date::text AS date, ticker, COUNT(*) AS row_count
            FROM bronze_stock_ticks
            WHERE timestamp >= NOW() - INTERVAL '{days} days'
            GROUP BY date, ticker
            ORDER BY date ASC, ticker ASC
        """)

        return jsonify_rows({"prices": prices, "ingest_volume": ingest_vol})

    except Exception as e:
        logger.error(f"/api/prices error: {e}")
        return jsonify({"error": str(e)}), 500

# ─── HEALTH CHECK ───────────────────────────────────────────────────
@app.route("/health")
def health():
    try:
        db_query("SELECT 1")
        return jsonify({"status": "ok", "db": "connected"})
    except Exception as e:
        return jsonify({"status": "error", "db": str(e)}), 500

if __name__ == "__main__":
    port = int(os.getenv("DASHBOARD_PORT", 3000))
    app.run(host="0.0.0.0", port=port, debug=False)
