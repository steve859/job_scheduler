import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException, Request


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _data_dir() -> str:
    return os.environ.get("AIOPS_DATA_DIR", "/data")


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _prom_base_url() -> Optional[str]:
    val = os.environ.get("PROMETHEUS_BASE_URL")
    return val.strip() if val and val.strip() else None


def _prom_query(query: str, timeout_s: float = 2.0) -> Optional[Dict[str, Any]]:
    base = _prom_base_url()
    if not base:
        return None
    try:
        resp = requests.get(
            f"{base}/api/v1/query",
            params={"query": query},
            timeout=timeout_s,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def _suggestions_for_alert(alertname: str) -> Dict[str, Any]:
    # Minimal, deterministic heuristics. Keep it ops-focused.
    if alertname == "JobDlqSpike":
        return {
            "impact": "Jobs are failing and being routed to DLQ; downstream may be unhealthy.",
            "checks": [
                "Inspect worker logs for recent exceptions.",
                "Check Cassandra health and timeouts.",
                "Look for a recent deploy/config change causing job failures.",
            ],
            "queries": [
                "sum(rate(jobs_dlq_total[5m]))",
                "sum(rate(jobs_processed_total{status=\"failed\"}[5m]))",
            ],
            "actions": [
                "If failures are transient, consider reducing retry aggressiveness / add backoff.",
                "If schema/timeouts: check Cassandra node load/GC and driver timeouts.",
            ],
        }

    if alertname == "WorkerSaturated":
        return {
            "impact": "Worker concurrency is near its cap; jobs may be delayed.",
            "checks": [
                "Confirm if incoming job rate increased.",
                "Check job duration histogram for p95/p99 increases.",
                "Check CPU/memory throttling on worker container.",
            ],
            "queries": [
                "max(jobs_in_progress)",
                "histogram_quantile(0.95, sum(rate(job_duration_seconds_bucket[5m])) by (le))",
            ],
            "actions": [
                "Scale worker replicas (or increase WORKER_MAX_IN_PROGRESS cautiously).",
                "Investigate slow jobs; optimize or split large workloads.",
            ],
        }

    if alertname == "LockRenewFailures":
        return {
            "impact": "Lease renewals are failing; risk of lock churn and job aborts.",
            "checks": [
                "Check LWT latency and Cassandra errors/timeouts.",
                "Look for clock skew or long GC pauses on workers.",
                "Verify TTL/lease settings vs job durations.",
            ],
            "queries": [
                "sum(rate(lock_renew_failure_total[5m]))",
                "histogram_quantile(0.99, sum(rate(lwt_latency_seconds_bucket[5m])) by (le, op))",
            ],
            "actions": [
                "Reduce contention (shard locks, reduce hot keys).",
                "Tune Cassandra / JVM / client timeouts.",
            ],
        }

    if alertname == "LwtLatencyP99High":
        return {
            "impact": "Cassandra LWT is slow; lock operations and scheduling will degrade.",
            "checks": [
                "Check which op is slow via label op.",
                "Inspect Cassandra node CPU, GC, IO, and pending compactions.",
                "Check for contention (many workers on same lock key).",
            ],
            "queries": [
                "histogram_quantile(0.99, sum(rate(lwt_latency_seconds_bucket[5m])) by (le, op))",
            ],
            "actions": [
                "Scale Cassandra / tune heap + GC, consider faster disks.",
                "Reduce LWT rate or hot-spot locks.",
            ],
        }

    if alertname == "SchedulerHttp5xx":
        return {
            "impact": "Scheduler API is failing; job submissions/enqueues may be broken.",
            "checks": [
                "Check scheduler logs for Cassandra connectivity errors.",
                "Check if schema/keyspace exists and Cassandra is healthy.",
            ],
            "queries": [
                "sum(rate(scheduler_http_requests_total{status=~\"5..\"}[5m]))",
                "sum(rate(scheduler_http_requests_total[5m]))",
            ],
            "actions": [
                "Restart scheduler if stuck; then investigate root cause.",
                "Roll back if this started after a deploy.",
            ],
        }

    return {
        "impact": "Unknown alert; add heuristics for this alertname.",
        "checks": [],
        "queries": [],
        "actions": [],
    }


def _build_report(payload: Dict[str, Any]) -> Dict[str, Any]:
    alerts: List[Dict[str, Any]] = payload.get("alerts") or []

    report_alerts: List[Dict[str, Any]] = []
    for a in alerts:
        labels = a.get("labels") or {}
        annotations = a.get("annotations") or {}
        alertname = labels.get("alertname", "Unknown")
        suggestion = _suggestions_for_alert(alertname)

        # Best-effort enrichment from Prometheus.
        enriched: List[Dict[str, Any]] = []
        for q in suggestion.get("queries", []):
            enriched.append({"query": q, "result": _prom_query(q)})

        report_alerts.append(
            {
                "alertname": alertname,
                "status": a.get("status"),
                "startsAt": a.get("startsAt"),
                "endsAt": a.get("endsAt"),
                "labels": labels,
                "annotations": annotations,
                "suggestion": suggestion,
                "prometheus": enriched,
            }
        )

    return {
        "generatedAt": _utc_now_iso(),
        "status": payload.get("status"),
        "receiver": payload.get("receiver"),
        "groupLabels": payload.get("groupLabels"),
        "commonLabels": payload.get("commonLabels"),
        "commonAnnotations": payload.get("commonAnnotations"),
        "externalURL": payload.get("externalURL"),
        "alerts": report_alerts,
        "summary": {
            "alertCount": len(alerts),
            "alertNames": sorted({(a.get("labels") or {}).get("alertname", "Unknown") for a in alerts}),
        },
    }


app = FastAPI(title="AIOps", version="0.1")


@app.get("/healthz")
def healthz() -> Dict[str, Any]:
    return {"ok": True, "time": _utc_now_iso()}


@app.post("/webhook/alertmanager")
async def webhook_alertmanager(request: Request) -> Dict[str, Any]:
    try:
        payload = await request.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    report_id = str(uuid.uuid4())
    base = _data_dir()
    _ensure_dir(base)

    raw_path = os.path.join(base, f"{report_id}.alertmanager.json")
    report_path = os.path.join(base, f"{report_id}.report.json")

    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    report = _build_report(payload)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    return {"ok": True, "reportId": report_id}


@app.get("/reports")
def list_reports() -> Dict[str, Any]:
    base = _data_dir()
    _ensure_dir(base)
    ids: List[str] = []
    for name in os.listdir(base):
        if name.endswith(".report.json"):
            ids.append(name[: -len(".report.json")])
    ids.sort(reverse=True)
    return {"reports": ids, "count": len(ids)}


@app.get("/reports/{report_id}")
def get_report(report_id: str) -> Dict[str, Any]:
    base = _data_dir()
    path = os.path.join(base, f"{report_id}.report.json")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Report not found")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "service": "aiops",
        "time": _utc_now_iso(),
        "endpoints": ["/healthz", "/webhook/alertmanager", "/reports", "/reports/{id}"],
    }
