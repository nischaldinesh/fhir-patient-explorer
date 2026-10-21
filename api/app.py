"""Metrics API (demo): the semantic layer's metrics over HTTP, small cells always hidden.

Run:  api/run.sh   (uvicorn on http://127.0.0.1:8000, docs at /docs)

Every number comes from analytics/metrics.py on the serving snapshot (api/engine.py), so the rule of
analytics/SEMANTIC_LAYER.md applies to every response: a count from 1 to 10 is never returned. Requests are built
from allowlists only (api/schema.py). The log holds the route, the status and the time taken, never query values
or the key.
"""
import hmac
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field

from api import schema
from api.engine import Engine, NoSnapshot

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SERVING = ROOT / "data" / "analytics" / "serving"
NOTE = ("Counts from 1 to 10 are hidden (empty, is_suppressed true), and so are rates and means whose counts are. "
        "Demo data: MIMIC IV demo (deidentified), Synthea (synthetic) and the invented Riverside General feed.")

log = logging.getLogger("metrics_api")
if not log.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    log.addHandler(_h)
    log.setLevel(logging.INFO)
key_header = APIKeyHeader(name="X-API-Key", auto_error=False, description="The key in METRICS_API_KEY")


class SnapshotInfo(BaseModel):
    version: str = Field(examples=["20261019T221723Z"])
    data_date: Optional[str] = Field(None, description="Last day of the live feed in the snapshot",
                                     examples=["2026-10-14"])


class Health(BaseModel):
    status: str = Field(examples=["ok"])
    snapshot: Optional[SnapshotInfo] = None


class Metric(BaseModel):
    name: str
    label: Optional[str]
    description: Optional[str]
    type: Optional[str]
    suppression: Optional[str] = Field(description="count, rate or mean: how small cells are hidden")
    group_by: list[str] = Field(description="Dimensions this metric can be split by")
    filters: list[str] = Field(description="Fields this metric can be filtered by (where=field:value)")
    needs_grain: bool = Field(description="A window over time: give a grain")


class Catalog(BaseModel):
    metrics: list[Metric]
    grains: list[str]
    max_group_by: int
    filters: dict[str, list[str]]
    saved_queries: list[str]


class Result(BaseModel):
    snapshot: SnapshotInfo
    query: dict[str, Any]
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    truncated: bool = Field(description="True when limit cut the rows")
    suppressed_rows: int = Field(description="Rows with at least one hidden value")
    note: str = NOTE


def create_app(api_key=None, engine=None, serving_dir=None, warm=True):
    """The app. api_key defaults to METRICS_API_KEY; without one the app refuses to start."""
    api_key = api_key if api_key is not None else os.environ.get("METRICS_API_KEY", "")
    if not api_key:
        raise RuntimeError("METRICS_API_KEY is not set (see api/.env.example)")
    engine = engine or Engine(serving_dir or os.environ.get("METRICS_SERVING_DIR") or DEFAULT_SERVING)

    @asynccontextmanager
    async def lifespan(app):
        if warm:
            try:
                engine.snapshot()   # a new snapshot warms the saved queries in the background
            except NoSnapshot:
                log.warning("no serving snapshot yet")
        yield

    app = FastAPI(
        lifespan=lifespan,
        title="Metrics API (demo)",
        version="1.0.0",
        summary="The semantic layer's metrics, with small cell suppression",
        description=("Aggregates only. Metrics are defined once in the dbt MetricFlow semantic layer "
                     "(analytics/SEMANTIC_LAYER.md) and read from a slim serving snapshot of the warehouse. " + NOTE),
    )

    def require_key(key: Optional[str] = Depends(key_header)):
        # constant time: the comparison takes as long whatever the key is
        if not key or not hmac.compare_digest(key.encode(), api_key.encode()):
            raise HTTPException(status_code=401, detail="missing or wrong X-API-Key")

    @app.middleware("http")
    async def access_log(request: Request, call_next):
        start = time.perf_counter()
        response = await call_next(request)
        route = request.scope.get("route")
        log.info("%s %s %s %.0fms", request.method, getattr(route, "path", "unmatched"), response.status_code,
                 (time.perf_counter() - start) * 1000)
        return response

    @app.exception_handler(RequestValidationError)
    async def invalid(request: Request, exc: RequestValidationError):
        # the reason only, never the value sent
        return JSONResponse(status_code=422, content={"detail": [
            {"loc": list(e.get("loc", [])), "msg": e.get("msg")} for e in exc.errors()]})

    def respond(metric_names, rows, snap, query, limit=schema.MAX_LIMIT):
        rows = schema.present(rows)
        try:
            schema.check_rows(rows, metric_names, engine.cat)
        except ValueError as e:
            log.error("response blocked by the guard: %s", str(e).split(":")[0])
            raise HTTPException(status_code=500, detail="response blocked by the suppression guard") from None
        shown = rows[:limit]
        return Result(snapshot=SnapshotInfo(version=snap["version"], data_date=snap.get("data_date")), query=query,
                      columns=list(shown[0]) if shown else [], rows=shown, row_count=len(shown),
                      truncated=len(rows) > limit, suppressed_rows=sum(1 for r in shown if r["is_suppressed"]))

    def run(metric_names, group_by, where):
        try:
            return engine.run(metric_names, group_by, where)
        except NoSnapshot as e:
            raise HTTPException(status_code=503, detail=str(e)) from None
        except Exception as e:
            log.error("query failed: %s", type(e).__name__)
            raise HTTPException(status_code=400, detail="the semantic layer could not answer this query") from None

    @app.get("/health", response_model=Health, tags=["status"], summary="Liveness and the snapshot in use (no key)")
    def health():
        try:
            snap = engine.snapshot()
        except NoSnapshot:
            return Health(status="no snapshot")
        return Health(status="ok", snapshot=SnapshotInfo(version=snap["version"], data_date=snap.get("data_date")))

    @app.get("/v1/metrics", response_model=Catalog, tags=["metrics"], dependencies=[Depends(require_key)],
             summary="The metrics catalog with definitions")
    def list_metrics():
        out = []
        for name in schema.PUBLIC_METRICS:
            info = engine.cat[name]
            ents = schema.METRIC_ENTITIES[name]
            out.append(Metric(name=name, label=info.get("label"), description=info.get("description"),
                              type=info.get("type"), suppression=info.get("suppression"),
                              group_by=[d for d, (_, e) in schema.DIMENSIONS.items() if e in ents],
                              filters=list(schema.FILTERS) if "encounter" in ents else [],
                              needs_grain=name in schema.NEEDS_GRAIN))
        return Catalog(metrics=out, grains=list(schema.GRAINS), max_group_by=schema.MAX_GROUP_BY,
                       filters={k: list(v) for k, (_, v) in schema.FILTERS.items()},
                       saved_queries=list(engine.saved))

    @app.get("/v1/metrics/query", response_model=Result, tags=["metrics"], dependencies=[Depends(require_key)],
             summary="Query metrics by allowlisted dimensions, grain, dates and filters",
             responses={401: {"description": "Missing or wrong key"},
                        422: {"description": "A metric, dimension, filter or value outside the allowlist"}})
    def query(
        metrics: str = Query(..., description="Comma separated public metric names",
                             openapi_examples={"admissions by source": {"value": "admissions,ed_visits"},
                                               "quality": {"value": "readmission_rate_30d,average_length_of_stay_days"}}),
        group_by: Optional[str] = Query(None, description=f"Up to {schema.MAX_GROUP_BY} of: {', '.join(schema.DIMENSIONS)}",
                                        openapi_examples={"source": {"value": "source"},
                                                          "source and age band": {"value": "source,age_band"}}),
        grain: Optional[str] = Query(None, description=f"Time grain: {', '.join(schema.GRAINS)}",
                                     openapi_examples={"month": {"value": "month"}}),
        start: Optional[str] = Query(None, description="First day, YYYY-MM-DD", openapi_examples={"start": {"value": "2026-08-01"}}),
        end: Optional[str] = Query(None, description="Last day, YYYY-MM-DD", openapi_examples={"end": {"value": "2026-10-31"}}),
        where: Optional[list[str]] = Query(None, description=("Filters as field:value, repeatable. Fields and values: "
                                                              + "; ".join(f"{k} ({', '.join(v)})" for k, (_, v) in schema.FILTERS.items())),
                                           openapi_examples={"live feed": {"value": ["is_live_feed:true"]}}),
        limit: Optional[int] = Query(None, description=f"Rows to return, 1 to {schema.MAX_LIMIT} (default {schema.DEFAULT_LIMIT})"),
    ):
        try:
            q = schema.build_query(metrics, group_by, grain, start, end, where, limit)
        except schema.Rejected as e:
            raise HTTPException(status_code=422, detail=str(e)) from None
        rows, snap = run(q["metrics"], q["group_by"], q["where"])
        return respond(q["metrics"], rows, snap, {**q["request"], "limit": q["limit"]}, q["limit"])

    @app.get("/v1/saved-queries/{name}", response_model=Result, tags=["saved queries"],
             dependencies=[Depends(require_key)], summary="Run a saved query of the semantic layer",
             responses={404: {"description": "Unknown saved query"}})
    def saved_query(name: str):
        if name not in engine.saved:
            raise HTTPException(status_code=404, detail=f"unknown saved query; known: {', '.join(engine.saved)}")
        try:
            metric_names, group_by, rows, snap = engine.run_saved(name)
        except NoSnapshot as e:
            raise HTTPException(status_code=503, detail=str(e)) from None
        except Exception as e:
            log.error("saved query failed: %s", type(e).__name__)
            raise HTTPException(status_code=400, detail="the semantic layer could not answer this query") from None
        return respond(metric_names, rows, snap, {"saved_query": name, "metrics": metric_names,
                                                  "group_by": [schema.column_name(x) for x in group_by],
                                                  "where": engine.saved[name].get("where", [])})

    return app
