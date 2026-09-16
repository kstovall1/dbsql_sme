# DBSQL Warehouse Metrics, Prometheus Exporter

A Prometheus sink for the **Real Time DBSQL Warehouse Monitor** (`../Real Time DBSQL Warehouse Monitor - v1.py`). It turns the monitor's per-warehouse metrics into Prometheus gauges and exposes them on a `/metrics` endpoint that Prometheus scrapes.

## Why this exists

The monitor already polls the Databricks Query History and Warehouses APIs and computes per-warehouse metrics, then hands them to pluggable sinks. It ships with Console, Datadog, and Delta sinks. This adds a **PrometheusSink** so the same metrics land in a Prometheus + Grafana stack.

## Design (scrape, not push)

The exporter runs as a long-running service. On its own interval (15-30s) it polls the two APIs, computes metrics, and writes them into in-memory gauges. Prometheus scrapes `/metrics` on its own interval and stores the samples. Nothing is pushed, and hitting `/metrics` does not trigger an API call, it just serializes the latest in-memory values.

```
Query History API + Warehouses API
        |  (poll every 15-30s, control-plane REST, no warehouse compute)
        v
metrics exporter  (this + the monitor)   <- runs in the customer's EKS
        |  exposes /metrics
        v
Prometheus  --scrape-->  Grafana
```

A push-to-Pushgateway variant is possible (the sink would push each poll instead of serving `/metrics`). We chose scrape because the exporter is a long-running service Prometheus can just scrape, and the customer confirmed they also pull metrics. Push only makes sense if the exporter cannot be scraped (for example if it ran as a short-lived Databricks job).

## Files

- `prometheus_sink.py` — the `PrometheusSink` class (implements the monitor's `emit(events)` Sink protocol) plus a `start_server()` helper and a runnable demo.
- `requirements.txt` — `prometheus-client`, `requests`, `pandas`.

## Try it standalone

```bash
pip install -r requirements.txt
python "prometheus_sink.py"
# then, in another shell:
curl http://localhost:9877/metrics
```

The demo emits one synthetic warehouse event every 15s so you can see the gauges render before wiring the real monitor.

## Wire it into the monitor

```python
from prometheus_sink import PrometheusSink

sink = PrometheusSink(namespace="dbsql", port=9877)
sink.start_server()                       # start /metrics once, at process start

settings = DatabricksSQLMonitorSettings(
    databricks_token=os.environ["DATABRICKS_TOKEN"],
    poll_interval_seconds=30,
    warehouse_workspace_map={ "<warehouse_id>": "<workspace_host>" },
    # use a fixed lookback to avoid the control-warehouse dependency:
    # leave control_workspace_host / control_warehouse_id unset and rely on long_lb_min_minutes
)

monitor = DatabricksSQLMonitor(settings=settings, sinks=[sink])
MonitorRunner(monitors=[monitor], poll_interval_seconds=settings.poll_interval_seconds).run_forever()
```

Note: the monitor currently lives as a Databricks notebook file, so it is not importable as-is. Extracting its classes (`MetricEvent`, `DatabricksSQLMonitor`, `DatabricksSQLMonitorSettings`, `MonitorRunner`) into a plain module is one of the prep steps below.

## Metrics and labels

Every numeric metric becomes a gauge named `dbsql_<metric_key>`, labeled by `warehouse_id`, `workspace_host`, and `monitor`. Examples: `dbsql_queue_depth`, `dbsql_queue_wait_seconds_p99`, `dbsql_qps`, `dbsql_runtime_seconds_p95`, `dbsql_running_concurrency_p95`, `dbsql_failure_rate_pct`, `dbsql_num_clusters`, `dbsql_max_clusters`.

- `warehouse_health` (string) is encoded numerically as `dbsql_warehouse_health`: `HEALTHY=0`, `DEGRADED=1`, `FAILED=2`, unknown=`-1`. Alert on `>= 1`.
- `dbsql_last_poll_unixtime` is a freshness signal per warehouse. Alert on `time() - dbsql_last_poll_unixtime` to catch an exporter that has stopped, since the scrape-`up` signal already covers a dead process.

## Prep before this goes to a customer (TODOs)

- [ ] Extract the monitor classes into an importable module (today it is a notebook).
- [ ] Auth: the monitor reads a static `DATABRICKS_TOKEN`. Move to a **service principal (OAuth M2M)** with token refresh, injected as a k8s secret. (This is the auth spec Reid asked for; the credential needs read access to query history and to each warehouse.)
- [ ] Resilience: wrap the monitor's `run_forever()` poll loop in try/except with backoff so an API blip or token expiry does not kill the process.
- [ ] Stale-series cleanup in the sink: drop label sets for warehouses no longer polled.
- [ ] Package for EKS: Dockerfile, expose the metrics port, and a k8s Deployment + Service + ServiceMonitor so Prometheus discovers and scrapes it.
- [ ] Confirm per-cluster granularity is not needed (the APIs attribute queries to a warehouse, not to an internal autoscaling cluster).

## Licensing note

The upstream repo (`CodyAustinDavis/dbsql_sme`) has no license file. Clear redistribution with Cody before any derived code is handed to a customer.
