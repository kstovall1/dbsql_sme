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

A push-to-Pushgateway variant is possible (the sink would push each poll instead of serving `/metrics`). We chose scrape because the exporter is a long-running service Prometheus can just scrape, and the customer confirmed on the Sep 16 sync that they also pull metrics. Push only matters if the exporter cannot be scraped (for example if it ran as a short-lived Databricks job).

## Files

- `prometheus_sink.py` — the `PrometheusSink` class (implements the monitor's `emit(events)` Sink protocol) plus a `start_server()` helper and a runnable demo.
- `test_prometheus_sink.py` — unit tests over the schema mapping, health/state encoding, stale-series removal, and timestamp handling.
- `requirements.txt` — `prometheus-client` (the sink's only dependency; the monitor brings its own `requests` / `pandas`).

## Try it standalone

```bash
pip install -r requirements.txt
python "prometheus_sink.py"          # binds 127.0.0.1 in the demo
# then, in another shell:
curl http://127.0.0.1:9877/metrics
```

The demo emits one synthetic warehouse event (using the monitor's real keys) every 15s.

## Run the tests

```bash
python test_prometheus_sink.py       # standalone, or: pytest
```

## Wire it into the monitor

```python
from prometheus_sink import PrometheusSink

sink = PrometheusSink(namespace="dbsql", port=9877)
sink.start_server()                       # start /metrics once, at process start (0.0.0.0 for in-cluster scrape)

settings = DatabricksSQLMonitorSettings(
    databricks_token=os.environ["DATABRICKS_TOKEN"],
    poll_interval_seconds=30,
    warehouse_workspace_map={ "<warehouse_id>": "<workspace_host>" },
    # leave control_workspace_host / control_warehouse_id unset to skip the dynamic
    # p99 refresh (its SQL query needs a warehouse) and rely on the fixed long_lb_min_minutes
)

monitor = DatabricksSQLMonitor(settings=settings, sinks=[sink])
MonitorRunner(monitors=[monitor], poll_interval_seconds=settings.poll_interval_seconds).run_forever()
```

Note: the monitor currently lives as a Databricks notebook file, so it is not importable as-is. Extracting its classes (`MetricEvent`, `DatabricksSQLMonitor`, `DatabricksSQLMonitorSettings`, `MonitorRunner`) into a plain module is one of the prep steps below.

## Metrics and labels

Every numeric metric becomes a gauge named `dbsql_<metric_key>`, labeled by `warehouse_id`, `workspace_host`, and `monitor`. The keys are exactly what the monitor emits from `_compute_metrics`, `_snapshot_query_counts_from_history`, and `_fetch_warehouse_status`. Examples:

- Throughput: `dbsql_qps`, `dbsql_qpm`
- Live counts: `dbsql_current_running_queries`, `dbsql_current_queued_queries`
- Queue wait seconds: `dbsql_queued_p50_sec`, `dbsql_queued_p95_sec`, `dbsql_queued_p99_sec`, `dbsql_queued_p100_sec`
- Runtime seconds: `dbsql_runtime_p50_sec`, `dbsql_runtime_p90_sec`, `dbsql_runtime_p95_sec`, `dbsql_runtime_p99_sec`, `dbsql_runtime_p100_sec`
- Concurrency: `dbsql_running_concurrency_p95`, `dbsql_queued_concurrency_p95` (also p50/p99/p100)
- Queue-life percent: `dbsql_queue_life_pct_p95` (also p50/p99/p100)
- Health of the query mix: `dbsql_failure_rate_pct`, `dbsql_spilled_query_pct`
- Warehouse shape: `dbsql_warehouse_current_clusters`, `dbsql_warehouse_max_num_clusters`, `dbsql_warehouse_min_num_clusters`, `dbsql_warehouse_active_sessions`, `dbsql_warehouse_auto_stop_mins`

Encoded string fields:

- `dbsql_warehouse_health_status` from `warehouse_health_status`: `HEALTHY=0`, `DEGRADED=1`, `FAILED=2`, unknown/unreachable=`-1`. Alert on `>= 1`. It is set on every poll, so a failed status call (which omits the key) flips it to `-1` rather than leaving the last good value.
- `dbsql_warehouse_state` from `warehouse_state`: `STOPPED=0`, `STARTING=1`, `RUNNING=2`, `STOPPING=3`, `DELETING=4`, `DELETED=5`, unknown=`-1`.

Freshness:

- `dbsql_last_poll_unixtime` per warehouse. Alert on `time() - dbsql_last_poll_unixtime` to catch a stopped exporter, alongside the scrape `up` signal.

Not exported (free text): `warehouse_name`, `warehouse_size`, `warehouse_status_error`, `queue_life_p95_sentence`. `warehouse_name` could be added as a label later for readable Grafana legends; avoid putting `warehouse_size` in a label unless the value set is small.

Stale series are removed each poll: if a warehouse drops out of `warehouse_workspace_map` (or a metric stops being produced), its series is dropped rather than frozen at the last value.

## Prep before this goes to a customer (TODOs)

- [ ] Extract the monitor classes into an importable module (today it is a notebook).
- [ ] Auth: the monitor reads a static `DATABRICKS_TOKEN`. Move to a **service principal (OAuth M2M)** with token refresh, injected as a k8s secret. (This is the auth spec Reid asked for; the credential needs read access to query history and to each warehouse.)
- [ ] Resilience: wrap the monitor's `run_forever()` poll loop in try/except with backoff so an API blip or token expiry does not kill the process.
- [ ] Package for EKS: Dockerfile, keep `/metrics` off the public network, and a k8s Deployment + Service + ServiceMonitor so Prometheus discovers and scrapes it. (Rename this folder without spaces first, it is awkward in a Docker `COPY`.)
- [ ] Confirm per-cluster granularity is not needed (the APIs attribute queries to a warehouse, not to an internal autoscaling cluster).

## Licensing note

The upstream repo (`CodyAustinDavis/dbsql_sme`) has no license file. Clear redistribution with Cody before any derived code is handed to a customer.
