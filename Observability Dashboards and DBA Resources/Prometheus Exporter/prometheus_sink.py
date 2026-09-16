"""
PrometheusSink for the Real Time DBSQL Warehouse Monitor.

This plugs into the monitor's existing Sink protocol, which is a single method,
`emit(events)`. On each poll the monitor hands us a batch of MetricEvent objects.
We update in-memory Prometheus gauges, labeled per warehouse, and a background HTTP
server exposes /metrics for Prometheus to scrape.

Design (scrape / pull):
  The exporter runs as a long-running service. It holds the latest gauge values in
  process memory and Prometheus scrapes the /metrics endpoint on its own interval.
  Nothing is pushed. Hitting /metrics does NOT trigger a Databricks API call, it just
  serializes whatever the poll loop last wrote into memory. Freshness is bounded by
  the monitor's poll interval, not by the scrape.

  (A push-to-Pushgateway variant is possible and noted in the README. Hinge confirmed
  on the Sep 16 sync that they also pull metrics, so scrape is the default here.)

The sink is intentionally decoupled from the monitor module: it reads event fields by
attribute (entity_id, workspace_host, monitor_name, ts_utc, metrics) so it does not
need to import the monitor notebook. Wire it in as just another Sink in the sinks list.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from prometheus_client import CollectorRegistry, Gauge, start_http_server


# Warehouse health arrives as a string. Encode it numerically so it can be graphed
# and alerted on. Lower is healthier; unknown values map to -1.
_HEALTH_ENCODING: Dict[str, float] = {
    "HEALTHY": 0.0,
    "DEGRADED": 1.0,
    "FAILED": 2.0,
}
_HEALTH_KEYS = {"warehouse_health", "health_status", "health"}

# Free-text metrics that are useful in logs but are not numeric gauges.
_SKIP_KEYS = {"queue_life_p95_sentence"}

_LABELS: Tuple[str, ...] = ("warehouse_id", "workspace_host", "monitor")


@dataclass
class PrometheusSink:
    """A Sink that exposes monitor metrics as Prometheus gauges over /metrics.

    Example:
        sink = PrometheusSink(namespace="dbsql", port=9877)
        sink.start_server()                       # start the /metrics endpoint once
        monitor = DatabricksSQLMonitor(settings=settings, sinks=[sink])
        MonitorRunner(monitors=[monitor], poll_interval_seconds=30).run_forever()
    """

    namespace: str = "dbsql"
    port: int = 9877
    registry: CollectorRegistry = field(default_factory=CollectorRegistry)

    # internal state
    _gauges: Dict[str, Gauge] = field(default_factory=dict, repr=False)
    _server_started: bool = field(default=False, repr=False)

    # ------------------------------------------------------------------ server
    def start_server(self, port: Optional[int] = None) -> None:
        """Start the /metrics HTTP endpoint. Safe to call once; later calls are no-ops."""
        if self._server_started:
            return
        start_http_server(port if port is not None else self.port, registry=self.registry)
        self._server_started = True

    # ------------------------------------------------------------ Sink protocol
    def emit(self, events: List[Any]) -> None:
        """Receive a batch of MetricEvents from the monitor and update gauges."""
        if not events:
            return

        for e in events:
            labels = {
                "warehouse_id": str(getattr(e, "entity_id", "") or ""),
                "workspace_host": str(getattr(e, "workspace_host", "") or ""),
                "monitor": str(getattr(e, "monitor_name", "") or ""),
            }
            metrics: Dict[str, Any] = getattr(e, "metrics", {}) or {}

            for key, value in metrics.items():
                if key in _SKIP_KEYS:
                    continue
                num = self._coerce(key, value)
                if num is None:
                    continue
                self._gauge(self._metric_name(key)).labels(**labels).set(num)

            # Freshness signal: when we last saw this warehouse. Alert on staleness
            # (now() - last_poll_unixtime) to catch an exporter that has stopped.
            ts = getattr(e, "ts_utc", None)
            last = ts.timestamp() if ts is not None else time.time()
            self._gauge(self._metric_name("last_poll_unixtime")).labels(**labels).set(last)

    # ------------------------------------------------------------------ helpers
    def _coerce(self, key: str, value: Any) -> Optional[float]:
        """Turn a metric value into a float gauge value, or None to skip it."""
        if isinstance(value, bool):
            return 1.0 if value else 0.0
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            if key in _HEALTH_KEYS:
                return _HEALTH_ENCODING.get(value.strip().upper(), -1.0)
            # other free-text strings (e.g. state) are not exported as gauges here.
            # TODO: encode `state` (RUNNING/STARTING/STOPPED) if you want to alert on it.
            return None
        return None

    def _metric_name(self, key: str) -> str:
        """Namespace + sanitize to a valid Prometheus metric name."""
        safe = "".join(c if (c.isalnum() or c == "_") else "_" for c in key)
        return f"{self.namespace}_{safe}"

    def _gauge(self, name: str) -> Gauge:
        g = self._gauges.get(name)
        if g is None:
            g = Gauge(
                name,
                f"DBSQL warehouse monitor metric: {name}",
                labelnames=_LABELS,
                registry=self.registry,
            )
            self._gauges[name] = g
        return g


# TODO (prod hardening, see README):
#   - Stale-series cleanup: drop label sets for warehouses no longer polled so a
#     removed warehouse does not linger at its last value.
#   - Auth: the monitor reads DATABRICKS_TOKEN today. Move to a service principal
#     (OAuth M2M) with token refresh for an unattended service.
#   - Resilience: wrap the monitor's poll loop in try/except with backoff.


if __name__ == "__main__":
    # Runnable demo with a synthetic event, so you can see /metrics working before
    # wiring the real monitor. Run this file, then: curl http://localhost:9877/metrics
    from datetime import datetime, timezone
    from types import SimpleNamespace

    sink = PrometheusSink(namespace="dbsql", port=9877)
    sink.start_server()

    demo_event = SimpleNamespace(
        monitor_name="databricks.warehouse",
        workspace_host="example.cloud.databricks.com",
        entity_id="abc123warehouse",
        ts_utc=datetime.now(timezone.utc),
        metrics={
            "queue_depth": 3,
            "queue_wait_seconds_p99": 4.2,
            "qps": 12.0,
            "runtime_seconds_p95": 8.5,
            "running_concurrency_p95": 6.0,
            "failure_rate_pct": 0.0,
            "spilled_query_pct": 1.5,
            "num_clusters": 2,
            "max_clusters": 4,
            "warehouse_health": "HEALTHY",
            "state": "RUNNING",
            "queue_life_p95_sentence": "ignored free text",
        },
    )

    print("Serving metrics at http://localhost:9877/metrics  (Ctrl-C to stop)")
    while True:
        sink.emit([demo_event])
        time.sleep(15)
