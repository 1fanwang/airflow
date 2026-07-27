import json, urllib.request
G = "http://localhost:3001"
uid = json.load(urllib.request.urlopen(f"{G}/api/datasources"))[0]["uid"]
ds = {"type": "prometheus", "uid": uid}
dash = {
  "dashboard": {
    "title": "AIP-97 - failure_kind metric tag",
    "panels": [
      {"id": 1, "title": "ti_failures by failure_kind (tagged)", "type": "barchart",
       "gridPos": {"h": 10, "w": 16, "x": 0, "y": 0}, "datasource": ds,
       "targets": [{"expr": "sum by (failure_kind) (airflow_ti_failures_total)",
                    "legendFormat": "{{failure_kind}}", "instant": True, "datasource": ds}]},
      {"id": 2, "title": "total ti_failures series (cardinality)", "type": "stat",
       "gridPos": {"h": 10, "w": 8, "x": 16, "y": 0}, "datasource": ds,
       "targets": [{"expr": "count(airflow_ti_failures_total)", "instant": True, "datasource": ds}]},
      {"id": 3, "title": "operator_failures by failure_kind", "type": "barchart",
       "gridPos": {"h": 9, "w": 24, "x": 0, "y": 10}, "datasource": ds,
       "targets": [{"expr": "sum by (failure_kind) (airflow_operator_failures_total)",
                    "legendFormat": "{{failure_kind}}", "instant": True, "datasource": ds}]},
    ],
    "time": {"from": "now-30m", "to": "now"}, "schemaVersion": 39, "refresh": "",
  }, "overwrite": True,
}
req = urllib.request.Request(f"{G}/api/dashboards/db", data=json.dumps(dash).encode(),
                            headers={"Content-Type": "application/json"})
print(json.load(urllib.request.urlopen(req))["url"])
