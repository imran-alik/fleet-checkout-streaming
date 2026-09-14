#!/usr/bin/env python
"""Interactive HTML dashboard from latest fleet certified run."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "data" / "evidence"
OUT = EVIDENCE / "dashboard.html"


def main() -> int:
    index = json.loads((EVIDENCE / "run_summary_index.json").read_text(encoding="utf-8"))
    artifact = EVIDENCE / Path(index["runs"][-1]["artifact"]).name
    payload = json.loads(artifact.read_text(encoding="utf-8"))
    kpis = payload.get("kpis", {})
    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8"/>
<title>Fleet Checkout Dashboard</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script></head>
<body style="font-family:system-ui;background:#0f172a;color:#e2e8f0;padding:2rem">
<h1>Fleet Checkout Streaming</h1>
<p>Redacted portfolio design · synthetic CDC · certified KPIs</p>
<div id="chart"></div>
<script>
Plotly.newPlot('chart', [{{
  type:'bar',
  x:['Events','Active rentals','Conversion %'],
  y:[{kpis.get('total_events',0)},{kpis.get('active_rentals',0)},{kpis.get('checkout_conversion_pct',0)}],
  marker:{{color:['#38bdf8','#a78bfa','#4ade80']}}
}}], {{paper_bgcolor:'#1e293b',plot_bgcolor:'#1e293b',font:{{color:'#e2e8f0'}}}});
</script></body></html>"""
    OUT.write_text(html, encoding="utf-8")
    print(json.dumps({"dashboard_html": str(OUT)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
