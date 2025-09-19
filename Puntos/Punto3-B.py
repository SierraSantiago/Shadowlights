"""
API /metrics - Exposición de métricas CAC y ROAS con comparación de períodos

Cómo correr:
    uvicorn Punto3-B:app --reload
Acceso:
    http://127.0.0.1:8000/metrics?start=YYYY-MM-DD&end=YYYY-MM-DD

Ejemplo:
    http://127.0.0.1:8000/metrics?start=2025-02-15&end=2025-02-20
"""

from fastapi import FastAPI, Query
import duckdb
import pandas as pd
from datetime import datetime

app = FastAPI()
conn = duckdb.connect("../files/ads.duckdb")  

@app.get("/metrics")
def get_metrics(
    start: str = Query(..., description="Fecha inicial en formato YYYY-MM-DD"),
    end: str = Query(..., description="Fecha final en formato YYYY-MM-DD")
):
    
    period_days = (datetime.strptime(end, "%Y-%m-%d") - datetime.strptime(start, "%Y-%m-%d")).days + 1

    query = f"""
    WITH metrics AS (
        SELECT
            CAST(c.date AS DATE) AS date,
            c.cac,
            r.roas
        FROM cac c
        JOIN roas r USING (record_id, date)
    ),

    last_period AS (
        SELECT
            AVG(cac) AS cac,
            AVG(roas) AS roas
        FROM metrics
        WHERE date BETWEEN '{start}' AND '{end}'
    ),

    prev_period AS (
        SELECT
            AVG(cac) AS cac,
            AVG(roas) AS roas
        FROM metrics
        WHERE date BETWEEN DATE('{start}') - INTERVAL '{period_days} days'
                       AND DATE('{start}') - INTERVAL '1 day'
    ),

    comparison AS (
        SELECT
            'CAC' AS metric,
            l.cac AS last_value,
            p.cac AS prev_value,
            (l.cac - p.cac) AS delta_abs,
            CASE WHEN p.cac > 0 THEN (l.cac - p.cac) / p.cac * 100 ELSE NULL END AS delta_pct
        FROM last_period l, prev_period p

        UNION ALL

        SELECT
            'ROAS' AS metric,
            l.roas AS last_value,
            p.roas AS prev_value,
            (l.roas - p.roas) AS delta_abs,
            CASE WHEN p.roas > 0 THEN (l.roas - p.roas) / p.roas * 100 ELSE NULL END AS delta_pct
        FROM last_period l, prev_period p
    )

    SELECT * FROM comparison
    """

    df = conn.execute(query).fetchdf()
    return df.to_dict(orient="records")
