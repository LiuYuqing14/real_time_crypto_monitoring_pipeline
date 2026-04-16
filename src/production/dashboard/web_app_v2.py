"""FastAPI backend for the real-time crypto market monitoring dashboard (V2)."""

import argparse
import asyncio
import json
import os
from contextlib import asynccontextmanager

import clickhouse_connect
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.responses import StreamingResponse

parser = argparse.ArgumentParser()
parser.add_argument("--crypto", action="store_true", help="Use crypto dashboard")
parser.add_argument("--port", type=int, default=8502, help="Port to run on")
args, _ = parser.parse_known_args()

DASHBOARD_MODE = os.environ.get("DASHBOARD_MODE", "crypto" if args.crypto else "stock")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "stocks")
RAW_TABLE = os.environ.get("CLICKHOUSE_RAW_TABLE", "crypto_ticks_raw")
METRICS_TABLE = os.environ.get("CLICKHOUSE_AGG_TABLE", "crypto_metrics_1m")
ALERT_TABLE = os.environ.get("CLICKHOUSE_ALERT_TABLE", "crypto_alert_events")

client = None


def get_client():
    global client
    if client is None:
        client = clickhouse_connect.get_client(
            host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
            port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
            database=CLICKHOUSE_DATABASE,
        )
    return client


@asynccontextmanager
async def lifespan(app: FastAPI):
    get_client()
    yield
    global client
    if client:
        client.close()


app = FastAPI(title="Real-Time Crypto Market Monitor V2", lifespan=lifespan)

DASHBOARD_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(DASHBOARD_DIR, "static")
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def _in_clause(symbols: str, prefix: str = "WHERE"):
    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not symbol_list:
        return ""
    joined = ", ".join([f"'{s}'" for s in symbol_list])
    return f" {prefix} symbol IN ({joined})"


@app.get("/")
async def root():
    crypto_index = os.path.join(STATIC_DIR, "crypto_index.html")
    stock_index = os.path.join(STATIC_DIR, "stock_index.html")
    if DASHBOARD_MODE == "crypto" and os.path.exists(crypto_index):
        return FileResponse(crypto_index)
    if os.path.exists(stock_index):
        return FileResponse(stock_index)
    return {
        "message": "Crypto monitoring API is running.",
        "endpoints": [
            "/api/latest",
            "/api/history",
            "/api/metrics/latest",
            "/api/metrics/history",
            "/api/overview",
            "/api/movers",
            "/api/alerts",
            "/api/symbols",
        ],
    }


@app.get("/api/latest")
async def get_latest_prices(symbols: str = ""):
    symbol_filter = _in_clause(symbols)
    query = f"""
    SELECT
        symbol,
        argMax(price, event_time) AS price,
        argMax(volume, event_time) AS volume,
        argMax(product_id, event_time) AS product_id,
        max(event_time) AS last_update
    FROM {RAW_TABLE}
    {symbol_filter}
    GROUP BY symbol
    ORDER BY symbol
    """
    result = get_client().query(query)
    return [
        {
            "symbol": row[0],
            "price": row[1],
            "volume": row[2],
            "product_id": row[3],
            "last_update": str(row[4]),
        }
        for row in result.result_rows
    ]


@app.get("/api/history")
async def get_price_history(symbols: str = "", minutes: int = 60):
    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not symbol_list:
        return []

    symbols_str = ", ".join([f"'{s}'" for s in symbol_list])
    query = f"""
    SELECT event_time, symbol, price, volume
    FROM {RAW_TABLE}
    WHERE symbol IN ({symbols_str})
      AND event_time > now() - INTERVAL {minutes} MINUTE
    ORDER BY event_time
    """
    result = get_client().query(query)
    return [
        {
            "event_time": str(row[0]),
            "symbol": row[1],
            "price": row[2],
            "volume": row[3],
        }
        for row in result.result_rows
    ]


@app.get("/api/metrics/latest")
async def get_latest_metrics(symbols: str = ""):
    symbol_filter = _in_clause(symbols)
    query = f"""
    SELECT
        symbol,
        argMax(product_id, window_end) AS product_id,
        argMax(window_start, window_end) AS window_start,
        max(window_end) AS window_end,
        argMax(close_price, window_end) AS close_price,
        argMax(return_pct_1m, window_end) AS return_pct_1m,
        argMax(volatility_pct, window_end) AS volatility_pct,
        argMax(total_volume, window_end) AS total_volume,
        argMax(trade_count, window_end) AS trade_count,
        argMax(is_price_spike, window_end) AS is_price_spike,
        argMax(is_volume_anomaly, window_end) AS is_volume_anomaly,
        argMax(is_volatility_breakout, window_end) AS is_volatility_breakout
    FROM {METRICS_TABLE}
    {symbol_filter}
    GROUP BY symbol
    ORDER BY symbol
    """
    result = get_client().query(query)
    return [
        {
            "symbol": row[0],
            "product_id": row[1],
            "window_start": str(row[2]),
            "window_end": str(row[3]),
            "close_price": row[4],
            "return_pct_1m": row[5],
            "volatility_pct": row[6],
            "total_volume": row[7],
            "trade_count": row[8],
            "is_price_spike": bool(row[9]),
            "is_volume_anomaly": bool(row[10]),
            "is_volatility_breakout": bool(row[11]),
        }
        for row in result.result_rows
    ]


@app.get("/api/metrics/history")
async def get_metrics_history(symbols: str = "", minutes: int = 60):
    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not symbol_list:
        return []

    symbols_str = ", ".join([f"'{s}'" for s in symbol_list])
    query = f"""
    SELECT
        window_start,
        window_end,
        symbol,
        close_price,
        return_pct_1m,
        volatility_pct,
        total_volume,
        trade_count,
        is_price_spike,
        is_volume_anomaly,
        is_volatility_breakout
    FROM {METRICS_TABLE} FINAL
    WHERE symbol IN ({symbols_str})
      AND window_end > now() - INTERVAL {minutes} MINUTE
    ORDER BY window_end
    """
    result = get_client().query(query)
    return [
        {
            "window_start": str(row[0]),
            "window_end": str(row[1]),
            "symbol": row[2],
            "close_price": row[3],
            "return_pct_1m": row[4],
            "volatility_pct": row[5],
            "total_volume": row[6],
            "trade_count": row[7],
            "is_price_spike": bool(row[8]),
            "is_volume_anomaly": bool(row[9]),
            "is_volatility_breakout": bool(row[10]),
        }
        for row in result.result_rows
    ]


@app.get("/api/overview")
async def get_overview():
    query = f"""
    WITH latest AS (
        SELECT
            symbol,
            argMax(return_pct_1m, window_end) AS return_pct_1m,
            argMax(volatility_pct, window_end) AS volatility_pct,
            argMax(total_volume, window_end) AS total_volume
        FROM {METRICS_TABLE}
        GROUP BY symbol
    ),
    active_alerts AS (
        SELECT count() AS cnt
        FROM {ALERT_TABLE}
        WHERE event_time > now() - INTERVAL 5 MINUTE
    )
    SELECT
        (SELECT count() FROM latest) AS tracked_symbols,
        (SELECT cnt FROM active_alerts) AS active_alerts,
        argMax(symbol, abs(return_pct_1m)) AS top_mover_symbol,
        max(abs(return_pct_1m)) AS top_mover_return_pct,
        argMax(symbol, volatility_pct) AS highest_vol_symbol,
        max(volatility_pct) AS highest_volatility_pct,
        sum(total_volume) AS total_volume_last_window
    FROM latest
    """
    row = get_client().query(query).result_rows[0]
    return {
        "tracked_symbols": row[0],
        "active_alerts_last_5m": row[1],
        "top_mover_symbol": row[2],
        "top_mover_return_pct": row[3],
        "highest_vol_symbol": row[4],
        "highest_volatility_pct": row[5],
        "total_volume_last_window": row[6],
    }


@app.get("/api/movers")
async def get_top_movers(limit: int = 10):
    query = f"""
    SELECT
        symbol,
        argMax(close_price, window_end) AS close_price,
        argMax(return_pct_1m, window_end) AS return_pct_1m,
        argMax(volatility_pct, window_end) AS volatility_pct,
        argMax(total_volume, window_end) AS total_volume,
        max(window_end) AS last_window_end
    FROM {METRICS_TABLE}
    GROUP BY symbol
    ORDER BY abs(return_pct_1m) DESC
    LIMIT {limit}
    """
    result = get_client().query(query)
    return [
        {
            "symbol": row[0],
            "close_price": row[1],
            "return_pct_1m": row[2],
            "volatility_pct": row[3],
            "total_volume": row[4],
            "last_window_end": str(row[5]),
        }
        for row in result.result_rows
    ]


@app.get("/api/alerts")
async def get_alerts(symbols: str = "", limit: int = 25):
    symbol_filter = _in_clause(symbols, prefix="AND")
    query = f"""
    SELECT
        event_time,
        symbol,
        product_id,
        alert_type,
        severity,
        metric_value,
        threshold,
        window_start,
        window_end
    FROM {ALERT_TABLE}
    WHERE 1=1
    {symbol_filter}
    ORDER BY event_time DESC
    LIMIT {limit}
    """
    result = get_client().query(query)
    return [
        {
            "event_time": str(row[0]),
            "symbol": row[1],
            "product_id": row[2],
            "alert_type": row[3],
            "severity": row[4],
            "metric_value": row[5],
            "threshold": row[6],
            "window_start": str(row[7]),
            "window_end": str(row[8]),
        }
        for row in result.result_rows
    ]


@app.get("/api/symbols")
async def get_symbols():
    result = get_client().query(f"SELECT DISTINCT symbol FROM {RAW_TABLE} ORDER BY symbol")
    return [row[0] for row in result.result_rows]


@app.get("/api/stream")
async def stream_ticks(symbols: str = ""):
    async def event_generator():
        last_ts = None
        while True:
            try:
                symbol_filter = _in_clause(symbols, prefix="AND")
                ts_filter = f" AND event_time > '{last_ts}'" if last_ts else ""
                query = f"""
                SELECT event_time, symbol, price, volume
                FROM {RAW_TABLE}
                WHERE 1=1 {symbol_filter} {ts_filter}
                ORDER BY event_time DESC
                LIMIT 50
                """
                rows = get_client().query(query).result_rows
                if rows:
                    last_ts = str(rows[0][0])
                    payload = [
                        {
                            "event_time": str(r[0]),
                            "symbol": r[1],
                            "price": r[2],
                            "volume": r[3],
                        }
                        for r in reversed(rows)
                    ]
                    yield f"data: {json.dumps(payload)}\n\n"
            except Exception as exc:
                yield f"data: {json.dumps({'error': str(exc)})}\n\n"
            await asyncio.sleep(1)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=args.port)
