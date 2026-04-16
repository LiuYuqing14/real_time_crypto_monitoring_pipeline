import json
import os
import signal
import sys
from datetime import datetime, timezone

from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "crypto_ticks_raw")

PRODUCTS = ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD", "LTC-USD"]
SYMBOL_NAMES = {
    "BTC-USD": "BTC",
    "ETH-USD": "ETH",
    "SOL-USD": "SOL",
    "XRP-USD": "XRP",
    "DOGE-USD": "DOGE",
    "LTC-USD": "LTC",
}

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=50,
)


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default



def on_message(ws, message):
    """Handle incoming Coinbase WebSocket messages and publish normalized ticks to Kafka."""
    try:
        data = json.loads(message)

        if data.get("type") != "ticker":
            return

        product_id = data.get("product_id", "")
        if product_id not in PRODUCTS:
            return

        price = _safe_float(data.get("price"))
        volume = _safe_float(data.get("last_size"))
        if price <= 0 or volume <= 0:
            return

        event_time = data.get("time") or datetime.now(timezone.utc).isoformat()
        symbol = SYMBOL_NAMES.get(product_id, product_id.replace("-USD", ""))

        tick = {
            "event_time": event_time,
            "symbol": symbol,
            "product_id": product_id,
            "price": round(price, 8),
            "volume": round(volume, 8),
            "source": "coinbase",
            "event_type": "ticker",
        }

        producer.send(KAFKA_TOPIC, tick)
        print(f"Sent {tick['symbol']} @ {tick['price']} ({tick['volume']})")

    except Exception as exc:
        print(f"Error processing message: {exc}")



def on_error(ws, error):
    print(f"WebSocket error: {error}")



def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed: {close_status_code} - {close_msg}")



def on_open(ws):
    print("WebSocket connected to Coinbase")
    subscribe_msg = {
        "type": "subscribe",
        "product_ids": PRODUCTS,
        "channels": ["ticker"],
    }
    ws.send(json.dumps(subscribe_msg))
    print(f"Subscribed to: {', '.join(PRODUCTS)}")



def main():
    import websocket

    print("=" * 60)
    print("Real-Time Crypto Producer (Coinbase -> Kafka)")
    print("=" * 60)
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}/{KAFKA_TOPIC}")
    print(f"Products: {', '.join(PRODUCTS)}")
    print("Press Ctrl+C to stop.")
    print("=" * 60)

    ws = websocket.WebSocketApp(
        "wss://ws-feed.exchange.coinbase.com",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    def signal_handler(sig, frame):
        print("\nShutting down producer...")
        ws.close()
        producer.flush()
        producer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    ws.run_forever(ping_interval=20, ping_timeout=10)


if __name__ == "__main__":
    main()
