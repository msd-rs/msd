# Integrate with an Exchange Data Feed

To integrate a real-time data feed (e.g., from a crypto exchange or stock market), you can use the Python or TypeScript bindings, or simply make HTTP requests.

## Python Example

Loop through your data source and insert data points as they arrive.

```python
import msd_rs # Hypothetical binding or use requests

def on_market_data(tick):
    # tick: { symbol: "BTCUSDT", price: 50000.0, ... }
    sql = f"INSERT INTO tickers VALUES ('{tick['symbol']}', '{tick['ts']}', {tick['price']})"
    msd_rs.execute(sql)
```

## HTTP Example

Post updates to the `/query` endpoint.

```bash
curl -X POST http://localhost:50510/query -d "INSERT INTO tickers VALUES ('BTCUSDT', ...)"
```

MSD-RS is designed to handle high-frequency concurrent writes efficiently.
