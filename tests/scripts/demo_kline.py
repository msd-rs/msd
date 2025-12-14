import time
import datetime
import random



def parse_datetime(ts: str) -> int:
  kind = len(ts.split(' '))
  if kind == 1:
    ts += ' 00:00:00 000000'
  elif kind == 2:
    ts += ' 000000'
  return int(datetime.datetime.strptime(ts, '%Y-%m-%d %H:%M:%S %f').timestamp() * 1_000_000)

def parse_duration(d: str) -> int:
  if d.endswith('us'):
    return int(d[:-2])
  if d.endswith('ms'):
    return int(d[:-2]) * 1_000
  if d.endswith('s'):
    return int(d[:-1]) * 1_000_000
  if d.endswith('m'):
    return int(d[:-1]) * 60 * 1_000_000
  if d.endswith('h'):
    return int(d[:-1]) * 60 * 60 * 1_000_000
  if d.endswith('d'):
    return int(d[:-1]) * 60 * 60 * 24 * 1_000_000
  raise ValueError(f"Invalid duration: {d}")
    

def generate_kline(obj: str, start_ts: int, rows: int, ts_interval: int):
  price = float(random.randint(1_00, 1000_00)) / 100.0
  for i in range(rows):
    ts = start_ts + i * ts_interval
    prices = [price *( 1+(random.randint(0, 20) - 10) / 100.0) for _ in range(4)]
    open = prices[0]
    high = max(prices)
    low = min(prices)
    close = prices[-1]
    volume = random.randint(10_000, 10_000_000)
    avg = sum(prices) / len(prices)
    amount = (volume * avg) / 1_0000.0
    price = avg
    print(f"{obj},{ts},{open:.2f},{high:.2f},{low:.2f},{close:.2f},{volume},{amount:.2f}")


def generate_codes(count: int, prefix: str = 'SH') -> list[str]:
  return [f"{prefix}{i:06d}" for i in range(count)]

    
if __name__ == '__main__':
  codes = generate_codes(10, "SH6")
  start_ts = parse_datetime('1990-01-01')
  ts_interval = parse_duration('1d')
  for code in codes:
    generate_kline(code, start_ts, 100, ts_interval)