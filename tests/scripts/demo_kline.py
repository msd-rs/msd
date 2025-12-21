import datetime
import random
import argparse



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

def datetime_str(ts: int) -> str:
  return datetime.datetime.fromtimestamp(ts / 1_000_000.0).strftime('%Y-%m-%dT%H:%M:%S.%f')
    

def generate_kline(obj: str, start_ts: int, rows: int, ts_interval: int, mode: str):
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
    if mode == "csv":
      print(f"{obj},{datetime_str(ts)},{open:.2f},{high:.2f},{low:.2f},{close:.2f},{volume},{amount:.2f}")
    elif mode == "tdsql":
      print(f'("{obj}", "{datetime_str(ts)}", {open:.2f}, {high:.2f}, {low:.2f}, {close:.2f}, {volume}, {amount:.2f})', end=' ')


def generate_codes(count: int, prefix: str = 'SH') -> list[str]:
  max_len = 8
  width = max_len - len(prefix)
  return [f"{prefix}{i:0{width}d}" for i in range(count)]


def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument("-n", "--code-count", type=int, default=10)
  parser.add_argument("-r", "--rows-count", type=int, default=10)
  parser.add_argument("-p", "--code-prefix", type=str, default="SH")
  parser.add_argument("-s", "--start-ts", type=str, default="1990-01-01")
  parser.add_argument("-d", "--ts-interval", type=str, default="1d")
  parser.add_argument("-o", "--output", type=str, default="demo.csv")
  parser.add_argument("-m", "--mode", type=str, default="csv", choices=["csv", "tdsql"])
  parser.add_argument("-H", "--header", type=str, default="")
  return parser.parse_args()

def read_file_as_str(fname: str) -> str:
  with open(fname, 'r') as f:
    return f.read()
    
if __name__ == '__main__':
  args = parse_args()
  header = read_file_as_str(args.header) if len(args.header) > 0 else ""
  codes = generate_codes(args.code_count, args.code_prefix)
  start_ts = parse_datetime(args.start_ts)
  ts_interval = parse_duration(args.ts_interval)

  print(header, end='')
  for code in codes:
    generate_kline(code, start_ts, args.rows_count, ts_interval, args.mode)
  if args.mode == "tdsql":
    print(';')