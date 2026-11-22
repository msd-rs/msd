import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import threading

import numpy as np
import pandas as pd
import polars as pl
import pymsd


def gen_datetime(start: str, count: int) -> np.ndarray:
  start_dt = int(datetime.datetime.fromisoformat(start).timestamp() * 1000)
  oneday = 24 * 60 * 60 * 1000
  return np.array(
    [start_dt + i * oneday for i in range(count)],
    dtype="datetime64[ms]",
  )


def gen_data(base: float, count: int) -> np.ndarray:
  return np.array([base + i for i in range(count)], dtype=np.float64)


def unpack_all_frames(buf: bytes) -> list[tuple[str, str, dict[str, np.ndarray]]]:
  frames = []
  pos = 0
  while pos < len(buf):
    size = pymsd.check_table_frame(buf[pos : pos + 8])
    frame_end = pos + 8 + size
    table_name, obj, cols = pymsd.parse_table_frame(buf[pos + 8 : frame_end])
    frames.append((table_name, obj, dict(cols)))
    pos = frame_end
  return frames


def test_pack_table_frame() -> None:
  cols = [
    ("ts", gen_datetime("2020-01-01", 10)),
    ("open", gen_data(0, 10)),
    ("high", gen_data(10, 10)),
    ("low", gen_data(20, 10)),
    ("close", gen_data(40, 10)),
    ("volume", gen_data(50, 10)),
    ("amount", gen_data(60, 10)),
    ("tag", np.array([f"t{i}" for i in range(10)])),
  ]
  content = pymsd.pack_table_frame(
    "test", cols, {"open": 2, "high": 2, "low": 2, "close": 2}
  )
  frames = unpack_all_frames(content)
  assert len(frames) == 1
  table_name, obj, parsed = frames[0]
  assert table_name == ""
  assert obj == "test"
  assert len(parsed) == 8
  assert len(parsed["ts"]) == 10
  assert list(parsed["tag"]) == [f"t{i}" for i in range(10)]


def test_import_dataframes_native() -> None:
  received_bodies: list[bytes] = []

  class Handler(BaseHTTPRequestHandler):
    def do_PUT(self):
      length = self.headers.get("Content-Length")
      if length is not None:
        body = self.rfile.read(int(length))
      else:
        chunks = []
        while True:
          line = self.rfile.readline().strip()
          if not line:
            continue
          chunk_len = int(line, 16)
          if chunk_len == 0:
            self.rfile.readline()
            break
          chunks.append(self.rfile.read(chunk_len))
          self.rfile.readline()
        body = b"".join(chunks)
      received_bodies.append(body)
      resp = json.dumps({"total_rows": 10, "time_used_ms": 1, "rows_per_sec": 10000}).encode()
      self.send_response(200)
      self.send_header("Content-Type", "application/json")
      self.send_header("Content-Length", str(len(resp)))
      self.end_headers()
      self.wfile.write(resp)

    def log_message(self, format, *args):
      pass

  server = HTTPServer(("127.0.0.1", 0), Handler)
  port = server.server_port
  t = threading.Thread(target=server.serve_forever, daemon=True)
  t.start()

  try:
    conn = pymsd.MsdConnection(f"http://127.0.0.1:{port}")

    df_pd_indexed = pd.DataFrame(
      {
        "ts": gen_datetime("2020-01-01", 4),
        "close": gen_data(10, 4),
        "tag": ["a", "b", "c", "d"],
        "seq": np.array([1, 2, 3, 4], dtype=np.uint64),
      }
    ).set_index("ts")

    df_pl = pl.DataFrame(
      {
        "ts": gen_datetime("2020-01-01", 4),
        "close": gen_data(20, 4),
        "tag": ["e", "f", "g", "h"],
        "seq": np.array([5, 6, 7, 8], dtype=np.uint64),
      }
    )

    raw_cols = [
      ("ts", gen_datetime("2020-01-01", 4)),
      ("close", gen_data(30, 4)),
      ("tag", ["i", "j", "k", "l"]),
      ("seq", np.array([9, 10, 11, 12], dtype=np.uint64)),
    ]

    res = conn.import_dataframes(
      "kline",
      [("PD001", df_pd_indexed), ("PL001", df_pl), ("RAW001", raw_cols)],
      decimal_fields={"close": 2},
    )
    assert res["total_rows"] == 10

    # Also test single (obj, df) tuple
    res_single = conn.import_dataframe("kline", ("SINGLE001", df_pl), {"close": 2})
    assert res_single["total_rows"] == 10

    assert len(received_bodies) == 2
    batch_frames = unpack_all_frames(received_bodies[0])
    assert [obj for _, obj, _ in batch_frames] == ["PD001", "PL001", "RAW001"]
    assert list(batch_frames[0][2]["tag"]) == ["a", "b", "c", "d"]
    assert list(batch_frames[1][2]["tag"]) == ["e", "f", "g", "h"]
    assert list(batch_frames[2][2]["tag"]) == ["i", "j", "k", "l"]
    assert np.allclose(batch_frames[0][2]["close"], [10.0, 11.0, 12.0, 13.0])
    assert np.array_equal(batch_frames[1][2]["seq"], np.array([5, 6, 7, 8], dtype=np.uint64))

    single_frames = unpack_all_frames(received_bodies[1])
    assert len(single_frames) == 1
    assert single_frames[0][1] == "SINGLE001"
  finally:
    server.shutdown()
