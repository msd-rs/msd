import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
import numpy as np
import pymsd


def test_query_concat_single_table():
  content1 = pymsd.pack_dataframe("SH600000", [("val", np.array([10, 20], dtype=np.int64))])
  content2 = pymsd.pack_dataframe("SZ000001", [("val", np.array([30, 40, 50], dtype=np.int64))])
  full_body = content1 + content2

  class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
      self.send_response(200)
      self.send_header("Content-Type", "application/octet-stream")
      self.send_header("Content-Length", str(len(full_body)))
      self.end_headers()
      self.wfile.write(full_body)

    def log_message(self, format, *args):
      pass

  server = HTTPServer(("127.0.0.1", 0), Handler)
  port = server.server_port
  t = threading.Thread(target=server.serve_forever)
  t.daemon = True
  t.start()

  try:
    conn = pymsd.MsdConnection(f"http://127.0.0.1:{port}")
    res = conn.query_concat("SELECT * FROM quote")
    assert "" in res
    table, ranges = res[""]

    assert table.rows == 5
    assert table.columns == 1
    assert ranges == [("SH600000", 0, 2), ("SZ000001", 2, 5)]

    d = table.to_dict()
    assert np.array_equal(d["val"], np.array([10, 20, 30, 40, 50]))
  finally:
    server.shutdown()

