create table kline (
  ts datetime,
  open Decimal64,
  high Decimal64,
  low Decimal64,
  close Decimal64,
  volume Decimal64,
  amount Decimal64
) with (
  chunkSize = 250,
  round='1d'
);

.import /home/jia/datas/tdx/shlday4.csv kline;