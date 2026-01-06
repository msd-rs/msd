create table kline (
  ts datetime,
  open double,
  high double,
  low double,
  close double,
  volume double,
  amount double
) with (
  chunkSize = 250,
  round='1d'
);

.import dev/demo.csv kline;