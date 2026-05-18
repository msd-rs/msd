

create table stock_kline_1d (
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

create table stock_kline_1m (
  ts datetime,
  open double,
  high double,
  low double,
  close double,
  volume double,
  amount double
) with (
  chunkSize = 250,
  round='1m'
);

create table stock_snapshot (
  ts datetime,
  open double,
  high double,
  low double,
  close double,
  volume double,
  amount double,
  pre_close double
) with (
  chunkSize = 500,
  chan = 'stock_kline_1d,stock_kline_1m:ts,changed_if(open, close),changed_if(high, close),changed_if(low, close),close,volume,amount'
);

