.output /dev/null
.timer on
create table kline (
  obj string,
  ts int,
  open double,
  high double,
  low double,
  close double,
  volume double,
  amount double
);

.separator ,
.import /home/jia/datas/tdx/shlday4.csv kline