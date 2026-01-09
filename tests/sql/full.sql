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

comment on table stock_kline_1d is 'daily kline';
comment on column stock_kline_1d.ts is 'timestamp';
comment on column stock_kline_1d.open is 'open price, 开盘价, 单位 元';
comment on column stock_kline_1d.high is 'high price, 最高价, 单位 元';
comment on column stock_kline_1d.low is 'low price, 最低价, 单位 元';
comment on column stock_kline_1d.close is 'close price, 收盘价, 单位 元';
comment on column stock_kline_1d.volume is 'volume, 成交量, 单位 股';
comment on column stock_kline_1d.amount is 'amount, 成交额, 单位 元';

create table stock_shares (
  ts datetime,
  total double,
  tradable double
) with (
  chunkSize = 50
);

comment on table stock_shares is 'stock shares, 股本变动';
comment on column stock_shares.ts is 'timestamp';
comment on column stock_shares.total is 'total shares, 总股本, 单位 股';
comment on column stock_shares.tradable is 'tradable shares, 流通股本, 单位 股';


create table stock_dividend (
  ts datetime,
  bonus double,
  transfers double,
  dividend double,
  rightShare double,
  rightPrice double
) with (
  chunkSize = 50
);

comment on table stock_dividend is 'stock dividend, 分红送配';
comment on column stock_dividend.ts is 'timestamp';
comment on column stock_dividend.bonus is 'bonus shares per 10 shares, 每10股送';
comment on column stock_dividend.transfers is 'transfer shares per 10 shares, 每10股转';
comment on column stock_dividend.dividend is 'cash dividend per 10 shares, 每10股派息';
comment on column stock_dividend.rightShare is 'right shares per 10 shares, 每10股配股';
comment on column stock_dividend.rightPrice is 'right price, 配股价格';



create table stock_financial_basic (
  ts datetime,
  eps_basic double,
  eps_diluted double,
  eps_deduct double,
  bps double,
  net_profit double,
  np_parent_growth double,
  net_profit_deduct double,
  total_revenue double,
  revenue_growth double,
  ocfps double  ,
  undist_profit_ps double,
  capital_reserve_ps double,
  gross_margin double,
  sales_cost_rate double,
  net_margin double,
  op_margin double,
  roe_diluted double,
  roe double,
  debt_asset_ratio double
) with (
  chunkSize = 50
);

comment on table stock_financial_basic is 'stock financial basic, 基本财务简表, 每季度更新一次';
comment on column stock_financial_basic.ts is 'timestamp, 日期 单位 季度 03-31, 06-30, 09-30, 12-31';
comment on column stock_financial_basic.eps_basic is 'basic earnings per share, 基本每股收益, 单位 元';
comment on column stock_financial_basic.eps_diluted is 'diluted earnings per share, 摊薄每股收益, 单位 元';
comment on column stock_financial_basic.eps_deduct is 'adjusted earnings per share, 扣非每股收益, 单位 元';
comment on column stock_financial_basic.bps is 'book value per share, 每股净资产, 单位 元';
comment on column stock_financial_basic.net_profit is 'net profit, 净利润, 单位 元';
comment on column stock_financial_basic.np_parent_growth is 'net profit growth, 净利润增长率, 单位 无';
comment on column stock_financial_basic.net_profit_deduct is 'adjusted net profit, 扣非净利润, 单位 元';
comment on column stock_financial_basic.total_revenue is 'total revenue, 营业总收入, 单位 元';
comment on column stock_financial_basic.revenue_growth is 'revenue growth, 营业总收入增长率, 单位 无';
comment on column stock_financial_basic.ocfps is 'operating cash flow per share, 每股经营现金流量, 单位 元';
comment on column stock_financial_basic.undist_profit_ps is 'undistributed profit per share, 每股未分配利润, 单位 元';
comment on column stock_financial_basic.capital_reserve_ps is 'capital reserve per share,每股资本公积金, 单位 元';
comment on column stock_financial_basic.gross_margin is 'gross margin,销售毛利率, 单位 无';
comment on column stock_financial_basic.sales_cost_rate is 'sales cost rate,销售成本率, 单位 无';
comment on column stock_financial_basic.net_margin is 'net margin,销售净利率, 单位 无';
comment on column stock_financial_basic.op_margin is 'operating margin,营业利润率, 单位 无';
comment on column stock_financial_basic.roe_diluted is 'return on equity,摊薄净资产收益率, 单位 无';
comment on column stock_financial_basic.roe is 'return on equity,净资产收益率, 单位 无';
comment on column stock_financial_basic.debt_asset_ratio is 'debt asset ratio,资产负债率, 单位 无';


.import tests/data/stock_financial_basic.csv stock_financial_basic;
.import tests/data/stock_kline_1d.csv stock_kline_1d;
.import tests/data/stock_shares.csv stock_shares;
.import tests/data/stock_dividend.csv stock_dividend;