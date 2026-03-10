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

CREATE TABLE stock_financial (
    ts DATETIME,
    eps DOUBLE,
    deducted_eps DOUBLE,
    upps DOUBLE,
    bps DOUBLE,
    crps DOUBLE,
    roe DOUBLE,
    ocfps DOUBLE,
    total_assets DOUBLE,
    revenue DOUBLE,
    operating_cost DOUBLE,
    operating_profit DOUBLE,
    total_profit DOUBLE,
    net_profit DOUBLE,
    np_attributable_parent DOUBLE,
    current_ratio DOUBLE,
    quick_ratio DOUBLE,
    cash_ratio DOUBLE,
    ncl_ratio DOUBLE,
    cl_ratio DOUBLE,
    debt_to_net_tangible_assets DOUBLE,
    ebitda_to_total_liabilities DOUBLE,
    ocf_to_total_liabilities DOUBLE,
    revenue_growth_rate DOUBLE,
    net_profit_growth_rate DOUBLE,
    total_assets_growth_rate DOUBLE,
    net_assets_growth_rate DOUBLE,
    op_growth_rate DOUBLE,
    deducted_eps_yoy DOUBLE,
    deducted_np_yoy DOUBLE,
    cost_expense_profit_margin DOUBLE,
    op_margin DOUBLE,
    net_profit_margin DOUBLE,
    roe_profitability DOUBLE,
    roa DOUBLE,
    gross_profit_margin DOUBLE,
    ebit DOUBLE,
    ebitda DOUBLE,
    ebitda_to_revenue DOUBLE,
    debt_to_assets_ratio DOUBLE,
    op_cash_flow_per_share DOUBLE,
    net_cash_flow_per_share DOUBLE,
    oc_ratio_short_debt DOUBLE,
    oc_ratio_total_debt DOUBLE,
    total_share_capital DOUBLE,
    tradable_a_shares DOUBLE,
    institutional_holdings DOUBLE,
    national_team_holdings DOUBLE
) with (
  chunkSize = 50
);

COMMENT ON COLUMN stock_financial.ts IS 'timestamp, 日期 单位 季度 03-31, 06-30, 09-30, 12-31';
COMMENT ON COLUMN stock_financial.eps IS '基本每股收益 (Basic EPS)';
COMMENT ON COLUMN stock_financial.deducted_eps IS '扣除非经常性损益每股收益 (Deducted EPS)';
COMMENT ON COLUMN stock_financial.upps IS '每股未分配利润 (Undistributed Profit Per Share)';
COMMENT ON COLUMN stock_financial.bps IS '每股净资产 (Book Value Per Share)';
COMMENT ON COLUMN stock_financial.crps IS '每股资本公积金 (Capital Reserve Per Share)';
COMMENT ON COLUMN stock_financial.roe IS '净资产收益率 (Return on Equity)';
COMMENT ON COLUMN stock_financial.ocfps IS '每股经营现金流量 (Operating Cash Flow Per Share)';
COMMENT ON COLUMN stock_financial.total_assets IS '资产总计 (Total Assets)';
COMMENT ON COLUMN stock_financial.revenue IS '营业收入 (Operating Revenue)';
COMMENT ON COLUMN stock_financial.operating_cost IS '营业成本 (Operating Cost)';
COMMENT ON COLUMN stock_financial.operating_profit IS '营业利润 (Operating Profit)';
COMMENT ON COLUMN stock_financial.total_profit IS '利润总额 (Total Profit / EBT)';
COMMENT ON COLUMN stock_financial.net_profit IS '净利润 (Net Profit)';
COMMENT ON COLUMN stock_financial.np_attributable_parent IS '归属于母公司所有者的净利润';
COMMENT ON COLUMN stock_financial.current_ratio IS '流动比率 (Current Ratio - 非金融类)';
COMMENT ON COLUMN stock_financial.quick_ratio IS '速动比率 (Quick Ratio - 非金融类)';
COMMENT ON COLUMN stock_financial.cash_ratio IS '现金比率 (Cash Ratio - 非金融类)';
COMMENT ON COLUMN stock_financial.ncl_ratio IS '非流动负债比率 (Non-current Liabilities Ratio)';
COMMENT ON COLUMN stock_financial.cl_ratio IS '流动负债比率 (Current Liabilities Ratio)';
COMMENT ON COLUMN stock_financial.debt_to_net_tangible_assets IS '有形资产净值债务率';
COMMENT ON COLUMN stock_financial.ebitda_to_total_liabilities IS 'EBITDA除以负债合计';
COMMENT ON COLUMN stock_financial.ocf_to_total_liabilities IS '经营活动产生的现金流量净额除以负债合计';
COMMENT ON COLUMN stock_financial.revenue_growth_rate IS '营业收入增长率 (Revenue Growth Rate)';
COMMENT ON COLUMN stock_financial.net_profit_growth_rate IS '净利润增长率 (Net Profit Growth Rate)';
COMMENT ON COLUMN stock_financial.total_assets_growth_rate IS '总资产增长率';
COMMENT ON COLUMN stock_financial.net_assets_growth_rate IS '净资产增长率';
COMMENT ON COLUMN stock_financial.op_growth_rate IS '营业利润增长率';
COMMENT ON COLUMN stock_financial.deducted_eps_yoy IS '扣非每股收益同比 (YoY)';
COMMENT ON COLUMN stock_financial.deducted_np_yoy IS '扣非净利润同比 (YoY)';
COMMENT ON COLUMN stock_financial.cost_expense_profit_margin IS '成本费用利润率';
COMMENT ON COLUMN stock_financial.op_margin IS '营业利润率 (Operating Profit Margin)';
COMMENT ON COLUMN stock_financial.net_profit_margin IS '销售净利率 (Net Profit Margin)';
COMMENT ON COLUMN stock_financial.roe_profitability IS '获利能力-净资产收益率 (ROE)';
COMMENT ON COLUMN stock_financial.roa IS '总资产净利率 (Return on Total Assets)';
COMMENT ON COLUMN stock_financial.gross_profit_margin IS '销售毛利率 (Gross Profit Margin)';
COMMENT ON COLUMN stock_financial.ebit IS '息税前利润 (EBIT)';
COMMENT ON COLUMN stock_financial.ebitda IS '息税折旧摊销前利润 (EBITDA)';
COMMENT ON COLUMN stock_financial.ebitda_to_revenue IS 'EBITDA除以营业总收入';
COMMENT ON COLUMN stock_financial.debt_to_assets_ratio IS '资产负债率 (Debt-to-Assets Ratio)';
COMMENT ON COLUMN stock_financial.op_cash_flow_per_share IS '每股经营性现金流 (元)';
COMMENT ON COLUMN stock_financial.net_cash_flow_per_share IS '每股现金流量净额 (元)';
COMMENT ON COLUMN stock_financial.oc_ratio_short_debt IS '经营净现金比率(短期债务)';
COMMENT ON COLUMN stock_financial.oc_ratio_total_debt IS '经营净现金比率(全部债务)';
COMMENT ON COLUMN stock_financial.total_share_capital IS '总股本';
COMMENT ON COLUMN stock_financial.tradable_a_shares IS '已上市流通A股';
COMMENT ON COLUMN stock_financial.institutional_holdings IS '机构持股总量 (股)';
COMMENT ON COLUMN stock_financial.national_team_holdings IS '国家队持股数量 (万股)';

.import tests/data/stock_financial_basic.csv stock_financial_basic;
.import tests/data/stock_kline_1d.csv stock_kline_1d;
.import tests/data/stock_shares.csv stock_shares;
.import tests/data/stock_dividend.csv stock_dividend;