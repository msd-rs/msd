# SQL Interface

MSD-RS supports a subset of SQL for data definition, manipulation, and querying.

## CREATE TABLE

There is a typical example of creating a kline table.

```sql
CREATE TABLE kline1d (
  ts DATETIME,
  open FLOAT64 AGG_FIRST,
  high FLOAT64 AGG_MAX,
  low FLOAT64 AGG_MIN,
  close FLOAT64,
  vol FLOAT64 AGG_DIFF_FIRST,
  amount FLOAT64 AGG_DIFF_LAST
) WITH (
  chunkSize = 250,
  round = 'd'
  chan = 'target_table: field1, field2'
); 
```

There are some highlight options:

- The first column must be `ts` of type `DATETIME`.
- No need define `obj` column, it will be added automatically.
- Each column can have an aggregation option, which is described how this column is aggregated.
- Table can have some options:
  - `chunkSize`: The number of rows in a store chunk, default is 200. Bigger chunk size will improve performance but consume more memory.
  - `round`: The time interval for rounding `ts`, for example `1d` means rounding to day. Round is happened before aggregation. If `round` is not specified, `ts` will not be rounded.
  - `chan`: The associated table that should be updated when this table is updated. See below for more details. 

### Data Types
  - `TIME`, `DATE`, `DATETIME`: DateTime
  - `D128`, `DECIMAL128`: A Decimal with 128 bits
  - `D64`, `DECIMAL64`: A Decimal with 64 bits
  - `UINT`, `U64`: An unsigned 64-bit integer
  - `INT`, `I64`: An signed 64-bit integer
  - `BOOL`: Boolean
  - `F32`, `FLOAT32`, `SINGLE`: Float32
  - `F64`, `FLOAT64`,`DOUBLE`: Float64
  - `STRING`, `TEXT`: String, length is not required.

### Column Aggregation Options
  You can specify aggregation methods for columns using `OPTIONS`.
  - `AGG_FIRST`: Take the first value of the time period. e.g. `open` in kline.
  - `AGG_MIN`: Take the minimum value of the time period. e.g. `low` in kline.
  - `AGG_MAX`: Take the maximum value of the time period. e.g. `high` in kline.
  - `AGG_SUM`: Take the sum of the time period. e.g. `volume` in kline when you from a current volume datafeed.
  - `AGG_COUNT`: Take the count of the time period.
  - `AGG_AVG`: Take the average of the time period.
  - `AGG_UNIQ_COUNT`: Take the count of unique values of the time period.
  - `AGG_DIFF_FIRST`: Take the difference between the first and last value of the time period. e.g. `volume` in kline when you from a continuous volume datafeed.
  - `AGG_DIFF_LAST`: Take the difference between the last and first value of the time period.

### Table Options

#### chunkSize

The number of rows in a store chunk, default is 250, it roughly equal to daily k line of one stock per year.

The `chunkSize` is a balance between read/write performance and memory usage. Try to set it to a value that is close to the number of rows you expect

#### round

This option controls the aggregation time interval. For example, if you set `round = '1d'`, then the data will be rounded to day before aggregation, so the aggregation will be happened on the same day.

for a datetime `2023-05-12 15:03:04.123456`,  `round` has the format like:
- `y`: round to year, the result will be like `2023-01-01 00:00:00`
- `M`: round to month, the result will be like `2023-05-01 00:00:00`
- `w`: round to week, the result will be like `2023-05-08 00:00:00`
- `d`: round to day, the result will be like `2023-05-12 00:00:00`
- `1h`: round to hour, the result will be like `2023-05-12 15:00:00`
- `1m`: round to minute, the result will be like `2023-05-12 15:03:00`
- `1s`: round to second, the result will be like `2023-05-12 15:03:04`

Please note that `h, m, s` can add a number to specify the interval.

#### chan

The associated table that should be updated when this table is updated. 

The format is :

```
CHAN = TARGET_TABLE(, TARGET_TABLE)* : FIELD(, FIELD)*
TARGET_TABLE = STRING
FIELD = STRING | CHANGED_IF(FIELD, FIELD)
```

For example:

```sql
CHAN = kline1d, kline1m : CHANGED_IF(open, close), CHANGED_IF(high, close), CHANGED_IF(low, close), close, volume, amount 
```

#### CHANGED_IF(field1, field2)

For some datafeed like chinese market level 1 data, it's snapshot data every 3 seconds, the `high`, `low` are today's high and low, when it happen in a snapshot, we should notify the change of `high` and `low` to the associated table. But when it not changed, we should not use it's value.

The `CHANGED_IF` means if `field1` is changed, use it's value, otherwise use `field2`'s value.


## SELECT

Query data from a table.

```sql
SELECT * FROM table_name WHERE obj = 'SH600000' AND ts >= '2023-01-01' AND ts < '2023-02-01' LIMIT 100;
```

- **WHERE clause:** : only `obj` and `ts` are supported.
  - `obj`: wildcard `*` and `?` are supported. eg: `SH60000?` will match all stocks start with `SH60000`.  `IN` is also supported. eg: `obj IN ('SH600000', 'SH600001')`, `NOT IN` is not supported. `LIKE` is not supported, use wildcard instead. 
  - `ts`: supports `=` and `>` and `<` and `>=` and `<=`
- **ORDER BY:** Optional, supports `ASC` (default) or `DESC`. Only `ts` is supported.
- **LIMIT:** Optional. Default no limit.

## INSERT

Insert data into a table. The first column `obj` is mandatory and acts as the partitioning key.

```sql
INSERT INTO table_name VALUES
  ('SH600000', '2023-01-01', 10.0, 11.0, 9.0, 10.5, 1000.0),
  ('SH600000', '2023-01-02', 10.5, 11.2, 9.5, 11.0, 1200.0);
```

## COPY

Import CSV data. This behaves similarly to `INSERT`.

```sql
COPY table_name
'SH600000',10.0,11.0,9.0,10.5,1000.0
'SH600000',10.5,11.2,9.5,11.0,1200.0
```

## DELETE

Delete data from a table.

```sql
DELETE FROM table_name WHERE obj = 'SH600000';
```

- If `WHERE` clause is omitted or no `obj` is specified, it may delete the entire table (use with caution).

## DESCRIBE

Show table schema.

```sql
DESC table_name;
```
