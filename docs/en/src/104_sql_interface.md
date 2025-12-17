# SQL Interface

MSD-RS supports a subset of SQL for data definition, manipulation, and querying.

## CREATE TABLE

Create a new table with a schema.

```sql
CREATE TABLE table_name (
  obj STRING,
  ts TIME,
  open FLOAT32,
  high FLOAT32,
  low FLOAT32,
  close FLOAT32,
  vol FLOAT32
) WITH (chunksize=86400); 
```

- **Supported types:** 
  - `TIME`, `DATE`: DateTime
  - `DECIMAL128`, `DECIMAL64`
  - `U...`: UInt64
  - `BOOL`: Boolean
  - `F32`, `FLOAT32`: Float32
  - `F...`, `DOUBLE`: Float64
  - `I...`, `INT`: Int64
  - `CHAR`, `STRING`, `TEXT`: String

- **Aggregation Options:**
  You can specify aggregation methods for columns using `OPTIONS`.
  - `AGG_FIRST`
  - `AGG_MIN`
  - `AGG_MAX`
  - `AGG_SUM`
  - `AGG_COUNT`
  - `AGG_AVG`
  - `AGG_UNIQ_COUNT`

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

## SELECT

Query data from a table.

```sql
SELECT * FROM table_name WHERE obj = 'SH600000' AND ts >= '2023-01-01' AND ts < '2023-02-01' LIMIT 100;
```

- **WHERE clause:** Supports filtering by `obj` (equality) and `ts` (range).
- **ORDER BY:** Optional, supports `ASC` (default) or `DESC`.
- **LIMIT:** Optional.

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
