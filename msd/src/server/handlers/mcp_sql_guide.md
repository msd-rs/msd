# Prerequisites

When your want to know what tables are available, call `list_tables` tool, then call `get_table` tool to get table schema detail

# When build SQL, There are two fixed columns `ts` and `obj` in each msd table
- `obj`: object name, can be represented as symbol, code, id or any meaningful string
- `ts`: timestamp, can be represented as date, datetime, timestamp or any other format that can be parsed to timestamp

you can use these two columns to filter data, for example:
```sql
select * from table where obj = 'AAPL' and ts between '2022-01-01' and '2022-12-31'
```
