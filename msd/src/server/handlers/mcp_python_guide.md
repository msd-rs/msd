# Prerequisites

When you want to know what tables are available, call `list_tables` tool, then call `get_table` tool to get table schema detail.

# Starting Write Python Script
1. Ensure `pymsd` library is installed, you can install it by:
```bash
pip install pymsd
```
or
```bash
uv add pymsd
```
2. There should be an msd server endpoint like `http://localhost:50510`.
3. Create a native `MsdConnection` instance with the endpoint:
```python
import pymsd

conn = pymsd.MsdConnection("http://localhost:50510")
```
4. Query data from msd server:
- `conn.query(sql) -> dict[str, dict[str, Table]]` for grouped tables `{obj: {table_name: Table}}`
- `conn.query_join(sql, base_table, fill, base_obj) -> tuple[Table, list[str], int]` for native multi-table as-of join
- `conn.query_concat(sql) -> dict[str, tuple[Table, list[tuple[str, int, int]]]]` for concatenated tables
- `conn.snapshot(tables, objects, date=None) -> Table` for latest multi-table snapshot
- Convert any `Table` to pandas/polars DataFrame via `pd.DataFrame(table.to_dict())` or `pl.DataFrame(table.to_dict())`
5. Import data to msd server:
- `conn.import_csv(table, data, header=True, delimiter=None) -> dict`
- `conn.import_table(table, data) -> dict`
- `conn.import_dataframes(table, data, decimal_fields=None) -> dict` (where `data` is `(obj, df)` or an iterable of `(obj, df)` pairs supporting `pandas.DataFrame`, `polars.DataFrame`, `Table`, or `(col_name, ndarray)` lists)
