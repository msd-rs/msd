# Prerequisites

When your want to know what tables are available, call `list_tables` tool, then call `get_table` tool to get table schema detail

# Starting Write Python Script
1. ensure `msd` library is installed, you can install it by:
```bash
pip install msd
```
or 
```bash
uv add msd
``` 
2. there should be a msd server endpoint like `http://localhost:50510`
3. use `msd.create_msd_polars` or `msd.create_msd_pandas` to create a `MsdClient` instance with endpoint, use which is depends on which DataFrame library you use
4. use `MsdClient.load` to load data from msd server, which is defined as 
```python
  def load(self, 
    objs: list[str] | str, 
    tables: list[str] | str, 
    join: Literal["backward", "forward", "nearest"] | None = None,
    start: str | datetime.datetime | None = None, 
    end: str | datetime.datetime | None = None, 
    fields : dict[str, list[str]] | list[str] | None = None, 
    )-> dict[str, dict[str, DF]] | dict[str, DF]:

    """
    Load data from msd, the data will be organized as {obj: {table: DF}} or {obj: DF} if join is specified.

    Args:
      objs: list of object names or a single object name
      tables: list of table names or a single table name
      join: join type, can be "backward", "forward", "nearest", or None
      start: start time, can be str or datetime.datetime
      end: end time, can be str or datetime.datetime
      fields: fields to load, can be dict[str, list[str]] or list[str] or None

    Returns:
      dict[str, dict[str, DF]] or dict[str, DF]: the loaded data

    """
```

5. use `MsdClient.save` to save data to msd server, which is defined as 

```python
  def save(self, table: str, data: Iterator[MsdTableFrame] | str, /, **kwargs) -> dict:
    """
    Save DataFrame or file to a table

    Args:
      table: table name
      data: iterator of MsdTableFrame or csv file path
    """
```
