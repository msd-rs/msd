# Data Import

## Using COPY (CSV)

The most efficient way to import bulk data is using the `COPY` command via the SQL interface.

```sql
COPY table_name
'OBJECT_ID', ... values ...
```

You can pipe a CSV file into the shell:

```bash
cat data.csv | cargo run --release -p msd -- shell
```

*Note: ensure your CSV matches the table schema order.*
