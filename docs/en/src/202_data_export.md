# Data Export

## Export via Shell

You can use the shell in non-interactive mode to query data and redirect output to a file.

```bash
cargo run --release -p msd -- shell "SELECT * FROM my_table WHERE obj = 'SH600000'" > output.txt
```

Future versions may support direct CSV or JSON output formatting options.
