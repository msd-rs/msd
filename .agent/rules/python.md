---
trigger: always_on
glob: "bindings/python/**/*"
description: "python code style"
---


- The project is managed by `uv`, there is a virtual environment.
- To execute any python command, please use `uv run`
- To install any python package, please use `uv add`
- Add test cases to `bindings/python/tests`, run `uv run pytest` to run tests, add `-k <keyword>` to run specific test cases
