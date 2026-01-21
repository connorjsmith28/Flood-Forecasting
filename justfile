# Lint all code
lint:
    ruff check .
    sqlfluff lint elt/transformation/
