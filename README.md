# Your SQLMesh Project

This is a SQLMesh project using uv for dependency management.

## Quickstart

```bash
# install dependencies
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt  # or rely on pyproject.toml directly

# Run SQLMesh
sqlmesh preview
sqlmesh plan
sqlmesh apply
```

## Project Structure

- `models/`: SQL models
- `macros/`: Jinja macros
- `tests/`: Python-based tests
