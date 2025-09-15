from pathlib import Path

from scripts.table_registry import TableRegistry

SQLMESH_MODELS_DIR = Path("models")


def write_sqlmesh_models(registry: TableRegistry) -> None:
    SQLMESH_MODELS_DIR.mkdir(exist_ok=True)
    for table in registry.tables.values():
        model_path = SQLMESH_MODELS_DIR / f"{table.name}.sql"
        with open(model_path, "w") as f:
            f.write(table.sql)