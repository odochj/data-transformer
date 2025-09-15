from scripts.api_helper import get_source_metadata, list_sources
from scripts.model_writer import write_sqlmesh_models
from scripts.table_builder import TableBuilder
from scripts.table_registry import TableRegistry


def main() -> None:
    # 1. Fetch all sources from the API
    sources = list_sources()
    registry = TableRegistry()

    for source in sources:
        metadata = get_source_metadata(source)
        # 2. Map API metadata to TableBuilder
        builder = TableBuilder(
            name=metadata["name"],
            hub=metadata.get("hub", ""),
            key_columns=metadata.get("key_columns", []),
            satellites={
                frozenset(
                    k.split(",")): v for k, v in metadata
                    .get("satellites", {})
                    .items()
            },
            dimensions=metadata.get("dimensions", {}),
            dimension_map=metadata.get("dimension_map", {}),
        )
        fact_table, dimension_tables = builder.build()
        registry.register(fact_table)
        for dim in dimension_tables:
            registry.register(dim)

    # 3. Write SQLMesh models
    write_sqlmesh_models(registry)

if __name__ == "__main__":
    main()