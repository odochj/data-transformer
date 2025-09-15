from dataclasses import dataclass, field
from typing import Dict, List

from scripts.tables import Table, TableType


@dataclass
class TableBuilder:
    name: str
    hub: str
    key_columns: List[str]
    satellites: Dict[
        frozenset[str], Dict[str, str]
    ]  # satellite group -> {canonical_name: source_column}
    dimensions: Dict[
        str, List[str]
    ]  # e.g., {'provider': ['provider_id', 'provider_name']}
    dimension_map: Dict[str, str] = field(
        default_factory=dict
    )  # e.g. 'provider' -> 'dim_provider'

    def build(self) -> tuple[Table, List[Table]]:
        # --------------------
        # 1. CTEs for satellites
        # --------------------
        satellite_ctes = []
        all_satellites = set()
        for group, mapping in self.satellites.items():
            for satellite in group:
                all_satellites.add(satellite)
                select_exprs = [f"{v} AS {k}" for k, v in mapping.items()]
                satellite_ctes.append(
                    f"""
                , {satellite}_satellite AS (
                    SELECT {", ".join(select_exprs)}
                    FROM {satellite}
                )
                """
                )

        # --------------------
        # 2. Dimension Tables
        # --------------------
        dimension_tables = []
        dim_fk_exprs = []
        for dim_name, attrs in self.dimensions.items():
            dim_table = self.dimension_map.get(dim_name, f"dim_{dim_name}")
            dim_pk = f"{dim_name}_pk"
            dim_attrs = ", ".join(attrs)

            dim_sql = f"""
            SELECT DISTINCT
                HASH({", ".join(attrs)}) AS {dim_pk},
                {dim_attrs},
                created_at
            FROM (
                {
                " UNION ".join(
                    [
                        f"SELECT {', '.join(attrs)}, created_at FROM {sat}_satellite"
                        for sat in all_satellites
                    ]
                )
            }
            )
            """

            dimension_tables.append(
                Table(
                    name=dim_table,
                    type=TableType.DIMENSION,
                    primary_key=dim_pk,
                    columns=[dim_pk] + attrs + ["created_at"],
                    sql=dim_sql,
                )
            )

            # Prepare FK references for fact table
            fk_expr = f"HASH({', '.join(attrs)}) AS {dim_name}_fk"
            dim_fk_exprs.append(fk_expr)

        # --------------------
        # 3. Coalesce all fact columns
        # --------------------
        fact_columns: set[str] = set()
        for mapping in self.satellites.values():
            fact_columns.update(mapping.keys())

        coalesced = []
        for col in sorted(fact_columns - set(self.key_columns)):
            sources = [
                f"{sat}_satellite.{col}"
                for sat in all_satellites
                if col in self.satellites[frozenset([sat])].keys()
            ]
            coalesced.append(f"COALESCE({', '.join(sources)}) AS {col}")

        fact_pk_expr = f"""
        HASH({', '.join(
            self.key_columns + [f'{d}_fk' for d in self.dimensions]
        )}) AS pk
        """

        fact_sql = f"""
        WITH {self.name}_hub AS (
            SELECT * FROM {self.hub}
        )
        {"".join(satellite_ctes)}
        SELECT
            {fact_pk_expr},
            {", ".join([f"hub.{col}" for col in self.key_columns])},
            {", ".join(dim_fk_exprs)},
            {", ".join(coalesced)}
        FROM {self.name}_hub AS hub
        {
            " ".join(
                [
                    f"""
                    LEFT JOIN {sat}_satellite AS sat
                        ON hub.{self.key_columns[0]} = sat.{self.key_columns[0]}
                    """
                    for sat in all_satellites
                ]
            )
        }
        """

        fact_table = Table(
            name=f"fact_{self.name}",
            type=TableType.FACT,
            primary_key="pk",
            columns=self.key_columns
            + [f"{d}_fk" for d in self.dimensions]
            + list(fact_columns - set(self.key_columns)),
            sql=fact_sql,
        )

        return fact_table, dimension_tables
