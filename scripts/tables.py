from enum import Enum
from dataclasses import dataclass, field
from typing import List, Dict

class TableType(Enum):
    FACT = "fact"
    DIMENSION = "dimension"

@dataclass
class Table:
    name: str  # Derived from tag
    primary_key: str = "pk"
    type: TableType | None = None
    columns: List[str]  # List of column names
    hub: str
    # satellites: {satellite_group: {canonical_column: source_column}}
    satellites: Dict[frozenset[str], Dict[str, str]] = field(default_factory=dict) 
    foreign_keys: List[str] = field(default_factory=list)

    def fact_sql(self) -> str:
        if self.type != TableType.FACT:
            raise ValueError("Provided table is not a fact table.")
        
        # 1. Build Hub CTE
        hub_cte = f"""
        WITH {self.name}_hub AS (
            SELECT *
            FROM {self.hub}
        )
        """

        # 2. Build Satellite CTEs
        satellite_ctes = []
        for satellite_group, mapping in self.satellites.items():
            for satellite_name in satellite_group:
            # Generate SQL per satellite_name using the shared `mapping`
                select_lines = [
                    f"{self.primary_key}",
                    *[f"{v} AS {k}" for k, v in mapping.items()]
                ]
                cte = f""",
                    {satellite_name}_satellite AS (
                        SELECT {', '.join(select_lines)}
                        FROM {satellite_name}
                    )
                    """
                satellite_ctes.append(cte)

        # 3. Build Final SELECT
        all_satellites = {s for group in self.satellites for s in group}
        coalesced_cols = []
        for col in self.columns:
            col_exprs = []
            for group, mapping in self.satellites.items():
                if col in mapping:
                    col_exprs.extend([f"{sat}_satellite.{col}" for sat in group])
            coalesce_expr = f"COALESCE({', '.join(col_exprs)}) AS {col}"
            coalesced_cols.append(coalesce_expr)

        final_select = f"""
        SELECT
            hub.{self.primary_key},
            {',\n        '.join(coalesced_cols)}
        FROM {self.name}_hub AS hub
        {' '.join([
            f"LEFT JOIN {satellite}_satellite ON hub.{self.primary_key} = {satellite}_satellite.{self.primary_key}"
            for satellite in all_satellites
        ])}
        """

        return hub_cte + ''.join(satellite_ctes) + final_select



    # def dimension_sql(self) -> str:
    #     if self.type == TableType.DIMENSION:
    #         columns = ",\n  ".join(self.columns)
    #         return f"""
    #         SELECT
    #             {self.primary_key}
    #             , {columns}
    #             , CURRENT_TIMESTAMP AS transformed_at
    #         FROM {self.hub}
    #         """
    #     else:
    #         raise ValueError("Provided table is not a dimension table.")