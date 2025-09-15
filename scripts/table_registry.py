from dataclasses import dataclass, field
from typing import Dict

from scripts.tables import Table


@dataclass
class TableRegistry:
    tables: Dict[str, Table] = field(default_factory=dict) 

    def register(self, table: Table)-> None:
        if table.name in self.tables:
            raise ValueError(f"Table {table.name} already registered.")
        self.tables[table.name] = table

    def get(self, name: str) -> Table:
        return self.tables[name]
