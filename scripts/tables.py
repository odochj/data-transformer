from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


class TableType(Enum):
    FACT = "fact"
    DIMENSION = "dimension"


@dataclass
class Table:
    name: str
    type: TableType
    primary_key: str
    columns: List[str]
    sql: str
    foreign_keys: Optional[List[str]] = field(default_factory=list)
    satellites: Dict[str, Dict[str, str]] = field(default_factory=dict)
