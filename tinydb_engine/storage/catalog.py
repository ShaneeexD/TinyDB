from __future__ import annotations

from typing import Dict

from tinydb_engine.schema import TableSchema, deserialize_schema_map, serialize_schema_map
from tinydb_engine.storage.pager import Pager


class Catalog:
    """Schema catalog stored inside the database header metadata.

    Keeping catalog metadata in page 0 keeps bootstrap logic small: opening the DB
    only requires reading one page before all table/index roots are known.
    """

    def __init__(self, pager: Pager):
        self.pager = pager

    def load(self) -> Dict[str, TableSchema]:
        metadata = self.pager.metadata()
        return deserialize_schema_map(metadata.get("schemas", {}))

    def save(self, schemas: Dict[str, TableSchema]) -> None:
        metadata = self.pager.metadata()
        metadata["schemas"] = serialize_schema_map(schemas)
        self.pager.set_metadata(metadata)
