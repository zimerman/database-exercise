from dataclasses import dataclass
from pathlib import Path
import shelve
from typing import Any, Dict, List, Type
from db_api import DataBase, DBField, DBTable


class DBField:
    name: str
    type: Type


class SelectionCriteria:
    field_name: str
    operator: str
    value: Any


class MyDBTable:
    name: str
    fields: List[DBField]
    key_field_name: str

    def count(self) -> int:
        raise NotImplementedError

    def insert_record(self, values: Dict[str, Any]) -> None:
        data_table = shelve.open(self.name)
        data_table[values[self.key_field_name]] = values



    def delete_record(self, key: Any) -> None:
        raise NotImplementedError

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        raise NotImplementedError

    def get_record(self, key: Any) -> Dict[str, Any]:
        raise NotImplementedError

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        raise NotImplementedError

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        raise NotImplementedError

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError


class MyDataBase(DataBase):

    dict_table = {}

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:
        data_table = shelve.open(f"{table_name}.db", writeback=True)
        data_table.close()
        dbtable = DBTable(table_name, fields, key_field_name)
        self.dict_table[table_name] = dbtable
        return dbtable

    def num_tables(self) -> int:
        return len(self.dict_table.keys())

    def get_table(self, table_name: str) -> DBTable:
         return self.dict_table.get(table_name)

    def get_tables_names(self) -> List[Any]:
        return List[self.dict_table.keys()]

    def delete_table(self, table_name: str) -> None:
        raise NotImplementedError











