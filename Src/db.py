from dataclasses import dataclass
from pathlib import Path
import os
import shelve
from typing import Any, Dict, List, Type

import db_api
from db_api import DataBase, DBField, DBTable


class DBField(db_api.DBField):
    name: str
    type: Type


class SelectionCriteria(db_api.SelectionCriteria):
    field_name: str
    operator: str
    value: Any


class DBTable(db_api.DBTable):
    name: str
    fields: List[DBField]
    key_field_name: str

    def count(self) -> int:
        data_table = shelve.open(f"db_files/{self.name}.db")
        count = len(data_table.keys())
        data_table.close()
        return count

    def insert_record(self, values: Dict[str, Any]) -> None:
        if values.get(self.key_field_name) == None:
            raise ValueError
        data_table = shelve.open(f"db_files/{self.name}.db", writeback=True)
        try:
            if data_table.get(str(values[self.key_field_name])):
                data_table.close()
                raise ValueError
            if len(values.keys()) > len(self.fields):
                data_table.close()
                raise ValueError
            data_table[str(values[self.key_field_name])] = {}
            for field in self.fields:
                if values.get(field.name):
                    data_table[str(values[self.key_field_name])][field.name] = values[field.name]
                else:
                    data_table[str(values[self.key_field_name])][field.name] = None
        finally:
            data_table.close()

    def delete_record(self, key: Any) -> None:
        data_table = shelve.open(f"db_files/{self.name}.db", writeback=True)
        try:
            if data_table.get(str(key)) != None:
                del data_table[str(key)]
            else:
                raise ValueError
        finally:
            data_table.close()

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        data_table = shelve.open(f"db_files/{self.name}.db", writeback=True)
        # for criter in criteria:
        #     if data_table[list(data_table.keys())[0]].get(criter.field_name) == None:
        #         raise ValueError
        # for criter in criteria:
        #     if data_table[list(data_table.keys())[0]].get(criter.field_name):
        #         for record in data_table.keys():
        #             if not eval(data_table[record][criter.field_name] + criter.operator + str(criter.value)):
        #                 self.delete_record(record)
        for record in data_table.keys():
            flag = 0
            for criter in criteria:
                if data_table[list(data_table.keys())[0]].get(criter.field_name):
                    if criter.operator == '=':
                        criter.operator = '=='
                    if not eval(data_table[record][criter.field_name] + criter.operator + str(criter.value)):
                        flag = 1
            if flag == 0:
                self.delete_record(record)
        data_table.close()

    def get_record(self, key: Any) -> Dict[str, Any]:
        data_table = shelve.open(f"db_files/{self.name}.db", writeback=True)
        if data_table.get(str(key)):
            t = data_table[str(key)]
        else:
            raise ValueError
        data_table.close()
        return t

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        data_table = shelve.open(f"db_files/{self.name}.db", writeback=True)
        if data_table.get(str(key)) == None:
            raise ValueError
        if values.get(self.key_field_name):
            raise ValueError
            # temp_dict = data_table[str(key)]
            # del data_table[str(key)]
            # data_table[values[self.key_field_name]] = temp_dict
            # key = values[self.key_field_name]
        # if data_table.get(str(key)) == None:
        #     raise ValueError
        for key_value in values.keys():
            if data_table[str(key)].get(key_value) == None:
                raise ValueError
        data_table[str(key)].update(values)
        data_table.close()

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        
        raise NotImplementedError

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError


class DataBase(db_api.DataBase):

    dict_table = {}

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:
        data_table = shelve.open(f"db_files/{table_name}.db", writeback=True)
        data_table.close()
        dbtable = DBTable(table_name, fields, key_field_name)
        self.dict_table[table_name] = dbtable
        return dbtable

    def num_tables(self) -> int:
        return len(self.dict_table.keys())

    def get_table(self, table_name: str) -> DBTable:
        if self.dict_table.get(table_name) is None:
            raise ValueError
        return self.dict_table.get(table_name)

    def get_tables_names(self) -> List[Any]:
        return list(self.dict_table.keys())

    def delete_table(self, table_name: str) -> None:
        os.remove(os.path.join('db_files', table_name + ".db.bak"))
        os.remove(os.path.join('db_files', table_name + ".db.dat"))
        os.remove(os.path.join('db_files', table_name + ".db.dir"))
        self.dict_table.pop(table_name)

