import os

import db_api
from db_api import DataBase, DBField, DBTable
from typing import Any, Dict, List, Type
import shelve
from dataclasses_json import dataclass_json
from dataclasses import dataclass


@dataclass_json
@dataclass
class DBField(db_api.DBField):
    name: str
    type: Type


@dataclass_json
@dataclass
class SelectionCriteria(db_api.SelectionCriteria):
    field_name: str
    operator: str
    value: Any


class DBTable(db_api.DBTable):
    name: str
    fields: List[DBField]
    key_field_name: str
    index_list = []

    def __init__(self, table_name, fields, key_field_name):
        self.name = table_name
        self.fields = fields
        self.key_field_name = key_field_name
        self.index_list = []

    def is_index(self, field):
        return field in self.index_list

    def count(self) -> int:
        data_table = shelve.open(f"db_files/{self.name}.db")
        count_tables = len(data_table.keys())
        data_table.close()
        return count_tables

    def check_validate_insert(self, values, data_table):
        if data_table.get(str(values[self.key_field_name])):
            data_table.close()
            raise ValueError
        if len(self.fields) < len(values.keys()):
            data_table.close()
            raise ValueError
        data_table[str(values[self.key_field_name])] = {}
        for field in self.fields:
            if self.is_index(field.name):
                if not values.get(field.name):
                    raise ValueError

    def insert_record(self, values: Dict[str, Any]) -> None:
        if not values.get(self.key_field_name):
            raise ValueError
        data_table = shelve.open(f"db_files/{self.name}.db", writeback=True)
        self.check_validate_insert(values, data_table)
        for field in self.fields:
            if self.is_index(field.name):
                db_index = shelve.open(f'db_files/{self.name}{field.name}.db', writeback=True)
                try:
                    db_index[values[field.name]].append(values[self.key_field_name])
                except:
                    db_index[values[field.name]] = [values[self.key_field_name]]
                finally:
                    db_index.close()
            if values.get(field.name):
                data_table[str(values[self.key_field_name])][field.name] = values[field.name]
            else:
                data_table[str(values[self.key_field_name])][field.name] = None
        data_table.close()

    def delete_record(self, key: Any) -> None:
        data_table = shelve.open(f"db_files/{self.name}.db", writeback=True)
        try:

            if not data_table.get(str(key)):
                data_table.close()
                raise ValueError
            else:
                for field in self.index_list:
                    db_index = shelve.open(f'db_files/{self.name}{field}.db', writeback=True)
                    if len(db_index[data_table[str(key)][field]]) == 1:
                        del db_index[data_table[str(key)][field]]
                    else:
                        b = db_index[data_table[str(key)][field]].index(key)
                        db_index[data_table[str(key)][field]].pop(b)
                    db_index.close()
                del data_table[str(key)]
        finally:
            data_table.close()

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        records_to_delete = self.query_table(criteria)
        for item in records_to_delete:
            self.delete_record(item[self.key_field_name])

    def is_criteria(self, critery, key):
        if critery.operator == '=':
            critery.operator = '=='
        return not eval(f'str(key){critery.operator}str(critery.value)')

    def get_record(self, key: Any) -> Dict[str, Any]:
        data_table = shelve.open(f"db_files/{self.name}.db", writeback=True)
        if data_table.get(str(key)):
            record = data_table[str(key)]
            data_table.close()
            return record
        else:
            data_table.close()
            raise ValueError

    def check_validate_update(self, data_table, values, key):
        if data_table.get(str(key)) == None:
            raise ValueError
        if values.get(self.key_field_name):
            raise ValueError
        for key_value in values.keys():
            if data_table[str(key)].get(key_value) == None:
                raise ValueError

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        data_table = shelve.open(f"db_files/{self.name}.db", writeback=True)
        self.check_validate_update(data_table, values, key)
        for field_index in self.index_list:
            if values.get(field_index):
                db_index = shelve.open(f'db_files/{self.name}{field_index}.db', writeback=True)
                if len(db_index[data_table[str(key)][field_index]]) == 1:
                    del db_index[data_table[str(key)][field_index]]
                else:
                    db_index[data_table[str(key)][field_index]].remove(key)
                try:
                    db_index[values[field_index]].append(key)
                    db_index.close()
                except:
                    db_index[values[field_index]] = [key]
                    db_index.close()
        data_table[str(key)].update(values)
        data_table.close()

    def query_by_index(self, criter):
        is_critery = set()
        db_index = shelve.open(f'db_files/{self.name}{criter.field_name}.db', writeback=True)
        for key in db_index.keys():
            try:
                if not self.is_criteria(criter, key):
                    if is_critery == set():
                        is_critery = set(db_index[key])
                    else:
                        is_critery.intersection_update(set(db_index[key]))
            except NameError:
                print("invalid")
        return is_critery

    def is_query_list(self, list_keys, list_criteria):
        data_table = shelve.open(f"db_files/{self.name}.db")
        Selection_criteria_list = []
        for record in list_keys:
            flag = 0
            for criter in list_criteria:
                if data_table[list(data_table.keys())[0]].get(criter.field_name):
                    if criter.operator == '=':
                        criter.operator = '=='
                    try:
                        if self.is_criteria(criter, data_table[record][criter.field_name]):
                            flag = 1
                    except NameError:
                        print("invalid")
            if not flag:
                Selection_criteria_list.append(data_table[record])
        data_table.close()
        return Selection_criteria_list

    def check_validate_query(self,criteria, data_table):
        for criter in criteria:
            if not data_table[list(data_table.keys())[0]].get(criter.field_name):
                raise ValueError

    def query_table(self, criteria: List[SelectionCriteria]) -> List[Dict[str, Any]]:
        data_table = shelve.open(f"db_files/{self.name}.db")
        list_select = []
        self.check_validate_query(criteria, data_table)
        set_criter = set()
        criter_without_index = []
        for criter in criteria:
            if self.is_index(criter.field_name):
                if set_criter == set():
                    set_criter = self.query_by_index(criter)
                else:
                    set_criter.intersection_update(self.query_by_index(criter))
            else:
                criter_without_index.append(criter)
        if criter_without_index:
            if set_criter == set():
                list_keys = data_table.keys()
            else:
                list_keys = list(set_criter)
            selection_criteria_list = self.is_query_list(list_keys, criter_without_index)
        else:
            list_select = []
            set_criter = list(set_criter)
            for key in set_criter:
                list_select.append(data_table[str(key)])
            selection_criteria_list = list_select
        data_table.close()
        return selection_criteria_list

    def create_index(self, field_to_index: str) -> None:
        if field_to_index == self.key_field_name:
            return
        flag = 0
        for field in self.fields:
            if field_to_index == field.name:
                flag = 1
                break
        if flag == 0:
            raise ValueError
        db_index = shelve.open(f'db_files/{self.name}{field_to_index}.db', writeback=True)
        data_table = shelve.open(f"db_files/{self.name}.db")
        db = shelve.open("db_files/DB.db", writeback=True)
        for record in data_table.keys():
            try:
                db_index[data_table[record][field_to_index]].append(int(record))
            except:
                db_index[data_table[record][field_to_index]] = [int(record)]
        self.index_list.append(field_to_index)
        db[self.name][2].append(field_to_index)
        data_table.close()
        db_index.close()
        db.close()


class DataBase(db_api.DataBase):
    __dict_tables__ = {}

    def __init__(self):
        with shelve.open(os.path.join(db_api.DB_ROOT, "DB.db"), writeback=True) as db:
            for key in db:
                db_table = DBTable(key, db[key][0], db[key][1])
                DataBase.__dict_tables__[str(key)] = db_table
                db_table.index_list = db[key][2]

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:
        if DataBase.__dict_tables__.get(table_name):
            return DataBase.__dict_tables__[table_name]
        flag = 0
        for field in fields:
            if key_field_name == field.name:
                flag = 1
        if flag == 0:
            raise ValueError
        data_table = shelve.open(os.path.join(db_api.DB_ROOT, table_name + ".db"), writeback=True)
        data_table.close()
        db_table = DBTable(table_name, fields, key_field_name)
        data_table.index_list = []
        DataBase.__dict_tables__[table_name] = db_table
        with shelve.open(os.path.join(db_api.DB_ROOT, "DB.db"), writeback=True) as db:
            db[table_name] = [fields, key_field_name, list()]
        return db_table

    def num_tables(self) -> int:
        return len(DataBase.__dict_tables__.keys())

    def get_table(self, table_name: str) -> DBTable:
        if None == DataBase.__dict_tables__.get(table_name):
            raise ValueError
        return DataBase.__dict_tables__[table_name]

    def delete_table(self, table_name: str) -> None:
        if DataBase.__dict_tables__.get(table_name):
            for suffix in ['bak', 'dat', 'dir']:
                os.remove(db_api.DB_ROOT.joinpath(f'{table_name}.db.{suffix}'))
            with shelve.open(os.path.join(db_api.DB_ROOT, "DB.db"), writeback=True) as db:
                del db[table_name]
            DataBase.__dict_tables__.pop(table_name)

    def get_tables_names(self) -> List[Any]:
        return list(DataBase.__dict_tables__.keys())

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
