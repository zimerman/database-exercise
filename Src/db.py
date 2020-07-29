import csv
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

    def is_index(self,field):
        return field in self.index_list

    def count(self) -> int:
        data_table = shelve.open(f"db_files/{self.name}.db")
        count_tables = len(data_table.keys())
        data_table.close()
        return count_tables

    def insert_record(self, values: Dict[str, Any]) -> None:
        if None == values.get(self.key_field_name):
            raise ValueError
        data_table = shelve.open(f"db_files/{self.name}.db",writeback=True)
        if data_table.get(str(values[self.key_field_name])):
            data_table.close()
            raise ValueError
        if len(self.fields) < len(values.keys()):
            data_table.close()
            raise ValueError
        data_table[str(values[self.key_field_name])] = {}
        for field in self.fields:
            if self.is_index(field.name):
                if values.get(field.name) == None:
                    raise ValueError
        for field in self.fields:
            if self.is_index(field.name):
                db_index = shelve.open(f'db_files/{self.name}{field.name}.db', writeback=True)
                try:
                    db_index[values[field.name]].append(values[self.key_field_name])
                    db_index.close()
                except:
                    db_index[values[field.name]] = [values[self.key_field_name]]
                    db_index.close()
            if values.get(field.name):
                data_table[str(values[self.key_field_name])][field.name] = values[field.name]
            else:
                data_table[str(values[self.key_field_name])][field.name] = None
        data_table.close()

    def delete_record(self, key: Any) -> None:
        data_table = shelve.open(f"db_files/{self.name}.db", writeback=True)
        try:
            if None == data_table.get(str(key)):
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
        data_table = shelve.open(f"db_files/{self.name}.db")
        for criter in criteria:
            if data_table[list(data_table.keys())[0]].get(criter.field_name)==None:
                 raise ValueError
        for record_in_table in data_table.keys():
            flag = 0
            for record in criteria:
                if data_table[list(data_table.keys())[0]].get(record.field_name):
                    if record.operator == '=':
                         record.operator = '=='
                    try:
                        if not eval(f"{data_table[record_in_table][record.field_name]}{record.operator}{str(record.value)}"):
                            flag = 1
                    except NameError:
                        print("invalid")
            if not flag:
                self.delete_record(record_in_table)
        data_table.close()



    #
    # def map_key(self, name, value):
    #
    # def is_criteria(self,critery,key):
    #     if critery.operator == '=':
    #         critery.operator = '=='
    #     return not eval(f"{key}{critery.operator}{str(critery.value)}")


    def get_record(self, key: Any) -> Dict[str, Any]:
        data_table = shelve.open(f"db_files/{self.name}.db", writeback=True)
        if data_table.get(str(key)):
            d = data_table[str(key)]
            data_table.close()
            return d
        else:
            data_table.close()
            raise ValueError

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        data_table = shelve.open(f"db_files/{self.name}.db", writeback=True)
        if data_table.get(str(key)) == None:
            raise ValueError
        if values.get(self.key_field_name):
            raise ValueError
        for key_value in values.keys():
            if data_table[str(key)].get(key_value) == None:
                raise ValueError
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

    def query_table(self, criteria: List[SelectionCriteria]) -> List[Dict[str, Any]]:
        data_table = shelve.open(f"db_files/{self.name}.db")
        Selection_criteria_list = []
        for criter in criteria:
            if data_table[list(data_table.keys())[0]].get(criter.field_name)==None:
                raise ValueError
        for record_in_table in data_table.keys():
            flag = 0
            for record in criteria:
                if record.operator == '=':
                    record.operator = '=='
                    if not eval(f'str(data_table[record_in_table][record.field_name]){record.operator}str(record.value)'):
                        flag = 1
            if not flag:
                Selection_criteria_list.append(data_table[record_in_table])
        return Selection_criteria_list




    # def query_table(self, criteria: List[SelectionCriteria]) -> List[Dict[str, Any]]:
    #     data_table = shelve.open(f"db_files/{self.name}.db")
    #     Selection_criteria_list = []
    #     for criter in criteria:
    #         if data_table[list(data_table.keys())[0]].get(criter.field_name)==None:
    #             raise ValueError
    #     for record_in_table in data_table.keys():
    #         flag = 0
    #         for record in criteria:
    #                 # if record.operator == '=':
    #      record.operator = '=='
    #     try:
    #      if not eval(f'str(data_table[record_in_table][record.field_name]){record.operator}str(record.value)'):
    #          flag = 1
    #                 flag = is_criteria(record, data_table[record_in_table][record.field_name])
    #                    print("invalid Name")
    #         if not flag:
    #             Selection_criteria_list.append(data_table[record_in_table])
    #    return Selection_criteria_list

    def create_index(self, field_to_index: str) -> None:
        if field_to_index == self.key_field_name:
            return
        flag = 0
        for field in self.fields:
            if field_to_index == field.name:
                flag = 1
        if flag == 0:
            raise ValueError
        db_index = shelve.open(f'db_files/{self.name}{field_to_index}.db', writeback=True)
        data_table = shelve.open(f"db_files/{self.name}.db")
        for record in data_table.keys():
            try:
                db_index[data_table[record][field_to_index]].append(record)
            except:
                db_index[data_table[record][field_to_index]] = [record]
        self.index_list.append(field_to_index)
        db = shelve.open("db_files/DB.db", writeback=True)
        db[self.name][2].append(field_to_index)
        data_table.close()
        db_index.close()



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
            db[table_name] = [fields, key_field_name, []]
        return db_table

    def num_tables(self) -> int:
        return len(DataBase.__dict_tables__.keys())

    def get_table(self, table_name: str) -> DBTable:
            if None==DataBase.__dict_tables__.get(table_name):
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


    # def delete_records(self, criteria: List[SelectionCriteria]) -> None:
    #     data_table = shelve.open(f"db_files/{self.name}.db")
    #     for criter in criteria:
    #         if data_table[list(data_table.keys())[0]].get(criter.field_name)==None:
    #              raise ValueError
    #
    #
    #     for criter in criteria:
    #         if self.is_index(criter.field_name):
    #             db_index = shelve.open(f'db_files/{self.name}{criter.field_name}.db', writeback=True)
    #             for key in db_index.keys():
    #                 flag = is_criteria(criter,key)
    #                 for
    #             if not flag:
    #                 self.delete_record(record_in_table)
    #
    #     for record_in_table in data_table.keys():
    #         flag = 0
    #         for record in criteria:
    #             if data_table[list(data_table.keys())[0]].get(record.field_name):
    #                 # if record.operator == '=':
    #                 #      record.operator = '=='
    #                 # if not eval(f"{data_table[record_in_table][record.field_name]}{record.operator}{str(record.value)}"):
    #                     flag = 1
    #                     flag = is_criteria(record,data_table[record_in_table][record.field_name])
    #         if not flag:
    #             self.delete_record(record_in_table)
    #     data_table.close()