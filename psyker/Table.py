# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
from .Sql import Sql
from .columns import Column


class Table:

    __slots__ = ('name', 'columns', 'relationships', 'reverse_relationships',
                 'alias')

    def __init__(self, db, _name, primary_key='uuid', alias=None, **kwargs):
        self.name = _name
        self.columns = self.make_columns(kwargs, primary_key)
        self.relationships = self.make_relationships(db, self.columns)
        self.reverse_relationships = []
        self.alias = alias
        self.make_reverse_relationships(db, self.relationships)

    @staticmethod
    def make_primary_key(primary_key):
        if primary_key == 'serial':
            return Column('id', 'serial', primary_key=True)
        return Column('id', 'uuid', nullable=False,
                      default='uuid_generate_v1()', primary_key=True)

    @staticmethod
    def column(name, field):
        if type(field) == str:
            return Column(name, field)
        return field

    @classmethod
    def make_columns(cls, fields, primary_key):
        columns = {
            name: cls.column(name, field) for name, field in fields.items()
        }
        if primary_key:
            columns['id'] = cls.make_primary_key(primary_key)
        return columns

    @classmethod
    def make_relationships(cls, db, columns):
        """
        Uses foreign columns to find the relationships of the table.
        """
        rels = [column.relationship() for column in columns.values()]
        unique_rels = set([rel for rel in rels if rel is not None])
        return [db.get_table(rel) for rel in unique_rels]

    def make_reverse_relationships(self, db, relationships):
        """
        Uses relationships to set reverse relationships on the receiving
        tables.
        """
        for table in relationships:
            table.reverse_relationships.append(self)

    def sql(self):
        columns = [column.sql() for column in self.columns.values()]
        return Sql.table(self.name, columns)

    def cast(self, values):
        """
        Casts values that are handled by psycopg2.
        """
        casted = {}
        for key, value in values.items():
            casted[key] = self.columns[key].cast(value)
        return casted

    def as_string(self, cursor):
        return self.sql().as_string(cursor)

    def __repr__(self):
        return f'<Table({self.name})>'
