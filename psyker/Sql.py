# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
from psycopg2.sql import Identifier, Placeholder, SQL


class Sql:

    __slots__ = ()

    @staticmethod
    def format(string, *args):
        return SQL(string).format(*args)

    @staticmethod
    def join(*args, separator=' '):
        return SQL(separator).join(args)

    @staticmethod
    def identifier(name):
        return Identifier(name)

    @staticmethod
    def identifiers(names):
        return [Identifier(i) for i in names]

    @staticmethod
    def table_columns(columns):
        return SQL(', ').join(columns)

    @staticmethod
    def placeholder():
        return Placeholder()

    @staticmethod
    def column_template(sql, default, primary):
        if default:
            sql = f'{sql} default {default}'
        if primary:
            sql = f'{sql} primary key'
        return sql

    @classmethod
    def placeholders(cls, columns):
        """
        Generates placeholders for values, e.g %s, %s
        """
        return SQL(', ').join(cls.placeholder() * len(columns))

    @classmethod
    def table_name(cls, table):
        """
        Produces the sql for a table's name, taking into account aliases.
        """
        name = cls.identifier(table.name)
        if table.alias:
            return cls.format('{} as {}', name, cls.identifier(table.alias))
        return name

    @classmethod
    def columns(cls, columns):
        return SQL(', ').join(cls.identifiers(columns.keys()))

    @classmethod
    def column_with_value(cls, condition):
        template = f'{{}} {condition[1]} {{}}'
        identifier = cls.identifier(condition[0])
        return cls.format(template, identifier, cls.placeholder())

    @classmethod
    def column_with_placeholder(cls, column):
        return cls.format('{} = {}', cls.identifier(column), cls.placeholder())

    @classmethod
    def columns_with_values(cls, conditions):
        return map(cls.column_with_value, conditions)

    @classmethod
    def columns_with_alias(cls, columns, alias):
        """
        Produces a list of columns but prefixes them with their table's alias.
        """
        alias = cls.identifier(alias)
        keys = columns.keys()
        return [
            cls.format('{}.{}', alias, cls.identifier(key)) for key in keys
        ]

    @classmethod
    def columns_with_placeholder(cls, values):
        """
        Builds the fragment used in updates for columns, such as col = %
        """
        columns = [cls.column_with_placeholder(key) for key in values.keys()]
        return SQL(', ').join(columns)

    @classmethod
    def comparison(cls, conditions):
        """
        Builds a comparison fragment e.g. col = %s, col2 > %s. Used in WHERE
        statements.
        """
        return SQL(', ').join(cls.columns_with_values(conditions))

    @classmethod
    def order(cls, columns):
        """
        Builds the column order fragment used in ORDER BY e.g col ord, col ord
        """
        items = columns.items()
        order = [cls.format(f'{{}} {v}', cls.identifier(k)) for k, v in items]
        return SQL(', ').join(order)

    @classmethod
    def where(cls, conditions):
        return cls.format('where {}', cls.comparison(conditions))

    @classmethod
    def count(cls, table):
        return cls.format('select count(*) from {}', cls.identifier(table))

    @classmethod
    def select(cls, table, columns):
        sql = 'select {} from {}'
        return cls.format(sql, cls.columns(columns), cls.identifier(table))

    @classmethod
    def join_table(cls, second_table, first_table, lhs_on, rhs_on='id'):
        """
        Builds the fragment for joined tables, e.g.
        'join second_table as t2 on t1.lhs_on = t2.rhs_on'
        """
        args = (first_table.alias, lhs_on, second_table.alias, rhs_on)
        identifiers = cls.identifiers(args)
        sql = 'join {} on {}.{} = {}.{}'
        return cls.format(sql, cls.table_name(second_table), *identifiers)

    @classmethod
    def join_columns(cls, tables):
        """
        Produces the list of all columns used in a join.
        """
        columns = []
        for table in tables:
            columns += cls.columns_with_alias(table.columns, table.alias)
        return cls.join(*columns, separator=', ')

    @classmethod
    def join_tables(cls, tables, on):
        join_tables = [cls.table_name(tables[0])]
        for i in range(1, len(tables)):
            table = tables[i]
            join_table = cls.join_table(table, *on[table.name])
            join_tables.append(join_table)
        return cls.join(*join_tables)

    @classmethod
    def join_statement(cls, tables, on):
        return cls.format('select {} from {}', cls.join_columns(tables),
                          cls.join_tables(tables, on))

    @classmethod
    def order_by(cls, **conditions):
        return cls.format('order by {}', cls.order(conditions))

    @classmethod
    def random(cls):
        return SQL('order by random()')

    @classmethod
    def limit(cls, offset=None):
        """
        Builds a limit N [offset N] statement.
        """
        if offset:
            return cls.format(' limit {} offset {}', cls.placeholder(),
                              cls.placeholder())
        return cls.format(' limit {}', cls.placeholder())

    @classmethod
    def insert(cls, table, returning, **kwargs):
        """
        Builds the insert sql.
        """
        columns = cls.columns(kwargs)
        placeholders = cls.placeholders(kwargs)
        sql = 'insert into {} ({}) values ({})'
        args = [sql, cls.identifier(table), columns, placeholders]
        if returning:
            args[0] = f'{args[0]} returning {{}}'
            args.append(Sql.identifier(returning))
        return cls.format(*args)

    @classmethod
    def update(cls, table, **values):
        sql = 'update {} set {}'
        columns = cls.columns_with_placeholder(values)
        return cls.format(sql, cls.identifier(table), columns)

    @classmethod
    def delete(cls, table, **conditions):
        identifier = cls.identifier(table)
        if conditions:
            sql = 'delete from {} where {}'
            return cls.format(sql, identifier, cls.comparison(conditions))
        return cls.format('delete from {}', identifier)

    @classmethod
    def foreign_key(cls, reference, reference_column):
        return SQL(f'uuid references {reference} ({reference_column})')

    @classmethod
    def column(cls, name, column_type, nullable, unique, default, primary):
        if type(column_type) == str:
            args = (cls.identifier(name), )
            sql = f'{{}} {column_type} {nullable} {unique}'
        else:
            sql = f'{{}} {{}} {nullable} {unique}'
            args = (cls.identifier(name), column_type)
        return Sql.format(cls.column_template(sql, default, primary), *args)

    @classmethod
    def extension(cls, extension):
        return cls.format(f'create extension "{extension}"')

    @classmethod
    def table(cls, name, columns):
        sql = 'create table if not exists {} ({})'
        return cls.format(sql, Sql.identifier(name),
                          cls.table_columns(columns))

    @classmethod
    def truncate(cls, table, cascade):
        sql = 'truncate {}'
        if cascade:
            sql = f'{sql} cascade'
        return cls.format(sql, cls.identifier(table))

    @classmethod
    def drop_table(cls, table, cascade):
        sql = 'drop table {}'
        if cascade:
            sql = f'{sql} cascade'
        return cls.format(sql, cls.identifier(table))
