# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
from .Sql import Sql


class Query:
    """
    Responsible for creating sql queries from chains of methods.

    This is necessary because some queries need to look at the entire chain to
    build the correct SQL statement. Joins are the most prominent example.
    """

    __slots__ = ('db', 'query_type', 'targets', 'params', 'conditions',
                 'order', 'options', '_limit', '_offset')

    def __init__(self, db, query_type, table, **kwargs):
        self.db = db
        self.targets = [table]
        self.query_type = query_type
        self.params = []
        self.conditions = None
        self.order = None
        self.options = kwargs
        self._limit = None
        self._offset = None

    @staticmethod
    def parse_condition(condition):
        """
        Transforms conditions in the correct format.

        Conditions are given by users as key-value params, and arrive here as
        (key, value) tuple.

        Value can be a string with an operator prefixed, e.g. '>value', or a
        tuple like (operator, value).
        """
        column = condition[0]
        value = condition[1]
        if value[0:2] in ('>=', '<='):
            return (column, value[0:2], value[2:])
        elif value[0] in ('!', '>', '<'):
            return (column, value[0], value[1:])
        elif value[0] in ('<=', '>='):
            return (column, value[0], (value[1], ))
        return (column, '=', value)

    @staticmethod
    def head(query_type, targets, options):
        """
        Creates the head of an sql statement, according to the query type.
        """
        if query_type == 'select':
            return Sql.select(targets[0].name, targets[0].columns)
        elif query_type == 'update':
            return Sql.update(targets[0].name, **targets[1])
        elif query_type == 'delete':
            return Sql.delete(targets[0].name)
        elif query_type == 'drop':
            return Sql.drop_table(targets[0].name, options['cascade'])
        elif query_type == 'count':
            return Sql.count(targets[0].name)
        elif query_type == 'join':
            return Sql.join_statement(targets, options)

    @staticmethod
    def select(db, table):
        return Query(db, 'select', table)

    @staticmethod
    def update(db, table, values):
        query = Query(db, 'update', table)
        query.targets = (table, values)
        query.params = list(values.values())
        return query

    @staticmethod
    def delete(db, table, **conditions):
        query = Query(db, 'delete', table)
        query.params = list(conditions.values())
        return query

    @staticmethod
    def drop(db, table, cascade):
        return Query(db, 'drop', table, cascade=cascade)

    @staticmethod
    def count(db, table):
        return Query(db, 'count', table)

    @staticmethod
    def on(targets, on):
        """
        Normalizes on to a (table, column) tuple.

        This allows to specify the table of a complex join, without making
        simple joins harder.
        """
        if type(on) == tuple:
            return (on[0], on[1], 'id')
        if on in targets[0].columns:
            return (targets[0], on, 'id')
        # NOTE(vesuvium): when performing a one_to_many we switch id and on
        # Clearly, if the first target does not have the on column, than the
        # join might be a one_to_many, although it might also be an erronous
        # join.
        return (targets[0], 'id', on)

    def join(self, table, on, join_type=None):
        """
        Adds a join fragment to the current query.
        """
        if join_type is None:
            join_type = 'join'
        self.targets.append(table)
        self.query_type = join_type
        self.options[table.name] = self.on(self.targets, on)

    def where(self, **conditions):
        self.conditions = [
            self.parse_condition(condition) for condition in conditions.items()
        ]
        self.params = [condition[2] for condition in self.conditions]

    def order_by(self, **order):
        self.order = order

    def random(self):
        self.order = 'random'

    def limit(self, limit, offset=None):
        # NOTE(vesuvium): limits are passed in parameters, so Sql needs
        # to create only the placeholder
        self._limit = limit
        self.params.append(limit)
        if offset:
            self._offset = offset

    def build(self):
        sql = self.head(self.query_type, self.targets, self.options)
        if self.conditions:
            sql = Sql.join(sql, Sql.where(self.conditions))
        if self.order == 'random':
            sql = Sql.join(sql, Sql.random())
        elif self.order:
            sql = Sql.join(sql, Sql.order_by(**self.order))
        if self._limit:
            sql = Sql.join(sql, Sql.limit(self._offset))
        return sql

    def sql(self):
        return self.db.sql(self.build())

    def execute(self, fetch, mode):
        if self.query_type == 'count':
            return self.db.count(self.build(), self.params)
        return self.db.execute(self.build(), self.params, fetch, mode,
                               self.targets)
