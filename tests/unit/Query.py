# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
from psyker.Query import Query
from psyker.Sql import Sql

from pytest import fixture, mark


@fixture
def query():
    return Query('db', 'type', 'table')


def test_query_init(query):
    assert query.db == 'db'
    assert query.targets == ['table']
    assert query.query_type == 'type'
    assert query.params == []
    assert query.conditions is None
    assert query.order is None
    assert query.options == {}
    assert query._limit is None
    assert query._offset is None


def test_query_init__options():
    query = Query('db', 'type', 'table', key='value')
    assert query.options['key'] == 'value'


def test_query_parse_condition():
    assert Query.parse_condition(('col', 'value')) == ('col', '=', 'value')


@mark.parametrize('operator', ('!', '<', '>', '<=', '>='))
def test_query_parse_condition__operator(operator):
    result = Query.parse_condition(('col', f'{operator}value'))
    assert result == ('col', operator, 'value')


@mark.parametrize('value', [
    ('>', 'value'), ('>=', 'value')
])
def test_query_parse_condition__tuple(value):
    result = Query.parse_condition(('col', value))
    assert result == ('col', value[0], (value[1], ))


def test_query_head(patch, table):
    patch.object(Sql, 'select')
    result = Query.head('select', [table], 'options')
    Sql.select.assert_called_with(table.name, table.columns)
    assert result == Sql.select()


def test_query_head__update(patch, table):
    patch.object(Sql, 'update')
    result = Query.head('update', [table, {'col': 'value'}], 'options')
    Sql.update.assert_called_with(table.name, col='value')
    assert result == Sql.update()


def test_query_head__delete(patch, table):
    patch.object(Sql, 'delete')
    result = Query.head('delete', [table], 'options')
    Sql.delete.assert_called_with(table.name)
    assert result == Sql.delete()


def test_query_head__drop(patch, table):
    patch.object(Sql, 'drop_table')
    result = Query.head('drop', [table], {'cascade': False})
    Sql.drop_table.assert_called_with(table.name, False)
    assert result == Sql.drop_table()


def test_query_head__count(patch, table):
    patch.object(Sql, 'count')
    result = Query.head('count', [table], 'options')
    Sql.count.assert_called_with(table.name)
    assert result == Sql.count()


def test_query_head__join(patch, table):
    patch.object(Sql, 'join_statement')
    result = Query.head('join', [table], 'options')
    Sql.join_statement.assert_called_with([table], 'options')
    assert result == Sql.join_statement()


def test_query_select(patch):
    patch.init(Query)
    result = Query.select('db', 'table')
    Query.__init__.assert_called_with('db', 'select', 'table')
    assert isinstance(result, Query)


def test_query_update(patch):
    patch.init(Query)
    result = Query.update('db', 'table', {'col': 'value'})
    Query.__init__.assert_called_with('db', 'update', 'table')
    assert isinstance(result, Query)
    assert result.targets == ('table', {'col': 'value'})
    assert result.params == ['value']


def test_query_delete(patch):
    patch.init(Query)
    result = Query.delete('db', 'table')
    Query.__init__.assert_called_with('db', 'delete', 'table')
    assert isinstance(result, Query)
    assert result.params == []


def test_query_delete__conditions(patch):
    patch.init(Query)
    result = Query.delete('db', 'table', col='value')
    assert result.params == ['value']


def test_query_drop(patch):
    patch.init(Query)
    result = Query.drop('db', 'table', 'cascade')
    Query.__init__.assert_called_with('db', 'drop', 'table', cascade='cascade')
    assert isinstance(result, Query)


def test_query_count(patch):
    patch.init(Query)
    result = Query.count('db', 'table')
    Query.__init__.assert_called_with('db', 'count', 'table')
    assert isinstance(result, Query)


def test_query_on(magic):
    target = magic(columns=['on'])
    assert Query.on([target], 'on') == (target, 'on', 'id')


def test_query_on__tuple():
    result = Query.on(['target'], ('table', 'column'))
    assert result == ('table', 'column', 'id')


def test_query_on__one_to_many(magic):
    target = magic(columns=[])
    assert Query.on([target], 'on') == (target, 'id', 'on')


def test_query_join(patch, query, table):
    patch.object(Query, 'on')
    query.join(table, 'on')
    Query.on.assert_called_with(query.targets, 'on')
    assert query.targets == ['table', table]
    assert query.query_type == 'join'
    assert query.options[table.name] == Query.on()


def test_query_join__type(patch, query, table):
    patch.object(Query, 'on')
    query.join(table, 'on', 'outer')
    assert query.query_type == 'outer'


def test_query_where(patch, query):
    patch.object(Query, 'parse_condition', return_value=[0, 1, 2])
    query.where(col='value')
    Query.parse_condition.assert_called_with(('col', 'value'))
    assert query.conditions == [Query.parse_condition()]
    assert query.params == [2]


def test_query_order_by(query):
    query.order_by(col='desc')
    assert query.order == {'col': 'desc'}


def test_query_random(query):
    query.random()
    assert query.order == 'random'


def test_query_limit(query):
    query.limit(1)
    assert query._limit == 1
    assert query.params == [1]


def test_query_limit__offset(query):
    query.limit(1, 2)
    assert query._offset == 2


def test_query_build(patch, query):
    patch.object(Query, 'head')
    result = query.build()
    Query.head.assert_called_with(query.query_type, query.targets,
                                  query.options)
    assert result == Query.head()


def test_query_build__conditions(patch, query):
    patch.many(Sql, ['join', 'where'])
    patch.object(Query, 'head')
    query.conditions = {'col': 'value'}
    result = query.build()
    Sql.where.assert_called_with(query.conditions)
    Sql.join.assert_called_with(Query.head(), Sql.where())
    assert result == Sql.join()


def test_query_build__order(patch, query):
    patch.many(Sql, ['join', 'order_by'])
    patch.object(Query, 'head')
    query.order = {'col': 'desc'}
    result = query.build()
    Sql.order_by.assert_called_with(**query.order)
    Sql.join.assert_called_with(Query.head(), Sql.order_by())
    assert result == Sql.join()


def test_query_build__order_random(patch, query):
    patch.many(Sql, ['join', 'random'])
    patch.object(Query, 'head')
    query.order = 'random'
    result = query.build()
    Sql.join.assert_called_with(Query.head(), Sql.random())
    assert result == Sql.join()


def test_query_build__limit(patch, query):
    patch.many(Sql, ['join', 'limit'])
    patch.object(Query, 'head')
    query._limit = 1
    result = query.build()
    Sql.limit.assert_called_with(query._offset)
    Sql.join.assert_called_with(Query.head(), Sql.limit())
    assert result == Sql.join()


def test_query_sql(patch, magic, query):
    patch.object(Query, 'build')
    query.db = magic()
    result = query.sql()
    query.db.sql.assert_called_with(Query.build())
    assert result == query.db.sql()


def test_query_execute(patch, magic, query):
    patch.object(Query, 'build')
    query.db = magic()
    result = query.execute('fetch', 'mode')
    query.db.execute.assert_called_with(Query.build(), [], 'fetch', 'mode',
                                        ['table'])
    assert result == query.db.execute()


def test_query_execute__count(patch, magic, query):
    patch.object(Query, 'build')
    query.db = magic()
    query.query_type = 'count'
    result = query.execute('fetch', 'mode')
    query.db.count.assert_called_with(Query.build(), [])
    assert result == query.db.count()
