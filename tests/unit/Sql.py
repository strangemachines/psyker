# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
from psycopg2.sql import Identifier, Placeholder, SQL

from psyker.Sql import Sql

from pytest import fixture


@fixture
def sql(patch):
    patch.init(SQL)


def test_sql_format(patch, sql):
    patch.object(SQL, 'format')
    result = Sql.format('string', 'args')
    SQL.__init__.assert_called_with('string')
    SQL.format.assert_called_with('args')
    assert result == SQL.format()


def test_sql_join(patch, sql):
    patch.object(SQL, 'join')
    result = Sql.join('one', 'two')
    SQL.__init__.assert_called_with(' ')
    SQL.join.assert_called_with(('one', 'two'))
    assert result == SQL().join()


def test_sql_join__separator(patch, sql):
    patch.object(SQL, 'join')
    Sql.join('one', 'two', separator='.')
    SQL.__init__.assert_called_with('.')


def test_sql_identifier(patch):
    patch.init(Identifier)
    assert isinstance(Sql.identifier('hello'), Identifier)


def test_sql_identifiers(patch):
    patch.init(Identifier)
    assert isinstance(Sql.identifiers(['hello'])[0], Identifier)


def test_sql_table_columns(patch, sql):
    patch.object(SQL, 'join')
    result = Sql.table_columns('columns')
    SQL.__init__.assert_called_with(', ')
    SQL.join.assert_called_with('columns')
    assert result == SQL.join()


def test_sql_placeholder(patch):
    patch.init(Placeholder)
    assert isinstance(Sql.placeholder(), Placeholder)


def test_sql_placeholders(patch, sql):
    patch.object(SQL, 'join')
    patch.object(Sql, 'placeholder')
    result = Sql.placeholders([0])
    SQL.__init__.assert_called_with(', ')
    SQL.join.assert_called_with(Sql.placeholder() * 1)
    assert result == SQL().join()


def test_sql_table_name(patch, table):
    patch.object(Sql, 'identifier')
    table.alias = None
    result = Sql.table_name(table)
    Sql.identifier.assert_called_with(table.name)
    assert result == Sql.identifier()


def test_sql_table_name__alias(patch, table):
    patch.many(Sql, ['identifier', 'format'])
    result = Sql.table_name(table)
    Sql.identifier.assert_called_with(table.alias)
    Sql.format.assert_called_with('{} as {}', Sql.identifier(),
                                  Sql.identifier())
    assert result == Sql.format()


def test_sql_column_template():
    assert Sql.column_template('string', None, False) == 'string'


def test_sql_column_template__default():
    result = Sql.column_template('string', 'value', False)
    assert result == 'string default value'


def test_sql_column_template__primary():
    result = Sql.column_template('string', None, True)
    assert result == 'string primary key'


def test_sql_columns(patch, sql):
    patch.object(SQL, 'join')
    patch.object(Sql, 'identifiers')
    result = Sql.columns({'hello': 'world'})
    SQL.__init__.assert_called_with(', ')
    Sql.identifiers.assert_called_with({'hello': 'world'}.keys())
    SQL.join.assert_called_with(Sql.identifiers())
    assert result == SQL().join()


def test_sql_column_with_placeholder(patch):
    patch.many(Sql, ['format', 'identifier', 'placeholder'])
    result = Sql.column_with_placeholder('col')
    Sql.identifier.assert_called_with('col')
    Sql.format.assert_called_with('{} = {}', Sql.identifier(),
                                  Sql.placeholder())
    assert result == Sql.format()


def test_sql_column_with_value(patch):
    patch.many(Sql, ['format', 'identifier', 'placeholder'])
    result = Sql.column_with_value(('col', 'operator', 'value'))
    Sql.identifier.assert_called_with('col')
    Sql.format.assert_called_with('{} operator {}', Sql.identifier(),
                                  Sql.placeholder())
    assert result == Sql.format()


def test_columns_with_placeholder(patch, sql):
    patch.object(SQL, 'join')
    patch.object(Sql, 'column_with_placeholder')
    result = Sql.columns_with_placeholder({'col': 'value'})
    Sql.column_with_placeholder.assert_called_with('col')
    SQL.__init__.assert_called_with(', ')
    SQL.join.assert_called_with([Sql.column_with_placeholder()])
    assert result == SQL().join()


def test_columns_with_values(patch):
    patch.object(Sql, 'column_with_value')
    result = list(Sql.columns_with_values({'col': 'value'}))
    Sql.column_with_value.assert_called_with('col')
    assert result == [Sql.column_with_value()]


def test_columns_with_alias(patch):
    patch.many(Sql, ['identifier', 'format'])
    result = Sql.columns_with_alias({'col': 'value'}, 'alias')
    Sql.identifier.assert_called_with('col')
    Sql.format.assert_called_with('{}.{}', Sql.identifier(), Sql.identifier())
    assert result == [Sql.format()]


def test_sql_comparison(patch, sql):
    patch.object(SQL, 'join')
    patch.object(Sql, 'columns_with_values')
    result = Sql.comparison({'col': 'value'})
    Sql.columns_with_values.assert_called_with({'col': 'value'})
    SQL.join.assert_called_with(Sql.columns_with_values())
    assert result == SQL().join()


def test_sql_order(patch, sql):
    patch.object(SQL, 'join')
    patch.many(Sql, ['identifier', 'format'])
    result = Sql.order({'col': 'desc'})
    Sql.identifier.assert_called_with('col')
    Sql.format.assert_called_with('{} desc', Sql.identifier())
    SQL.__init__.assert_called_with(', ')
    SQL.join.assert_called_with([Sql.format()])
    assert result == SQL.join()


def test_sql_where(patch):
    patch.many(Sql, ['format', 'comparison'])
    result = Sql.where({'col': 'value'})
    Sql.comparison.assert_called_with({'col': 'value'})
    Sql.format.assert_called_with('where {}', Sql.comparison())
    assert result == Sql.format()


def test_sql_count(patch):
    patch.many(Sql, ['format', 'identifier'])
    result = Sql.count('hello')
    Sql.identifier.assert_called_with('hello')
    Sql.format.assert_called_with('select count(*) from {}', Sql.identifier())
    assert result == Sql.format()


def test_sql_select(patch):
    patch.many(Sql, ['format', 'columns', 'identifier'])
    result = Sql.select('hello', {'col': 'value'})
    Sql.identifier.assert_called_with('hello')
    Sql.format.assert_called_with('select {} from {}', Sql.columns(),
                                  Sql.identifier())
    assert result == Sql.format()


def test_sql_join_table(patch, magic, table):
    patch.many(Sql, ['identifiers', 'table_name', 'format'])
    first_table = magic()
    result = Sql.join_table(table, first_table, 'col')
    args = (first_table.alias, 'col', table.alias, 'id')
    Sql.identifiers.assert_called_with(args)
    Sql.format.assert_called_with('join {} on {}.{} = {}.{}', Sql.table_name(),
                                  *Sql.identifiers())
    assert result == Sql.format()


def test_sql_join_table__rhs(patch, magic, table):
    patch.many(Sql, ['identifiers', 'table_name', 'format'])
    first_table = magic()
    Sql.join_table(table, first_table, 'col', 'fk')
    args = (first_table.alias, 'col', table.alias, 'fk')
    Sql.identifiers.assert_called_with(args)


def test_sql_join_columns(patch, table):
    patch.many(Sql, ['columns_with_alias', 'join'])
    result = Sql.join_columns([table])
    Sql.columns_with_alias.assert_called_with(table.columns, table.alias)
    Sql.join.assert_called_with(*[] + Sql.columns_with_alias(),
                                separator=', ')
    assert result == Sql.join()


def test_sql_join_tables(patch, magic, table):
    patch.many(Sql, ['join', 'table_name', 'join_table'])
    second_table = magic()
    result = Sql.join_tables([table, second_table],
                             {second_table.name: (table, 'col', 'rhs')})
    Sql.table_name.assert_called_with(table)
    Sql.join_table.assert_called_with(second_table, table, 'col', 'rhs')
    Sql.join.assert_called_with(Sql.table_name(), Sql.join_table())
    assert result == Sql.join()


def test_sql_join_statement(patch):
    patch.many(Sql, ['format', 'join_columns', 'join_tables'])
    result = Sql.join_statement('tables', 'on')
    Sql.join_columns.assert_called_with('tables')
    Sql.join_tables.assert_called_with('tables', 'on')
    Sql.format.assert_called_with('select {} from {}', Sql.join_columns(),
                                  Sql.join_tables())
    assert result == Sql.format()


def test_sql_order_by(patch):
    patch.many(Sql, ['format', 'order'])
    result = Sql.order_by(col='desc')
    Sql.order.assert_called_with({'col': 'desc'})
    Sql.format.assert_called_with('order by {}', Sql.order())
    assert result == Sql.format()


def test_sql_random(sql):
    result = Sql.random()
    SQL.__init__.assert_called_with('order by random()')
    assert isinstance(result, SQL)


def test_sql_limit(patch):
    patch.many(Sql, ['format', 'placeholder'])
    result = Sql.limit()
    Sql.format.assert_called_with(' limit {}', Sql.placeholder())
    assert result == Sql.format()


def test_sql_limit__offset(patch):
    patch.many(Sql, ['format', 'placeholder'])
    Sql.limit(100)
    Sql.format.assert_called_with(' limit {} offset {}', Sql.placeholder(),
                                  Sql.placeholder())


def test_sql_insert(patch):
    patch.many(Sql, ['format', 'identifier', 'columns', 'placeholders'])
    result = Sql.insert('hello', None, message='world')
    Sql.columns.assert_called_with({'message': 'world'})
    Sql.placeholders.assert_called_with({'message': 'world'})
    Sql.identifier.assert_called_with('hello')
    sql = 'insert into {} ({}) values ({})'
    Sql.format.assert_called_with(sql, Sql.identifier(), Sql.columns(),
                                  Sql.placeholders())
    assert result == Sql.format()


def test_sql_insert__returning(patch):
    patch.many(Sql, ['format', 'identifier', 'columns', 'placeholders'])
    Sql.insert('hello', 'col', message='world')
    Sql.identifier.assert_called_with('col')
    sql = 'insert into {} ({}) values ({}) returning {}'
    Sql.format.assert_called_with(sql, Sql.identifier(), Sql.columns(),
                                  Sql.placeholders(), Sql.identifier())


def test_sql_update(patch):
    patch.many(Sql, ['format', 'identifier', 'columns_with_placeholder'])
    result = Sql.update('hello', message='pizza')
    Sql.identifier.assert_called_with('hello')
    Sql.columns_with_placeholder.assert_called_with({'message': 'pizza'})
    Sql.format.assert_called_with('update {} set {}', Sql.identifier(),
                                  Sql.columns_with_placeholder())
    assert result == Sql.format()


def test_sql_delete(patch):
    patch.many(Sql, ['format', 'identifier'])
    result = Sql.delete('table')
    Sql.identifier.assert_called_with('table')
    Sql.format.assert_called_with('delete from {}', Sql.identifier())
    assert result == Sql.format()


def test_sql_delete__conditions(patch):
    patch.many(Sql, ['format', 'identifier', 'comparison'])
    Sql.delete('table', col='value')
    Sql.comparison.assert_called_with({'col': 'value'})
    Sql.format.assert_called_with('delete from {} where {}', Sql.identifier(),
                                  Sql.comparison())


def test_sql_foreign_key(patch, sql):
    patch.object(Sql, 'identifiers')
    result = Sql.foreign_key('ref', 'column')
    SQL.__init__.assert_called_with('uuid references ref (column)')
    assert isinstance(result, SQL)


def test_sql_column(patch):
    patch.many(Sql, ['format', 'identifier', 'column_template'])
    result = Sql.column('name', 'type', 'null', 'unique', 'value', False)
    Sql.column_template.assert_called_with('{} type null unique', 'value',
                                           False)
    Sql.identifier.assert_called_with('name')
    Sql.format.assert_called_with(Sql.column_template(), Sql.identifier())
    assert result == Sql.format()


def test_sql_column__sql(patch, magic):
    patch.many(Sql, ['format', 'identifier', 'column_template'])
    column = magic()
    Sql.column('name', column, 'null', 'unique', 'value', False)
    Sql.column_template.assert_called_with('{} {} null unique', 'value', False)
    Sql.format.assert_called_with(Sql.column_template(), Sql.identifier(),
                                  column)


def test_table_extension(patch):
    patch.object(Sql, 'format')
    result = Sql.extension('ext')
    Sql.format.assert_called_with('create extension "ext"')
    assert result == Sql.format()


def test_sql_table(patch):
    patch.many(Sql, ['format', 'identifier', 'table_columns'])
    result = Sql.table('name', ['columns'])
    Sql.identifier.assert_called_with('name')
    Sql.table_columns.assert_called_with(['columns'])
    Sql.format.assert_called_with('create table if not exists {} ({})',
                                  Sql.identifier(), Sql.table_columns())
    assert result == Sql.format()


def test_sql_truncate(patch):
    patch.many(Sql, ['format', 'identifier'])
    result = Sql.truncate('table', False)
    Sql.identifier.assert_called_with('table')
    Sql.format.assert_called_with('truncate {}', Sql.identifier())
    assert result == Sql.format()


def test_sql_truncate__cascade(patch):
    patch.many(Sql, ['format', 'identifier'])
    Sql.truncate('table', True)
    Sql.format.assert_called_with('truncate {} cascade', Sql.identifier())


def test_sql_drop_table(patch):
    patch.many(Sql, ['format', 'identifier'])
    result = Sql.drop_table('table', False)
    Sql.identifier.assert_called_with('table')
    Sql.format.assert_called_with('drop table {}', Sql.identifier())
    assert result == Sql.format()


def test_sql_drop_table__cascade(patch):
    patch.many(Sql, ['format', 'identifier'])
    Sql.drop_table('table', True)
    Sql.format.assert_called_with('drop table {} cascade', Sql.identifier())
