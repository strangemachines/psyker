# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
from psyker.Sql import Sql
from psyker.Table import Table
from psyker.columns import Column

from pytest import fixture


@fixture
def table(patch):
    patch.many(Table, ['make_columns', 'make_relationships',
                       'make_reverse_relationships'])
    return Table('db', 'name', col='type')


def test_table_init(table):
    assert table.name == 'name'
    Table.make_columns.assert_called_with({'col': 'type'}, 'uuid')
    assert table.columns == Table.make_columns()
    Table.make_relationships.assert_called_with('db', Table.make_columns())
    assert table.relationships == Table.make_relationships()
    assert table.reverse_relationships == []
    assert table.alias is None
    Table.make_reverse_relationships.assert_called_with('db',
                                                        table.relationships)


def test_table_init__alias(patch):
    patch.many(Table, ['make_columns', 'make_relationships',
                       'make_reverse_relationships'])
    table = Table('db', 'name', alias='other')
    assert table.alias == 'other'


def test_table_init__underscore_name(patch):
    """
    Ensures that a name column is not eaten by that table name.
    """
    patch.many(Table, ['make_columns', 'make_relationships',
                       'make_reverse_relationships'])
    Table('db', 'name', name='hello')
    Table.make_columns.assert_called_with({'name': 'hello'}, 'uuid')


def test_table_column(magic):
    column = magic()
    assert Table.column('name', column) == column


def test_table_column__string(patch):
    patch.init(Column)
    result = Table.column('name', 'int')
    Column.__init__.assert_called_with('name', 'int')
    assert isinstance(result, Column)


def test_table_make_primary_key(patch):
    patch.init(Column)
    result = Table.make_primary_key('uuid')
    Column.__init__.assert_called_with('id', 'uuid', nullable=False,
                                       default='uuid_generate_v1()',
                                       primary_key=True)
    assert isinstance(result, Column)


def test_table_make_primary_key__serial(patch):
    patch.init(Column)
    Table.make_primary_key('serial')
    Column.__init__.assert_called_with('id', 'serial', primary_key=True)


def test_table_make_columns(patch):
    patch.object(Table, 'column')
    result = Table.make_columns({'name': 'type'}, None)
    Table.column.assert_called_with('name', 'type')
    assert result == {'name': Table.column()}


def test_table_make_columns__primary_key(patch):
    patch.many(Table, ['column', 'make_primary_key'])
    result = Table.make_columns({'name': 'type'}, 'primary_key')
    Table.make_primary_key.assert_called_with('primary_key')
    assert result['id'] == Table.make_primary_key()


def test_table_make_relationships(magic, db):
    column = magic()
    result = Table.make_relationships(db, {'col1': column})
    db.get_table.assert_called_with(column.relationship())
    assert result == [db.get_table()]


def test_table_make_relationships__none(magic, db):
    column = magic(relationship=magic(return_value=None))
    assert Table.make_relationships(db, {'col1': column}) == []


def test_table_make_relationships__unique(magic, db):
    column = magic()
    result = Table.make_relationships(db, {'col1': column, 'col2': column})
    assert result == [db.get_table()]


def test_table_make_reverse_relationships(patch, magic, db):
    patch.init(Table)
    relationship = magic()
    table = Table(db, 'name')
    table.make_reverse_relationships(db, [relationship])
    relationship.reverse_relationships.append.assert_called_with(table)


def test_table_sql(patch, magic, table):
    patch.object(Sql, 'table')
    column = magic()
    table.columns = {'col': column}
    result = table.sql()
    Sql.table.assert_called_with('name', [column.sql()])
    assert result == Sql.table()


def test_table_cast(patch, magic, table):
    column = magic()
    table.columns = {'key': column}
    result = table.cast({'key': 'value'})
    column.cast.assert_called_with('value')
    assert result == {'key': column.cast()}


def test_table_as_string(patch, table):
    patch.object(Table, 'sql')
    result = table.as_string('cursor')
    Table.sql().as_string.assert_called_with('cursor')
    assert result == Table.sql().as_string()


def test_table__repr(table):
    assert str(table) == '<Table(name)>'
