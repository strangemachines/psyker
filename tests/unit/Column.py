# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
import uuid

from psyker.Column import Column
from psyker.Sql import Sql
from psyker.exceptions import FieldTypeError

from pytest import fixture, mark, raises


@fixture
def column():
    return Column('name', 'int')


def test_column_init(column):
    assert column.name == 'name'
    assert column.field_type == 'int'
    assert column.nullable is True
    assert column.unique is False
    assert column.default is None
    assert column.primary_key is False
    assert column.options == {}


def test_column_init__nullable():
    assert Column('name', 'int', nullable=False).nullable is False


def test_column_init__unique():
    assert Column('name', 'int', unique=True).unique is True


def test_column_init__default():
    assert Column('name', 'int', default='hello').default == 'hello'


def test_column_init__options():
    assert Column('name', 'int', length=64).options == {'length': 64}


@mark.parametrize('field_type,column_type', (
    ('int', 'integer'),
    ('float', 'numeric'),
    ('bool', 'boolean'),
    ('serial', 'serial'),
    ('str', 'varchar'),
    ('text', 'text'),
    ('date', 'date'),
    ('datetime', 'timestamp'),
    ('uuid', 'uuid')
))
def test_column_column_type(column, field_type, column_type):
    column.field_type = field_type
    assert column.column_type() == column_type


def test_column_column_type__varchar_n(column):
    column.field_type = 'str'
    column.options = {'length': 64}
    assert column.column_type() == 'varchar(64)'


def test_column_column_type__foreign_key(patch, column):
    patch.object(Sql, 'foreign_key')
    column.field_type = 'foreign'
    column.options = {'reference': 'reference', 'reference_column': 'column'}
    result = column.column_type()
    Sql.foreign_key.assert_called_with('reference', 'column')
    assert result == Sql.foreign_key()


def test_column_type__error(column):
    column.field_type = 'whatever'
    with raises(FieldTypeError):
        column.column_type()


def test_column_relationship(column):
    column.field_type = 'foreign'
    column.options['reference'] = 'table'
    assert column.relationship() == 'table'


def test_column_relationship__none(column):
    assert column.relationship() is None


def test_column_get_default(column):
    assert column.get_default() == ''


def test_column_get_default__default(column):
    column.default = 'value'
    assert column.get_default() == 'value'


def test_column_sql(patch, column):
    patch.object(Sql, 'column')
    patch.many(Column, ['column_type', 'get_default'])
    result = column.sql()
    Sql.column.assert_called_with('name', Column.column_type(), '', '',
                                  Column.get_default(), False)
    assert result == Sql.column()


def test_column_sql__not_null(patch):
    patch.object(Sql, 'column')
    patch.many(Column, ['column_type', 'get_default'])
    column = Column('name', 'int', nullable=False)
    column.sql()
    Sql.column.assert_called_with('name', Column.column_type(), 'not null', '',
                                  Column.get_default(), False)


def test_column_sql__unique(patch):
    patch.object(Sql, 'column')
    patch.many(Column, ['column_type', 'get_default'])
    column = Column('name', 'int', unique=True)
    column.sql()
    Sql.column.assert_called_with('name', Column.column_type(), '', 'unique',
                                  Column.get_default(), False)


def test_column_cast(column):
    assert column.cast('value') == 'value'


def test_column_cast__uuid(column):
    column.field_type = 'uuid'
    value = uuid.uuid4()
    assert column.cast(value) == str(value)


def test_column_repr(column):
    assert str(column) == '<Column(name, type: int)>'
