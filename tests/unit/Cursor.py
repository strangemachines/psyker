# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
from psycopg2.extras import NamedTupleCursor

from psyker.Cursor import Cursor

from pytest import fixture, mark


@fixture
def cursor(patch):
    patch.init(Cursor)
    return Cursor('conn')


def test_cursor():
    assert issubclass(Cursor, NamedTupleCursor)


def test_cursor_columns(magic):
    column = magic()
    assert Cursor.columns([column]) == [column.name]


def test_cursor_rows_dict():
    assert Cursor.rows_dict(['col'], [['value']]) == [{'col': 'value'}]


@mark.skip
def test_cursor_fetch_returned(patch, cursor):
    patch.object(NamedTupleCursor, 'fetchone')
    assert cursor.fetch_returned() == NamedTupleCursor.fetchone()[0]


@mark.skip
def test_cursor_count(patch, cursor):
    patch.object(NamedTupleCursor, 'fetchall')
    assert cursor.count() == NamedTupleCursor.fetchall()[0][0]


@mark.skip
def test_cursor_fetchone(patch, cursor):
    # NOTE(vesuvium): it's not possible to patch NamedTupleCursor.fetchone
    pass


def test_cursor_make_related(patch, magic, cursor):
    patch.object(Cursor, 'make')
    target = magic(columns={'col': 'col'})
    cursor.models = {target.name: 'model'}
    result = cursor.make_related('row', [target])
    Cursor.make.assert_called_with('model', 'ow', {'col': 'col'}.keys(), [])
    assert result == Cursor.make()


def test_cursor_make_related__mode(patch, magic, cursor):
    patch.object(Cursor, 'make_dicts')
    target = magic(columns={'col': 'col'})
    cursor.models = {target.name: 'model'}
    result = cursor.make_related('row', [target], mode='dictionaries')
    Cursor.make_dicts.assert_called_with('model', 'ow', {'col': 'col'}.keys(),
                                         [])
    assert result == Cursor.make_dicts()


def test_cursor_make(magic, cursor):
    model = magic()
    result = cursor.make(model, ['value'], ['col'], None)
    model.assert_called_with(col='value')
    assert result == model()


def test_cursor_make__related(patch, magic, cursor, target):
    patch.object(Cursor, 'make_related')
    model = magic()
    result = cursor.make(model, ['value'], ['col'], [target])
    Cursor.make_related.assert_called_with(['value'], [target])
    assert result.target == (Cursor.make_related(), )


def test_cursor_make_dicts(cursor):
    result = cursor.make_dicts(['value'], ['col'], None)
    assert result == {'col': 'value'}


def test_cursor_make_dicts__related(patch, cursor, target):
    patch.object(Cursor, 'make_related')
    result = cursor.make_dicts(['value'], ['col'], [target])
    Cursor.make_related.assert_called_with(['value'], [target], 'dictionaries')
    assert result['target'] == (Cursor.make_related(), )


@mark.skip
def test_cursor_fetchall(patch, cursor):
    # NOTE(vesuvium): it's not possible to patch NamedTupleCursor.fetchall
    pass
