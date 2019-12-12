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


@mark.skip
def test_cursor_fetchall(patch, cursor):
    # NOTE(vesuvium): it's not possible to patch NamedTupleCursor.fetchall
    pass
