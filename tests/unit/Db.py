# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
import psycopg2
from psycopg2 import OperationalError

from psyker.Cursor import Cursor
from psyker.Db import Db
from psyker.exceptions import ConnectionError

from pytest import fixture, raises


@fixture
def db():
    return Db('url', {'name': 'model'})


@fixture
def conn(magic):
    return magic()


def test_db_init(db):
    assert db.url == 'url'
    assert db.models == {'name': 'model'}
    assert db.conn is None
    assert db.cursor is None


def test_db_setup_cursor(patch, conn):
    result = Db.setup_cursor(conn, 'models')
    conn.cursor.assert_called_with(cursor_factory=Cursor)
    assert result == conn.cursor()
    assert result.models == 'models'


def test_db_get_table(magic, db):
    db.models = {'table_name': magic(__table__='table')}
    result = db.get_table('table_name')
    assert result == db.models['table_name'].__table__


def test_db_connect(patch, db):
    patch.object(psycopg2, 'connect')
    patch.object(Db, 'setup_cursor')
    db.connect()
    psycopg2.connect.assert_called_with('url')
    psycopg2.connect().set_session.assert_called_with(autocommit=True)
    Db.setup_cursor.assert_called_with(db.conn, db.models)
    assert db.conn == psycopg2.connect()
    assert db.cursor == Db.setup_cursor()


def test_db_connect__connection_error(patch, db):
    patch.object(psycopg2, 'connect', side_effect=OperationalError)
    patch.object(Db, 'setup_cursor')
    with raises(ConnectionError):
        db.connect()


def test_db_execute(magic, db):
    db.cursor = magic()
    db.execute('query', 'params', 'fetch', 'mode', 'targets')
    db.cursor.execute.assert_called_with('query', 'params')


def test_db_execute__fetch(magic, db):
    db.cursor = magic()
    result = db.execute('query', 'params', True, 'mode', 'targets')
    db.cursor.fetchall.assert_called_with('targets', mode='mode')
    assert result == db.cursor.fetchall()


def test_db_execute__fetchone(magic, db):
    db.cursor = magic()
    result = db.execute('query', 'params', 'one', 'mode', 'targets')
    db.cursor.fetchone.assert_called_with('targets', mode='mode')
    assert result == db.cursor.fetchone()


def test_db_count(magic, db):
    db.cursor = magic()
    result = db.count('query')
    db.cursor.execute.assert_called_with('query', None)
    assert result == db.cursor.count()


def test_db_count__params(magic, db):
    db.cursor = magic()
    db.count('query', 'params')
    db.cursor.execute.assert_called_with('query', 'params')


def test_db_sql(magic, db):
    query = magic()
    result = db.sql(query)
    query.as_string.assert_called_with(db.cursor)
    assert result == query.as_string()


def test_db_close(magic, db):
    db.conn = magic()
    db.close()
    assert db.conn.close.call_count == 1
