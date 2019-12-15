# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
import psycopg2

from .Cursor import Cursor


class Db:
    """
    Responsible for interactions with the database.
    """

    __slots__ = ('url', 'models', 'conn', 'cursor')

    def __init__(self, url, models):
        self.url = url
        self.models = models
        self.conn = None
        self.cursor = None

    @classmethod
    def setup_cursor(cls, conn, models):
        cursor = conn.cursor(cursor_factory=Cursor)
        cursor.models = models
        return cursor

    def get_table(self, table_name):
        """
        Gets the requested table
        """
        return self.models[table_name].__table__

    def connect(self):
        """
        Connects to the database and creates the cursor.
        """
        self.conn = psycopg2.connect(self.url)
        self.conn.set_session(autocommit=True)
        self.cursor = self.setup_cursor(self.conn, self.models)

    def execute(self, query, params, fetch, mode, targets):
        self.cursor.execute(query, params)
        if fetch == 'one':
            return self.cursor.fetchone(targets)
        elif fetch:
            return self.cursor.fetchall(targets)

    def count(self, query, params=None):
        self.cursor.execute(query, params)
        return self.cursor.count()

    def sql(self, query):
        return query.as_string(self.cursor)

    def close(self):
        self.conn.close()
