# Copyright (C) 2003-2019 Federico Di Gregorio  <fog@debian.org>
#
# psycopg2 is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# In addition, as a special exception, the copyright holders give
# permission to link this program with the OpenSSL library (or with
# modified versions of OpenSSL that use the same license as OpenSSL),
# and distribute linked combinations including the two.
#
# You must obey the GNU Lesser General Public License in all respects for
# all of the code used other than OpenSSL.
#
# psycopg2 is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
# License for more details.
# -*- coding: utf-8 -*-
from psycopg2.extras import NamedTupleCursor


class Cursor(NamedTupleCursor):
    """
    A cursor specific for Psyker.

    psycopg2 does not have an argument to set rename to True.
    """

    @staticmethod
    def columns(description):
        return [column.name for column in description]

    @staticmethod
    def rows_dict(columns, rows):
        return [dict(zip(columns, row)) for row in rows]

    def count(self):
        return super(NamedTupleCursor, self).fetchall()[0][0]

    def fetch_returned(self):
        """
        Fetches the result of a returning clause.
        """
        row = super(NamedTupleCursor, self).fetchone()
        return row[0]

    def fetchone(self, targets):
        row = super(NamedTupleCursor, self).fetchone()
        table = targets[0]
        row_dict = dict(zip(table.columns.keys(), row))
        return self.models[table.name](**row_dict)

    def make(self, model, row, columns, targets, row_start=0):
        instance = model(**dict(zip(columns, row)))
        if targets:
            target = targets[0]
            columns = target.columns
            model = self.models[target.name]
            row_start = len(columns)
            related = self.make(model, row[row_start:], columns.keys(),
                                targets[1:])
            setattr(instance, target.name, (related, ))
        return instance

    def fetchall(self, targets):
        rows = super(NamedTupleCursor, self).fetchall()
        columns = targets[0].columns.keys()
        model = self.models[targets[0].name]
        return [self.make(model, row, columns, targets[1:]) for row in rows]
