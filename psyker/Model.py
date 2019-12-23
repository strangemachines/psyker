# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
from .Query import Query
from .Sql import Sql
from .Table import Table


class Model:
    __db__ = None
    __table__ = None
    __query__ = None
    __slots__ = ()

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    @classmethod
    def columns(cls):
        """
        This needs to be implement in models to deine columns.
        """
        raise NotImplementedError()

    @classmethod
    def setup(cls, db, alias):
        """
        Makes a model usable by setting the database and building the
        underlying table.
        """
        cls.__db__ = db
        cls.__table__ = Table(db, cls.__name__.lower(), **cls.columns(),
                              alias=alias)

    @classmethod
    def create_table(cls):
        cls.__db__.execute(cls.__table__.sql(), None, None, None, None)

    @classmethod
    def execute(cls, fetch=None, mode=None):
        result = cls.__query__.execute(fetch, mode)
        cls.__query__ = None
        return result

    def related_as_dictionary(self, values):
        for relationship in self.__table__.relationships:
            name = relationship.name
            if hasattr(self, name):
                values[name] = [
                    item.as_dictionary() for item in getattr(self, name)
                ]
        return values

    def as_dictionary(self):
        """
        Returns the instance as dictionary. Intended to be used to convert to
        a dictionary on the fly. Otherwise, Model.dictionary and
        Model.dictionaries are faster.
        """
        values = {}
        for column in self.__table__.columns.keys():
            if column != 'id':
                values[column] = getattr(self, column)
        if hasattr(self, 'id'):
            values['id'] = self.id
        return self.related_as_dictionary(values)

    def save(self, fetch='id'):
        """
        Saves an instance of the model to the database.
        """
        values = self.__table__.cast(self.as_dictionary())
        sql = Sql.insert(self.__table__.name, fetch, **values)
        self.__db__.execute(sql, list(values.values()), None, None, None)
        if fetch:
            id = self.__db__.cursor.fetch_returned()
            return self.select().where(id=id).one()

    @classmethod
    def update(cls, **values):
        cls.__query__ = Query.update(cls.__db__, cls.__table__, values)
        return cls

    @classmethod
    def where(cls, **conditions):
        """
        Adds a where clause to the current query
        """
        cls.__query__.where(**conditions)
        return cls

    @classmethod
    def join(cls, table, on, join_type=None):
        """
        Adds a join clause to the current query.
        """
        if type(on) == tuple:
            on = (cls.__db__.get_table(on[0]), on[1])
        cls.__query__.join(cls.__db__.get_table(table), on, join_type)
        return cls

    @classmethod
    def select(cls, **conditions):
        cls.__query__ = Query.select(cls.__db__, cls.__table__)
        if conditions:
            cls.__query__.where(**conditions)
        return cls

    @classmethod
    def count(cls, **conditions):
        cls.__query__ = Query.count(cls.__db__, cls.__table__)
        if conditions:
            cls.__query__.where(**conditions)
        return cls

    @classmethod
    def get(cls):
        if cls.__query__ is None:
            cls.select()
        return cls.execute(fetch=True)

    @classmethod
    def dictionaries(cls):
        """
        Produces a list of dictionaries instead of model instances, at the
        cursor level.
        """
        if cls.__query__ is None:
            cls.select()
        return cls.execute(fetch=True, mode='dictionaries')

    @classmethod
    def dictionary(cls):
        if cls.__query__ is None:
            cls.select()
        return cls.execute(fetch='one', mode='dictionaries')

    @classmethod
    def one(cls):
        if cls.__query__ is None:
            cls.select()
        return cls.execute(fetch='one')

    @classmethod
    def order_by(cls, **conditions):
        cls.__query__.order_by(**conditions)
        return cls

    @classmethod
    def random(cls):
        cls.__query__.random()
        return cls

    @classmethod
    def limit(cls, limit, offset=None):
        cls.__query__.limit(limit, offset)
        return cls

    @classmethod
    def paginate(cls, page, items):
        """
        Wraps Model.limit to make paginating easier.
        """
        return cls.limit(items, offset=page * items)

    @classmethod
    def delete(cls, **conditions):
        cls.__query__ = Query.delete(cls.__db__, cls.__table__)
        if conditions:
            cls.__query__.where(**conditions)
        return cls

    @classmethod
    def truncate(cls, cascade=None):
        cls.__query__ = Query.truncate(cls.__db__, cls.__table__, cascade)
        return cls.execute()

    @classmethod
    def drop(cls, cascade=None):
        cls.__query__ = Query.drop(cls.__db__, cls.__table__, cascade)
        return cls.execute()

    @classmethod
    def sql(cls):
        """
        Produces the SQL of the current query.
        """
        return cls.__query__.sql()

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            if other.id == self.id:
                return True
        return False

    def __repr__(self):
        return f'<{self.__table__.name.capitalize()}({self.id})>'
