# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
from psyker.Model import Model
from psyker.Query import Query
from psyker.Sql import Sql
from psyker.Table import Table

from pytest import fixture, mark, raises


@fixture
def query(magic):
    return magic()


@fixture
def db(magic):
    return magic()


@fixture
def model(patch):
    patch.init(Model)
    return Model(col='value')


def test_model():
    assert Model.__db__ is None
    assert Model.__table__ is None
    assert Model.__query__ is None


def test_model_init():
    class Users(Model):
        __slots__ = ('col', )
    model = Users(col='val')
    assert model.col == 'val'


def test_model_columns():
    with raises(NotImplementedError):
        Model.columns()


def test_model_setup(patch):
    patch.init(Table)
    patch.object(Model, 'columns')
    Model.setup('db', 'alias')
    assert Model.__db__ == 'db'
    Table.__init__.assert_called_with('db', 'model', **Model.columns(),
                                      alias='alias')
    assert isinstance(Model.__table__, Table)


def test_create_table(magic):
    Model.__table__ = magic()
    Model.__db__ = magic()
    Model.create_table()
    Model.__db__.execute.assert_called_with(Model.__table__.sql(), None, None,
                                            None, None)


def test_model_execute(query):
    Model.__query__ = query
    result = Model.execute()
    query.execute.assert_called_with(None, None)
    assert result == query.execute()
    assert Model.__query__ is None


def test_model_execute__fetch(query):
    Model.__query__ = query
    Model.execute(fetch=True)
    query.execute.assert_called_with(True, None)


def test_model_execute_mode(query):
    Model.__query__ = query
    Model.execute(mode='mode')
    query.execute.assert_called_with(None, 'mode')


def test_model_related_as_dictionary(magic, model, table):
    table.relationships = [magic()]
    table.relationships[0].name = 'rel'
    Model.__table__ = table
    item = magic()
    Model.rel = [item]
    assert model.related_as_dictionary({}) == {'rel': [item.as_dictionary()]}


def test_model_related_as_dictionary__none(model, table):
    Model.__table__ = table
    assert model.related_as_dictionary({}) == {}


def test_model_as_dictionary(model, table):
    table.columns = {'col': 'value'}
    Model.__table__ = table
    Model.col = 'value'
    result = model.as_dictionary()
    assert result == {'col': 'value'}


def test_model_as_dictionary__id(model, table):
    table.columns = {'col': 'value', 'id': 'id'}
    Model.__table__ = table
    Model.col = 'value'
    Model.id = 'id'
    assert model.as_dictionary() == {'col': 'value', 'id': 'id'}


def test_model_save(patch, model, table, db):
    patch.object(Model, 'as_dictionary')
    patch.object(Sql, 'insert')
    Model.__table__ = table
    Model.__db__ = db
    model.save(fetch=None)
    table.cast.assert_called_with(Model.as_dictionary())
    Sql.insert.assert_called_with(table.name, None, **table.cast())
    Model.__db__.execute.assert_called_with(Sql.insert(), [], None, None, None)


def test_model_save__fetch(patch, model, table, db):
    patch.many(Model, ['as_dictionary', 'select'])
    patch.object(Sql, 'insert')
    Model.__table__ = table
    Model.__db__ = db
    result = model.save()
    Model.select().where.assert_called_with(id=db.cursor.fetch_returned())
    assert result == Model.select().where().one()


def test_model_update(patch):
    patch.object(Query, 'update')
    result = Model.update(col='value')
    Query.update.assert_called_with(Model.__db__, Model.__table__,
                                    {'col': 'value'})
    assert result == Model


def test_model_where(query):
    Model.__query__ = query
    result = Model.where(col='value')
    query.where.assert_called_with(col='value')
    assert result == Model


def test_model_join(query):
    Model.__query__ = query
    result = Model.join('table_name', 'on')
    Model.__db__.get_table.assert_called_with('table_name')
    query.join.assert_called_with(Model.__db__.get_table(), 'on', None)
    assert result == Model


def test_model_join__tuple(query):
    Model.__query__ = query
    Model.join('table_name', ('table', 'column'))
    Model.__db__.get_table.assert_called_with('table_name')
    query.join.assert_called_with(Model.__db__.get_table(),
                                  (Model.__db__.get_table(), 'column'), None)


def test_model_join__join_type(query):
    Model.__query__ = query
    Model.join('table_name', 'on', 'outer')
    query.join.assert_called_with(Model.__db__.get_table(), 'on', 'outer')


def test_model_select(patch):
    patch.object(Query, 'select')
    result = Model.select()
    Query.select.assert_called_with(Model.__db__, Model.__table__)
    assert Model.__query__ == Query.select()
    assert result == Model


def test_model_select__conditions(patch):
    patch.object(Query, 'select')
    Model.select(col='value')
    Query.select().where.assert_called_with(col='value')


def test_model_count(patch):
    patch.object(Query, 'count')
    result = Model.count()
    Query.count.assert_called_with(Model.__db__, Model.__table__)
    assert Model.__query__ == Query.count()
    assert result == Model


def test_model_count__conditions(patch):
    patch.object(Query, 'count')
    Model.count(col='value')
    Query.count().where.assert_called_with(col='value')


def test_model_get(patch):
    patch.object(Model, 'execute')
    result = Model.get()
    Model.execute.assert_called_with(fetch=True)
    assert result == Model.execute()


def test_model_get__no_query(patch):
    patch.many(Model, ['execute', 'select'])
    Model.__query__ = None
    Model.get()
    assert Model.select.call_count == 1


def test_model_dictionaries(patch):
    patch.object(Model, 'execute')
    result = Model.dictionaries()
    Model.execute.assert_called_with(fetch=True, mode='dictionaries')
    assert result == Model.execute()


def test_model_dictionaries__no_query(patch):
    patch.many(Model, ['execute', 'select'])
    Model.__query__ = None
    Model.dictionaries()
    assert Model.select.call_count == 1


def test_model_dictionary(patch):
    patch.object(Model, 'execute')
    result = Model.dictionary()
    Model.execute.assert_called_with(fetch='one', mode='dictionaries')
    assert result == Model.execute()


def test_model_dictionary__no_query(patch):
    patch.many(Model, ['execute', 'select'])
    Model.__query__ = None
    Model.dictionary()
    assert Model.select.call_count == 1


def test_model_one(patch):
    patch.object(Model, 'execute')
    result = Model.one()
    Model.execute.assert_called_with(fetch='one')
    assert result == Model.execute()


def test_model_one__no_query(patch):
    patch.many(Model, ['execute', 'select'])
    Model.__query__ = None
    Model.one()
    Model.select.call_count == 1


def test_model_order_by(query):
    Model.__query__ = query
    result = Model.order_by(col='asc')
    query.order_by.assert_called_with(col='asc')
    assert result == Model


def test_model_random(query):
    Model.__query__ = query
    result = Model.random()
    assert query.random.call_count == 1
    assert result == Model


def test_model_limit(query):
    Model.__query__ = query
    result = Model.limit(1)
    query.limit.assert_called_with(1, None)
    assert result == Model


def test_model_limit__offset(query):
    Model.__query__ = query
    Model.limit(1, 2)
    query.limit.assert_called_with(1, 2)


def test_model_paginate(patch):
    patch.object(Model, 'limit')
    result = Model.paginate(2, 50)
    Model.limit.assert_called_with(50, offset=100)
    assert result == Model.limit()


def test_model_delete(patch):
    patch.object(Query, 'delete')
    result = Model.delete()
    Query.delete.assert_called_with(Model.__db__, Model.__table__)
    assert Model.__query__ == Query.delete()
    assert result == Model


def test_model_delete__conditions(patch):
    patch.object(Query, 'delete')
    Model.delete(col='value')
    Query.delete().where.assert_called_with(col='value')


def test_model_truncate(patch):
    patch.object(Query, 'truncate')
    patch.object(Model, 'execute')
    result = Model.truncate()
    Query.truncate.assert_called_with(Model.__db__, Model.__table__, None)
    assert Model.__query__ == Query.truncate()
    assert result == Model.execute()


def test_model_truncate__cascade(patch):
    patch.object(Query, 'truncate')
    patch.object(Model, 'execute')
    Model.truncate(cascade=True)
    Query.truncate.assert_called_with(Model.__db__, Model.__table__, True)


def test_model_drop(patch):
    patch.object(Query, 'drop')
    patch.object(Model, 'execute')
    result = Model.drop()
    Query.drop.assert_called_with(Model.__db__, Model.__table__, None)
    assert Model.__query__ == Query.drop()
    assert result == Model.execute()


def test_model_drop__cascade(patch):
    patch.object(Query, 'drop')
    patch.object(Model, 'execute')
    Model.drop(cascade=True)
    Query.drop.assert_called_with(Model.__db__, Model.__table__, True)


def test_model_sql(query):
    Model.__query__ = query
    assert Model.sql() == query.sql()


def test_model_eq(patch, model):
    assert model == Model(col='value')


@mark.skip
def test_model_eq__not(patch):
    # NOTE(vesuvium): in order to work, this test would require creating a
    # class that inherits Model. That would no longer be a unit test, so
    # this feature is tested in only in integration.
    assert Model(id=1) == Model(id=2)


def test_model_repr(model):
    model.__table__.name = 'table'
    assert str(model) == '<Table(id)>'
