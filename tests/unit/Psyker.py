# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
from psyker import Db, ModelFactory, Psyker

from pytest import fixture


@fixture
def db(magic):
    return magic()


@fixture
def psyker(db):
    psyker = Psyker()
    psyker.db = db
    return psyker


def test_psyker_init():
    psyker = Psyker()
    assert psyker.db is None
    assert psyker.models == {}


def test_psyker_setup_models(magic):
    model = magic()
    Psyker.setup_models('db', [model])
    model.setup.assert_called_with('db', 't0')


def test_psyker_add_models(magic, psyker):
    model = magic(__name__='')
    psyker.add_models(model)
    assert psyker.models == {model.__name__: model}


def test_psyker_make_model(patch, psyker):
    patch.object(ModelFactory, 'make')
    patch.many(Psyker, ['add_models', 'create_tables'])
    result = psyker.make_model('name', 'fields')
    Psyker.add_models.assert_called_with(result)
    result.setup.assert_called_with(psyker.db, len(psyker.models))
    assert psyker.db.cursor.models == psyker.models
    assert Psyker.create_tables.call_count == 1
    assert result == ModelFactory.make()


def test_psyker_create_tables(magic, psyker):
    psyker.models = {'model': magic()}
    psyker.create_tables()
    assert psyker.models['model'].create_table.call_count == 1


def test_psyker_start(patch, psyker):
    patch.init(Db)
    patch.object(Db, 'start')
    patch.many(Psyker, ['create_tables', 'setup_models'])
    psyker.start('url')
    Db.__init__.assert_called_with('url', psyker.models)
    assert isinstance(psyker.db, Db)
    assert Db.start.call_count == 1
    Psyker.setup_models.assert_called_with(psyker.db,
                                           list(psyker.models.values()))
    assert Psyker.create_tables.call_count == 1


def test_psyker_close(psyker, db):
    psyker.close()
    assert db.close.call_count == 1
