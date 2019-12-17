# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
from psyker import Foreign, Model, Psyker

from pytest import fixture


@fixture(scope='module')
def trees():
    class Trees(Model):
        @classmethod
        def columns(cls):
            return {'name': 'str', 'max_height': 'int'}
    return Trees


@fixture(scope='module')
def fruits():
    class Fruits(Model):
        @classmethod
        def columns(cls):
            return {'name': 'str', 'tree': Foreign('tree', 'trees', 'id')}
    return Fruits


@fixture(scope='module')
def flies():
    class Flies(Model):
        @classmethod
        def columns(cls):
            return {'name': 'str', 'fruit': Foreign('fruit', 'fruits', 'id')}
    return Flies


@fixture(scope='module')
def psyker(trees, fruits, flies):
    psyker = Psyker()
    psyker.add_models(trees, fruits, flies)
    psyker.start('postgres://postgres:postgres@localhost:5432/psyker')
    return psyker


def test_psyker_insert(psyker, trees):
    pine = trees(name='pine', max_height=80).save()
    assert isinstance(pine, trees)
    assert pine.id is not None
    assert pine.name == 'pine'
    assert pine.max_height == 80


def test_psyker_insert__no_return(psyker, trees):
    oak = trees(name='oak', max_height=40).save(fetch=False)
    assert oak is None


def test_psyker_get(psyker, trees):
    result = trees.get()
    assert isinstance(result[0], trees)


def test_psyker_get__dicts(psyker, trees):
    result = trees.dictionaries()
    assert type(result[0]) == dict


def test_psyker_select(psyker, trees):
    assert type(trees.select().get()) == list


def test_psyker_select__conditions(psyker, trees):
    assert len(trees.select(name='pine').get()) == 1


def test_psyker_select__where(psyker, trees):
    assert len(trees.select().where(name='pine').get()) == 1


def test_psyker_select__where_gt(psyker, trees):
    trees = trees.select().where(max_height='>40').get()
    assert trees[0].max_height > 40


def test_psyker_select_where__tuple():
    assert 0


def test_psyker_select__limit(psyker, trees):
    assert len(trees.select().limit(1).get()) == 1


def test_psyker_select__order_by(psyker, trees):
    result = trees.select().order_by(max_height='asc').get()
    assert result[0].max_height == 40
    assert result[1].max_height == 80


def test_psyker_select__random(psyker, trees):
    assert len(trees.select().random().get()) == 2


def test_psyker_count(psyker, trees):
    assert trees.count().get() == 2


def test_psyker_count__conditions(psyker, trees):
    assert trees.count(name='pine').get() == 1


def test_psyker_one(psyker, trees):
    assert isinstance(trees.select().one(), trees)


def test_psyker_comparison(psyker, trees):
    pine = trees.select(name='pine').one()
    same_pine = trees.select(name='pine').one()
    assert pine == same_pine


def test_psyker_comparison__false(psyker, trees):
    pine = trees.select(name='pine').one()
    oak = trees.select(name='oak').one()
    assert (pine == oak) is False


def test_psyker_join(psyker, trees, fruits):
    pine = trees.select(name='pine').one()
    pinecone = fruits(name='pinecone', tree=pine.id).save()
    result = fruits.select().join('trees', 'tree').get()
    assert result[0] == pinecone
    assert result[0].trees[0] == pine


def test_psyker_join__three(psyker, trees, fruits, flies):
    pinecone = fruits.select(name='pinecone').one()
    pine = trees.select(name='pine').one()
    flies(name='common fly', fruit=pinecone.id).save()
    result = flies.select()\
        .join('fruits', 'fruit')\
        .join('trees', ('fruits', 'tree'))\
        .get()
    assert isinstance(result[0], flies)
    assert result[0].fruits[0] == pinecone
    assert result[0].fruits[0].trees[0] == pine


def test_psyker_update(psyker, trees):
    trees.update(max_height=50).execute()
    result = trees.get()
    assert result[0].max_height == 50
    assert result[1].max_height == 50


def test_psyker_delete(psyker, flies):
    flies.delete().execute()


def test_psyker_delete__conditions(psyker, trees):
    trees.delete(name='oak').execute()


def test_psyker_drop(trees, fruits, flies):
    flies.drop()
    fruits.drop()
    trees.drop()


def test_psyker_close(psyker):
    psyker.close()
