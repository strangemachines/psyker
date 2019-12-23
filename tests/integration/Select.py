# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
from pytest import fixture


@fixture(scope='module')
def pine(request, psyker, trees):
    tree = trees(name='pine', max_height=80).save()

    def f():
        tree.delete().execute()
    request.addfinalizer(f)
    return tree


@fixture(scope='module')
def oak(request, psyker, trees):
    tree = trees(name='oak', max_height=40).save()

    def f():
        tree.delete().execute()
    return tree


def test_psyker_select(trees, pine):
    assert type(trees.select().get()) == list


def test_psyker_select__conditions(trees, pine):
    assert len(trees.select(name='pine').get()) == 1


def test_psyker_select__where(trees, pine):
    assert len(trees.select().where(name='pine').get()) == 1


def test_psyker_select__where_gt(trees, pine, oak):
    trees = trees.select().where(max_height='>40').get()
    assert trees[0].max_height > 40


def test_psyker_select_where__tuple(trees, pine, oak):
    trees = trees.select().where(max_height=('>', 40)).get()
    assert trees[0].max_height > 40


def test_psyker_select_where__tuple_two_chars(trees, pine, oak):
    trees = trees.select().where(max_height=('>=', 40)).get()
    assert trees[0].max_height > 40


def test_psyker_select__limit(trees, pine, oak):
    assert len(trees.select().limit(1).get()) == 1


def test_psyker_select__order_by(trees, pine, oak):
    result = trees.select().order_by(max_height='asc').get()
    assert result[0].max_height == 40
    assert result[1].max_height == 80


def test_psyker_select__random(trees, pine, oak):
    assert len(trees.select().random().get()) == 2


def test_psyker_select_one(psyker, trees):
    assert isinstance(trees.select().one(), trees)


def test_psyker_select_one__none(psyker, trees):
    assert trees.select(name='walnut').one() is None


def test_psyker_select_dictionaries(psyker, trees):
    result = trees.dictionaries()
    assert type(result[0]) == dict


def test_psyker_select_dictionary(psyker, trees):
    result = trees.dictionary()
    assert type(result) == dict


def test_psyker_select_dictionary__none(psyker, trees):
    assert trees.select(name='walnut').dictionary() is None


def test_psyker_select_comparison(psyker, trees):
    pine = trees.select(name='pine').one()
    same_pine = trees.select(name='pine').one()
    assert pine == same_pine


def test_psyker_select_comparison__false(psyker, trees):
    pine = trees.select(name='pine').one()
    oak = trees.select(name='oak').one()
    assert (pine == oak) is False
