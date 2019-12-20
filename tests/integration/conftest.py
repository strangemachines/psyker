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
