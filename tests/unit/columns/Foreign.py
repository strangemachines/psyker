# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
from psyker.columns import Column, Foreign

from pytest import mark


def test_foreign():
    assert issubclass(Foreign, Column)


def test_foreign_init(patch):
    patch.init(Column)
    Foreign('name', 'foreign_table')
    kwargs = {'nullable': True, 'unique': False, 'reference': 'foreign_table',
              'reference_column': 'id'}
    Column.__init__.assert_called_with('name', 'foreign', **kwargs)


def test_foreign_init__column(patch):
    patch.init(Column)
    Foreign('name', 'foreign_table', 'foreign_key')
    kwargs = {'nullable': True, 'unique': False, 'reference': 'foreign_table',
              'reference_column': 'foreign_key'}
    Column.__init__.assert_called_with('name', 'foreign', **kwargs)


@mark.parametrize('kwarg', ['nullable', 'unique'])
def test_foreign_init__nullable(patch, kwarg):
    patch.init(Column)
    Foreign('name', 'foreign_table', **{kwarg: 'yes'})
    kwargs = {'nullable': True, 'unique': False, 'reference': 'foreign_table',
              'reference_column': 'id'}
    kwargs[kwarg] = 'yes'
    Column.__init__.assert_called_with('name', 'foreign', **kwargs)
