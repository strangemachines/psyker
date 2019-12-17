# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
from psyker.Model import Model
from psyker.ModelFactory import ModelFactory


def test_modelfactory_make():
    result = ModelFactory.make('model', fields={'name': 'str'})
    assert issubclass(result, Model)
    assert result.columns() == {'name': 'str'}
