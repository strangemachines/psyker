# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
from psyker.exceptions import FieldTypeError


def test_fieldtypeerror_init():
    assert FieldTypeError('type').field_type == 'type'


def test_fieldtypeerror_str():
    result = str(FieldTypeError('type'))
    assert result == 'Field type error: type is invalid or not supported'
