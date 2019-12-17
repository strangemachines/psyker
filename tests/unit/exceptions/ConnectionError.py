# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
from psyker.exceptions import ConnectionError


def test_connection_error_init():
    assert ConnectionError('url').url == 'url'


def test_connection_error_str():
    result = str(ConnectionError('url'))
    assert result == ('Connection to url failed. Check your credentials and'
                      'try again.')
