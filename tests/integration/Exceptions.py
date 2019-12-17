# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
from psyker import Psyker
from psyker.exceptions import ConnectionError

from pytest import raises


def test_connection_error():
    psyker = Psyker()
    with raises(ConnectionError):
        psyker.start('postgres://hello:world@localhost:5432/psyker')
