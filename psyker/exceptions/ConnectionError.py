# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http
# -*- coding: utf-8 -*-


class ConnectionError(ValueError):

    __slots__ = ('url', )

    def __init__(self, url):
        self.url = url

    def __str__(self):
        return (f'Connection to {self.url} failed. Check your credentials and'
                'try again.')
