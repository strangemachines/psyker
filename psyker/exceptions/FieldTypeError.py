# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-


class FieldTypeError(ValueError):
    __slots__ = ('field_type', )

    def __init__(self, field_type):
        self.field_type = field_type

    def __str__(self):
        return (f'Field type error: {self.field_type} is invalid or not '
                'supported')
