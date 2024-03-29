# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
from .Model import Model


class ModelFactory:
    """
    Provides an interface to create models dynamically.
    """

    @classmethod
    def make(cls, name, fields={}):
        return type(name, (Model, ), {'columns': lambda: fields})
