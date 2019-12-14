# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
from .Column import Column


class Foreign(Column):
    """
    Provides a simpler interface for foreign columns.
    """

    def __init__(self, name, reference, column='id', nullable=True,
                 unique=False):
        super().__init__(name, 'foreign', nullable=nullable, unique=unique,
                         reference=reference, reference_column=column)
