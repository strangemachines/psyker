# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
from .Db import Db


class Psyker:

    __slots__ = ('db', 'models')

    def __init__(self):
        self.db = None
        self.models = {}

    @classmethod
    def setup_models(cls, db, models):
        for index in range(0, len(models)):
            model = models[index]
            model.setup(db, f't{index}')

    def add_models(self, *models):
        new_models = {model.__name__.lower(): model for model in models}
        self.models = {**self.models, **new_models}

    def create_tables(self):
        for model in self.models.values():
            model.create_table()

    def start(self, url):
        self.db = Db(url, self.models)
        self.db.connect()
        self.setup_models(self.db, list(self.models.values()))
        self.create_tables()

    def close(self):
        self.db.close()