# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
from .Db import Db
from .ModelFactory import ModelFactory


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

    def make_model(self, name, fields):
        """
        Creates a model using ModelFactory so that is ready for querying.
        """
        model = ModelFactory.make(name, fields)
        model.setup(self.db, len(self.models))
        self.add_models(model)
        self.db.cursor.models = self.models
        self.create_tables()
        return model

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
