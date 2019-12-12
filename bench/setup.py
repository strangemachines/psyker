# -*- coding: utf-8 -*-
import os

from psyker import Column, Model, Psyker


default_url = 'postgres://postgres:postgres@localhost:5432/psyker'
url = os.environ.get('db', default_url)


class Users(Model):

    @classmethod
    def columns(cls):
        return {'username': 'str'}


class Todos(Model):

    @classmethod
    def columns(cls):
        return {
            'title': 'str',
            'done': 'bool',
            'user': Column('user', 'foreign', reference='users',
                           reference_column='id')
        }


db = Psyker()
db.add_models(Users, Todos)
db.start(url)
Todos.drop(cascade=True)
Users.drop(cascade=True)
Users.create_table()
Todos.create_table()
eldrad = Users(username='Eldrad').save()
Users(username='Taldeer').save()
Todos(title='Meditate', done=False, user=eldrad.id).save()
db.close()
