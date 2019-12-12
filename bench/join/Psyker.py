# -*- coding: utf-8 -*-
import os
import time

from psyker import Column, Model, Psyker


count = int(os.environ.get('rounds', '10000'))
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

start = now = time.time()
for i in range(count):
    Todos.select().join('users', 'user').get()
now = time.time()

print(f'Psyker, queries/s: {count / (now - start): 10.2f}')
db.close()
