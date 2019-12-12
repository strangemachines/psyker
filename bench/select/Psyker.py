# -*- coding: utf-8 -*-
import os
import time

from psyker import Model, Psyker


count = int(os.environ.get('rounds', '10000'))
default_url = 'postgres://postgres:postgres@localhost:5432/psyker'
url = os.environ.get('db', default_url)


class Users(Model):

    @classmethod
    def columns(cls):
        return {'username': 'str'}


db = Psyker()
db.add_models(Users)
db.start(url)

start = now = time.time()
for i in range(count):
    Users.get()
now = time.time()

print(f'Psyker, queries/s: {count / (now - start): 10.2f}')
db.close()
