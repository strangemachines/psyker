# -*- coding: utf-8 -*-
import os
import time

from peewee import BooleanField, CharField, ForeignKeyField, Model

from playhouse.db_url import connect


count = int(os.environ.get('rounds', '10000'))
default_url = 'postgres://postgres:postgres@localhost:5432/psyker'
url = os.environ.get('db', default_url)


db = connect(url)


class Users(Model):
    class Meta:
        database = db

    id = CharField(primary_key=True)
    username = CharField()


class Todos(Model):
    class Meta:
        database = db

    id = CharField(primary_key=True)
    done = BooleanField()
    user = ForeignKeyField(Users)

# NOTE(vesuvium): peewee is silly and demands Todos to have users_id column
# to make the join, even with an explicit on parameter.
start = now = time.time()
for i in range(count):
    Todos.select().join(Users, on=(Todos.user == Users.id)).get()
now = time.time()

print(f'Psyker, A: Rows/sec: {count / (now - start): 10.2f}')
db.close()
