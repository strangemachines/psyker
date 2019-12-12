# -*- coding: utf-8 -*-
import os
import time

from pony.orm import Database, PrimaryKey, Required, db_session, select


count = int(os.environ.get('rounds', '10000'))
default_url = 'postgres://postgres:postgres@localhost:5432/psyker'
url = os.environ.get('db', default_url)


db = Database()


class Users(db.Entity):
    id = PrimaryKey(str)
    username = Required(str)


db.bind(provider='postgres', user='postgres', password='postgres',
        host='localhost', database='psyker')
db.generate_mapping(create_tables=True)


start = now = time.time()
for i in range(count):
    with db_session:
        list(select(u for u in Users))
now = time.time()

print(f'Pony: Rows/sec: {count / (now - start): 10.2f}')
