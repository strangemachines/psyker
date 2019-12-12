# Welcome to Psyker!

## Installation

```
pip install psyker
```

## Usage

### Bootstraping

```python
from psyker import Psyker


db = Psyker()
db.table('users', username='str', password='str', last_login='datetime')
db.start('psql://...', create_tables=True, check_models=False, timeout=30,
         pool_size=None, pool_timeout=None)
```

### Querying

```python
db.get('users') # select * from users limit 1
db.select('users').where(username='user').get() # select * from users
db.select('todo').where(owner=1).random().get() # select * from todos where owner = 1 order by RANDOM();

db.select('todo', 'users') # select * from todo, users where todo.owner = users.id;

db.select('todo').join('users').join(...).get()

db.update('users', username='new').where(id=1).save()
```

### Defining tables

```python
db.table('todos', content='str', done='bool', owner='Users')
```

Specifying column options:

```python
from psyker import Column


username = Column('username', 'str', nullable=True, unique=False)
db.table('users', username=username, password='str', last_login='datetime')
```


### Using models


```python
from psyker import Model


class Users(Model):
    username = 'str'
    password = 'str'
    last_login = 'datetime'

db.add(Users)

Users.get(...)
Users.join('todos')

user = Users.get(username='user')
user.update(username='new').save()
```
