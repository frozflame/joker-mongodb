joker-mongodb
=============

Access mongodb with handy utilities and fun.

## Connnect to multiple mongo servers with MongoInterface

Example:

`GlobalInterface` is defined in `example/environ.py` as:

```python
from functools import cached_property

import volkanic
from joker.mongodb.interfaces import MongoInterface


class GlobalInterface(volkanic.GlobalInterface):
    package_name = 'example'
    default_config = {
        "mongoi": {
            "local": {},
            "remote": {
                "host": "192.168.22.122",
                "port": 27017
            }
        }
    }

    @cached_property
    def mongoi(self) -> MongoInterface:
        return MongoInterface.from_config(self.conf['mongoi'])
```

If a configuration file is found at one of the follow locations:

- Under your project directory in a development enviornment
- `~/.example/config.json5`
- `/etc/example/config.json5`
- `/example/config.json5`

it will override `GlobalInterface.default_config`.

Usage in code `example/application.py`:

```python
from bson import ObjectId
# noinspection PyUnresolvedReferences,PyPackageRequirements
from example.environ import GlobalInterface

gi = GlobalInterface()


def get_product(product_oid):
    coll = gi.mongoi.get_coll('remote', 'example', 'products')
    return coll.find_one({'_id': ObjectId(product_oid)})


if __name__ == '__main__':
    print(get_product('60f231605e0a4ea3c6c31c13'))
```

recent changes
--------------

version 0.1.4

* add `MongoDocumentSchemator` and `MongoFieldSchemator`

version 0.1.3

* add `TransactionHelper`

