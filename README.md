joker-mongodb
=============

Example:

`GlobalInterface` is defined in `example/environ.py` as:

```python
import volkanic
from joker.mongodb.environ import GIMixinMongo


class GlobalInterface(volkanic.GlobalInterface, GIMixinMongo):
    package_name = 'example'
```

Configuration file `config.json5`:

```json5
{
    "mongoi": {
        "local": {},
        "remote": {
            "host": "192.168.22.122",
            // default mongo port is 27017
            "port": 27018
        }
    }
}
```

This `config.json5` is at one of the follow locations:
- Under your project directory in a development enviornment
- `~/.example/config.json5`
- `/etc/example/config.json5`
- `/example/config.json5`

Usage in code `example/application.py`:

```python
from bson import ObjectId
from example.environ import GlobalInterface  # noqa

gi = GlobalInterface()


def get_product(product_oid):
    coll = gi.mongoi.get_coll('remote', 'example', 'products')
    return coll.find_one({'_id': ObjectId(product_oid)})
    


if __name__ == '__main__':
    print(get_product('60f231605e0a4ea3c6c31c13'))

```
