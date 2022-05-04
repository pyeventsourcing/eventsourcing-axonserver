# Event Sourcing with Axon Server

This package supports using the Python
[eventsourcing](https://github.com/pyeventsourcing/eventsourcing) library
with Axon Server.

## Installation

Use pip to install the stable distribution from the Python Package Index.

    $ pip install eventsourcing-axonserver

Please note, it is recommended to install Python packages into a Python virtual environment.

## Getting started

Define aggregates and applications in the usual way.

```python
from eventsourcing.application import Application
from eventsourcing.domain import Aggregate, event
from uuid import uuid5, NAMESPACE_URL


class TrainingSchool(Application):
    def register(self, name):
        dog = Dog(name)
        self.save(dog)

    def add_trick(self, name, trick):
        dog = self.repository.get(Dog.create_id(name))
        dog.add_trick(trick)
        self.save(dog)

    def get_tricks(self, name):
        dog = self.repository.get(Dog.create_id(name))
        return dog.tricks


class Dog(Aggregate):
    @event('Registered')
    def __init__(self, name):
        self.name = name
        self.tricks = []

    @staticmethod
    def create_id(name):
        return uuid5(NAMESPACE_URL, f'/dogs/{name}')

    @event('TrickAdded')
    def add_trick(self, trick):
        self.tricks.append(trick)
```

Configure the application to use Axon Server. Set environment variable
`PERSISTENCE_MODULE` to `'eventsourcing_axonserver'`, and set
`AXONSERVER_URI` to the host and port of your Axon Server.

```python
school = TrainingSchool(env={
    "PERSISTENCE_MODULE": "eventsourcing_axonserver",
    "AXONSERVER_URI": "localhost:8124",
})
```

The application's methods may be called from interfaces.

```python
school.register('Fido')
school.add_trick('Fido', 'roll over')
school.add_trick('Fido', 'play dead')
tricks = school.get_tricks('Fido')
assert tricks == ['roll over', 'play dead']
```

For more information, please refer to the Python
[eventsourcing](https://github.com/johnbywater/eventsourcing) library
and the [Axon Server](https://developer.axoniq.io/axon-server/overview) project.

## Developers

Clone the eventsourcing-axonserver repository, set up a virtual
environment, and install dependencies.

Use your IDE (e.g. PyCharm) to open the project repository. Create a
Poetry virtual environment, and then update packages.

    $ make update-packages

Alternatively, use the ``make install`` command to create a dedicated
Python virtual environment for this project.

    $ make install

Start Axon Server.

    $ make start-axon-server

Run tests.

    $ make test

Add tests in `./tests`. Add code in `./eventsourcing_axonserver`.

Check the formatting of the code.

    $ make lint

Reformat the code.

    $ make fmt

Add dependencies in `pyproject.toml` and then update installed packages.

    $ make update-packages

Stop Axon Server.

    $ make stop-axon-server
