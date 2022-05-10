# Event Sourcing with Axon Server

This package supports using the Python
[eventsourcing](https://github.com/pyeventsourcing/eventsourcing) library
with [Axon Server](https://developer.axoniq.io/axon-server).

## Installation

Use pip to install the stable distribution from the Python Package Index.

    $ pip install eventsourcing-axonserver

Please note, it is recommended to install Python packages into a Python virtual environment.

## Getting started

Define aggregates and applications in the usual way. Please note, aggregate
sequences  in Axon Server are expected to start from position `0`, whereas
the default for the library's `Aggregate` class is to start from `1`. So we
need to set the `INITIAL_VERSION` attribute on the aggregate class to `0`.

```python
from typing import Any, Dict
from uuid import UUID

from eventsourcing.application import Application
from eventsourcing.domain import Aggregate, event


class TrainingSchool(Application):
    def register(self, name: str) -> UUID:
        dog = Dog(name)
        self.save(dog)
        return dog.id

    def add_trick(self, dog_id: UUID, trick: str) -> None:
        dog = self.repository.get(dog_id)
        dog.add_trick(trick)
        self.save(dog)

    def get_dog(self, dog_id) -> Dict[str, Any]:
        dog = self.repository.get(dog_id)
        return {'name': dog.name, 'tricks': tuple(dog.tricks)}


class Dog(Aggregate):
    INITIAL_VERSION = 0

    @event('Registered')
    def __init__(self, name: str) -> None:
        self.name = name
        self.tricks = []

    @event('TrickAdded')
    def add_trick(self, trick: str) -> None:
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

The application's methods may be then called, from tests and
user interfaces.

```python
# Register dog.
dog_id = school.register('Fido')

# Add tricks.
school.add_trick(dog_id, 'roll over')
school.add_trick(dog_id, 'play dead')

# Get details.
dog = school.get_dog(dog_id)
assert dog["name"] == 'Fido'
assert dog["tricks"] == ('roll over', 'play dead')
```

For more information, please refer to the Python
[eventsourcing](https://github.com/johnbywater/eventsourcing) library
and the [Axon Server](https://developer.axoniq.io/axon-server) project.

## Developers

Clone the [project repository](https://github.com/johnbywater/eventsourcing),
set up a virtual environment, and install dependencies.

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
