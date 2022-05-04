# -*- coding: utf-8 -*-
import os
from typing import Type

from axonclient.client import DEFAULT_LOCAL_AXONSERVER_URI
from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    InfrastructureFactory,
    ProcessRecorder,
)
from eventsourcing.tests.persistence import InfrastructureFactoryTestCase
from eventsourcing.utils import Environment

from eventsourcing_axonserver.factory import Factory
from eventsourcing_axonserver.recorders import (
    AxonServerAggregateRecorder,
    AxonServerApplicationRecorder,
)


class TestFactory(InfrastructureFactoryTestCase):
    def test_create_process_recorder(self) -> None:
        self.skipTest("Axon Server doesn't support tracking records")

    def expected_factory_class(self) -> Type[InfrastructureFactory]:
        return Factory

    def expected_aggregate_recorder_class(self) -> Type[AggregateRecorder]:
        return AxonServerAggregateRecorder

    def expected_application_recorder_class(self) -> Type[ApplicationRecorder]:
        return AxonServerApplicationRecorder

    def expected_process_recorder_class(self) -> Type[ProcessRecorder]:
        raise NotImplementedError()

    def setUp(self) -> None:
        self.env = Environment("TestCase")
        self.env[InfrastructureFactory.PERSISTENCE_MODULE] = Factory.__module__
        self.env[Factory.AXONSERVER_URI] = DEFAULT_LOCAL_AXONSERVER_URI
        super().setUp()

    def tearDown(self) -> None:
        if Factory.AXONSERVER_URI in os.environ:
            del os.environ[Factory.AXONSERVER_URI]
        super().tearDown()


del InfrastructureFactoryTestCase
