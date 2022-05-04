# -*- coding: utf-8 -*-
import os

from axonclient.client import DEFAULT_LOCAL_AXONSERVER_URI
from eventsourcing.domain import Aggregate
from eventsourcing.tests.application import TIMEIT_FACTOR, ExampleApplicationTestCase


class TestApplicationWithAxonServer(ExampleApplicationTestCase):
    timeit_number = 30 * TIMEIT_FACTOR
    expected_factory_topic = "eventsourcing_axonserver.factory:Factory"

    def setUp(self) -> None:
        self.original_initial_version = Aggregate.INITIAL_VERSION
        Aggregate.INITIAL_VERSION = 0
        super().setUp()
        os.environ["PERSISTENCE_MODULE"] = "eventsourcing_axonserver"
        os.environ["AXONSERVER_URI"] = DEFAULT_LOCAL_AXONSERVER_URI

    def tearDown(self) -> None:
        Aggregate.INITIAL_VERSION = self.original_initial_version
        del os.environ["PERSISTENCE_MODULE"]
        del os.environ["AXONSERVER_URI"]
        super().tearDown()

    def test_example_application(self) -> None:
        super().test_example_application()


del ExampleApplicationTestCase
