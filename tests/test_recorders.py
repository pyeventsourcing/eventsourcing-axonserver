# -*- coding: utf-8 -*-
from axonclient.client import DEFAULT_LOCAL_AXONSERVER_URI, AxonClient
from eventsourcing.persistence import AggregateRecorder, ApplicationRecorder
from eventsourcing.tests.persistence import (
    AggregateRecorderTestCase,
    ApplicationRecorderTestCase,
)

from eventsourcing_axonserver.recorders import (
    AxonServerAggregateRecorder,
    AxonServerApplicationRecorder,
)


class TestAxonServerAggregateRecorder(AggregateRecorderTestCase):
    INITIAL_VERSION = 0

    def setUp(self) -> None:
        self.axon_client = AxonClient(DEFAULT_LOCAL_AXONSERVER_URI)

    def create_recorder(self) -> AggregateRecorder:
        return AxonServerAggregateRecorder(axon_client=self.axon_client)

    def test_insert_and_select(self) -> None:
        super(TestAxonServerAggregateRecorder, self).test_insert_and_select()


class TestAxonServerApplicationRecorder(ApplicationRecorderTestCase):
    INITIAL_VERSION = 0

    def setUp(self) -> None:
        self.axon_client = AxonClient(DEFAULT_LOCAL_AXONSERVER_URI)

    def tearDown(self) -> None:
        self.axon_client.close_connection()

    def create_recorder(self) -> ApplicationRecorder:
        return AxonServerApplicationRecorder(axon_client=self.axon_client)

    def test_insert_select(self) -> None:
        super().test_insert_select()

    def test_concurrent_no_conflicts(self) -> None:
        super().test_concurrent_no_conflicts()


del AggregateRecorderTestCase
del ApplicationRecorderTestCase
