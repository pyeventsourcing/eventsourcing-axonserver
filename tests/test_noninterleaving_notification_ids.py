# -*- coding: utf-8 -*-
from unittest import skip

from axonclient.client import DEFAULT_LOCAL_AXONSERVER_URI, AxonClient
from eventsourcing.persistence import ApplicationRecorder
from eventsourcing.tests.persistence import NonInterleavingNotificationIDsBaseCase

from eventsourcing_axonserver.recorders import AxonServerApplicationRecorder


@skip("This is still a bit flakey - not sure why")
class TestNonInterleaving(NonInterleavingNotificationIDsBaseCase):
    insert_num = 1000

    def setUp(self) -> None:
        super().setUp()
        self.axon_client = AxonClient(DEFAULT_LOCAL_AXONSERVER_URI)

    def tearDown(self) -> None:
        self.axon_client.close_connection()

    def create_recorder(self) -> ApplicationRecorder:
        return AxonServerApplicationRecorder(axon_client=self.axon_client)


del NonInterleavingNotificationIDsBaseCase
