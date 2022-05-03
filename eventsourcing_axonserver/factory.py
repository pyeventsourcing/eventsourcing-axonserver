# -*- coding: utf-8 -*-
from axonclient.client import AxonClient
from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    InfrastructureFactory,
    ProcessRecorder,
)
from eventsourcing.utils import Environment

from eventsourcing_axonserver.recorders import (
    AxonServerAggregateRecorder,
    AxonServerApplicationRecorder,
)


class Factory(InfrastructureFactory):
    """
    Infrastructure factory for Axon infrastructure.
    """

    AXON_SERVER_URI = "AXON_SERVER_URI"

    def __init__(self, env: Environment):
        super().__init__(env)
        axon_server_uri = self.env.get(self.AXON_SERVER_URI)
        if axon_server_uri is None:
            raise EnvironmentError(
                f"'{self.AXON_SERVER_URI}' not found "
                "in environment with keys: "
                f"'{', '.join(self.env.create_keys(self.AXON_SERVER_URI))}'"
            )
        self.axon_client = AxonClient(uri=axon_server_uri)

    def aggregate_recorder(self, purpose: str = "events") -> AggregateRecorder:
        return AxonServerAggregateRecorder(
            axon_client=self.axon_client, for_snapshotting=bool(purpose == "snapshots")
        )

    def application_recorder(self) -> ApplicationRecorder:
        return AxonServerApplicationRecorder(self.axon_client)

    def process_recorder(self) -> ProcessRecorder:
        raise NotImplementedError()

    def __del__(self) -> None:
        self.axon_client.close_connection()
