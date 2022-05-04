# -*- coding: utf-8 -*-
from typing import Any, List, Optional, Sequence
from uuid import UUID, uuid4

from axonclient.client import AxonClient, AxonEvent
from axonclient.exceptions import OutOfRangeError
from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    IntegrityError,
    Notification,
    OperationalError,
    StoredEvent,
)
from google.protobuf.internal.wire_format import INT32_MAX


class AxonServerAggregateRecorder(AggregateRecorder):
    def __init__(
        self,
        axon_client: AxonClient,
        for_snapshotting: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super(AxonServerAggregateRecorder, self).__init__(*args, **kwargs)
        self.axon_client = axon_client
        self.for_snapshotting = for_snapshotting

    def insert_events(
        self, stored_events: List[StoredEvent], **kwargs: Any
    ) -> Optional[Sequence[int]]:
        self._insert_events(stored_events, **kwargs)
        return None

    def _insert_events(
        self, stored_events: List[StoredEvent], **kwargs: Any
    ) -> Optional[Sequence[int]]:
        axon_events: List[AxonEvent] = []
        # Todo: Need to get the IDs in the response. But only get a Confirmation msg.
        next_notification_id = self.max_notification_id() + 1
        for stored_event in stored_events:
            message_identifier = str(uuid4())
            axon_event = AxonEvent(
                message_identifier=message_identifier,
                aggregate_identifier=str(stored_event.originator_id),
                aggregate_sequence_number=stored_event.originator_version,
                aggregate_type="AggregateRoot",
                timestamp=0,
                payload_type=stored_event.topic,
                payload_revision="1",
                payload_data=stored_event.state,
                snapshot=self.for_snapshotting,
                meta_data={},
            )
            axon_events.append(axon_event)
        try:
            if self.for_snapshotting:
                self.axon_client.append_snapshot(axon_events[0])
            else:
                self.axon_client.append_event(axon_events)
        except AssertionError as e:
            # In case confirmation.success is False.
            raise OperationalError(e) from e
        except OutOfRangeError as e:
            raise IntegrityError(e) from e
        return list(
            range(next_notification_id, next_notification_id + len(axon_events))
        )

    def select_events(
        self,
        originator_id: UUID,
        gt: Optional[int] = None,
        lte: Optional[int] = None,
        desc: bool = False,
        limit: Optional[int] = None,
    ) -> List[StoredEvent]:
        if self.for_snapshotting and desc and limit == 1:
            return []
        if gt is not None:
            initial_sequence_number = gt + 1
        else:
            initial_sequence_number = 0

        if self.for_snapshotting:
            axon_events = self.axon_client.list_snapshot_events(
                aggregate_id=str(originator_id),
                initial_sequence=initial_sequence_number,
            )
            # Axon returns snapshots in reversed order.
            axon_events.reverse()
        else:
            axon_events = self.axon_client.list_aggregate_events(
                aggregate_id=str(originator_id),
                initial_sequence=initial_sequence_number,
                allow_snapshots=True,
            )

        if lte is not None:
            axon_events = [
                ae for ae in axon_events if ae.aggregate_sequence_number <= lte
            ]

        if desc:
            axon_events.reverse()

        if limit:
            axon_events = axon_events[:limit]

        return [
            StoredEvent(
                originator_id=UUID(ae.aggregate_identifier),
                originator_version=ae.aggregate_sequence_number,
                topic=ae.payload_type,
                state=ae.payload_data,
            )
            for ae in axon_events
        ]

    def max_notification_id(self) -> int:
        return self.axon_client.get_last_token()


class AxonServerApplicationRecorder(AxonServerAggregateRecorder, ApplicationRecorder):
    def insert_events(
        self, stored_events: List[StoredEvent], **kwargs: Any
    ) -> Optional[Sequence[int]]:
        return self._insert_events(stored_events, **kwargs)

    def select_notifications(
        self,
        start: int,
        limit: int,
        stop: Optional[int] = None,
        topics: Sequence[str] = (),
    ) -> List[Notification]:
        generator = self.axon_client.iter_events(
            tracking_token=start, number_of_permits=INT32_MAX
        )
        notifications: List[Notification] = []
        count = 0
        for tracking_token, axon_event in generator:
            if count == limit:
                # print("Breaking out of for loop: limit reached")
                break
            if topics and axon_event.payload_type not in topics:
                continue
            # print("Tracking token:", tracking_token, "Axon event:", axon_event)
            notification = Notification(
                id=tracking_token,
                originator_id=UUID(axon_event.aggregate_identifier),
                originator_version=axon_event.aggregate_sequence_number,
                topic=axon_event.payload_type,
                state=axon_event.payload_data,
            )
            notifications.append(notification)
            count += 1

            if stop is not None and stop <= tracking_token:
                break

        return notifications
