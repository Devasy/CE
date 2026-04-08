"""Migrations for 5.1.0 release."""

import os
import traceback

import amqp
from kombu import Connection, Exchange, Queue
from kombu.mixins import ConsumerProducerMixin
from kombu.utils.encoding import safe_repr

from netskope.common import RABBITMQ_QUEUE_NAME, RABBITMQ_QUORUM_QUEUE_NAME
from netskope.common.celery.task_priorities import TASK_PRIORITIES
from netskope.common.utils import Logger


logger = Logger()


class CustomConsumerProducerMixin(ConsumerProducerMixin):
    """Consumer and Producer mixin."""

    def __init__(self, connection, queue_name):
        """Init method for class."""
        self.old_queue_name = queue_name
        exchange = Exchange(queue_name, type="direct")
        params = {
            "name": queue_name,
            "exchange": exchange,
            "routing_key": queue_name,
        }
        if queue_name == RABBITMQ_QUEUE_NAME:
            params["queue_arguments"] = {"x-max-priority": 10}

        self.old_queue = Queue(**params)
        self.connection = connection
        self.channel = self.connection.channel()
        self.queue_length = self.get_queue_length()
        print(
            f"Migrating {self.queue_length} message(s) from '{queue_name}' queue."
        )
        self.published_queues = self.get_publish_queue()
        self.count = 0

    def get_queue_length(self):
        """Get queue length of celery queue."""
        try:
            queue = self.channel.queue_declare(
                self.old_queue_name, passive=True
            )
            return queue.message_count
        except amqp.exceptions.NotFound:
            return 0

    def get_publish_queue(self):
        """Create quorum queue."""
        mapping = {}
        for i in [3, 6, 9]:
            queue_name = RABBITMQ_QUORUM_QUEUE_NAME.format(i)
            exchange = Exchange(queue_name, type="direct", durable=True)
            exchange.declare(channel=self.channel)
            queue = Queue(
                name=queue_name,
                exchange=exchange,
                routing_key=queue_name,
                durable=True,
                queue_arguments={"x-queue-type": "quorum"},
            )
            queue.declare(channel=self.channel)
            queue.bind(self.channel)
            mapping[i] = queue
        return mapping

    def get_consumers(self, Consumer, channel):
        """Get consumers method which is used by self.run method."""
        return [
            Consumer(
                queues=[self.old_queue],
                callbacks=[self.handle_message],
                accept=["json", "pickle"],
                prefetch_count=5,
            )
        ]

    def convert_format(self, message, body):
        """Convert message format."""
        if message.headers.get("task", "None") in [
            # "cre.fetch_records",
            "cte.execute_plugin",
            "itsm.pull_alerts"
        ]:
            kwargs = body[1]
            if kwargs.get("alerts") is not None:
                kwargs["data"] = kwargs["alerts"]
            kwargs.pop("alerts", None)
            message.headers["kwargsrepr"] = safe_repr(body[1])
        if message.headers.get("task", "None") == "itsm.pull_alerts":
            message.headers["task"] = "itsm.pull_data_items"
        if message.headers.get("task", "None") == "itsm.delete_alerts":
            message.headers["task"] = "itsm.data_cleanup"
        if message.headers.get("task", "None") == "itsm.sync_alerts":
            message.headers["task"] = "itsm.sync_alerts_and_events"

        if message.headers.get("task", "None") == "cls.execute_plugin":
            kwargs = body[1]
            if kwargs.get("events") is not None:
                kwargs["data"] = kwargs["events"]
                kwargs["data_type"] = "events"
                kwargs["sub_type"] = kwargs["event_type"]
            elif kwargs.get("alerts") is not None:
                kwargs["data"] = kwargs.get("alerts")
                kwargs["data_type"] = "alerts"

            kwargs.pop("events", None)
            kwargs.pop("event_type", None)
            kwargs.pop("alerts", None)
            message.headers["kwargsrepr"] = safe_repr(body[1])
        # Addd migration for historical pulling task new arguments.
        if message.headers.get("task", "None") == "cls.execute_historical":
            kwargs = body[0]
            kwargs.insert(4, kwargs[3])
            message.headers["argsrepr"] = safe_repr(body[0])
        if message.headers.get("task", "None") == "cte.execute_plugin":
            kwargs = body[1]
            if kwargs.get("lock_field") is not None:
                kwargs["lock_field"] = "lockedAt.pull"
            message.headers["kwargsrepr"] = safe_repr(body[1])

    def handle_message(self, body, message):
        """Publish received messages from priority to quorum queue."""
        if message.properties.get("priority"):
            msg_priority = message.properties.get("priority")
            message.properties.pop("priority", None)
        else:
            msg_priority = TASK_PRIORITIES.get(
                message.headers.get("task", "None"), 3
            )
        current_queue = self.published_queues[msg_priority]

        if message.headers.get("task", "None") not in [
            "common.pull_alerts",
            "common.pull_events",
            "cls.webtx_logs",
            "cre.calculate_aggregate",
            "cre.evaluate_rules",
            "cre.fetch_scores",
            "cre.update_normalized_scores",
            "grc.get_application_details",
            "grc.execute_plugin",
            "grc.share_applications",
            "grc.unmute",
            "grc.historical_appdata"
        ]:
            self.convert_format(message, body)
            self.producer.publish(
                body,
                headers=message.headers,
                exchange=current_queue.exchange,
                routing_key=current_queue.routing_key,
            )

        message.ack()
        self.count += 1
        if self.count == self.queue_length:
            self.should_stop = True

    def run(self):
        """Start consuming message from celery queue."""
        if self.queue_length > 0:
            super().run()
        self.channel.queue_delete(self.old_queue_name)


def create_and_migrate_to_quorum_queues():
    """Create Quorum queues for HA."""
    try:
        print("Starting migration script for quorum queue migrations...")
        connection = Connection(os.environ["RABBITMQ_CONNECTION_STRING"])

        consumer = CustomConsumerProducerMixin(connection, "celery")
        consumer.run()

        consumer = CustomConsumerProducerMixin(connection, RABBITMQ_QUEUE_NAME)
        consumer.run()

        print("Completed migration script for quorum queue migrations.")
    except Exception:
        logger.error(
            "Error occurred while migrating messages to quorum queue.",
            details=traceback.format_exc(),
            error_code="CE_1131",
        )
    finally:
        connection.close()


if __name__ == "__main__":
    create_and_migrate_to_quorum_queues()
