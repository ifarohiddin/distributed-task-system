import unittest
from unittest.mock import patch

import pika

from src import producer
from src.config import DEAD_LETTER_EXCHANGE, EXCHANGE_NAME, TASK_QUEUES
from tests.helpers import decode, fake_connection


class TaskEncodingTests(unittest.TestCase):
    def test_build_task_has_expected_fields(self):
        task = producer.build_task("resize", {"image": "a.jpg"}, priority=8)
        self.assertEqual((task["type"], task["payload"], task["priority"], task["retry_count"]),
                         ("resize", {"image": "a.jpg"}, 8, 0))
        self.assertEqual(len(task["task_id"]), 36)

    def test_task_ids_are_unique(self):
        self.assertEqual(len({producer.build_task("x", {})["task_id"] for _ in range(20)}), 20)

    def test_encode_round_trip(self):
        task = producer.build_task("resize", {"image": "a.jpg"})
        self.assertEqual(decode(producer.encode_task(task)), task)


class ProducerTests(unittest.TestCase):
    def setUp(self):
        self.connection, self.channel = fake_connection()
        patcher = patch("src.rabbit.connect", return_value=self.connection)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_declares_exchanges_and_priority_queues_with_dead_letter_exchange(self):
        producer.Producer()

        exchanges = {c.kwargs["exchange"]: c.kwargs["exchange_type"] for c in self.channel.exchange_declare.call_args_list}
        self.assertEqual(exchanges, {EXCHANGE_NAME: "direct", DEAD_LETTER_EXCHANGE: "fanout"})
        self.channel.confirm_delivery.assert_called()
        declared = {c.kwargs["queue"]: c.kwargs for c in self.channel.queue_declare.call_args_list}
        self.assertEqual(set(declared), set(TASK_QUEUES))
        for kwargs in declared.values():
            self.assertTrue(kwargs["durable"])
            self.assertEqual(kwargs["arguments"]["x-dead-letter-exchange"], DEAD_LETTER_EXCHANGE)
            self.assertEqual(kwargs["arguments"]["x-max-priority"], 10)

    def test_submit_task_publishes_persistent_prioritised_message(self):
        task_id = producer.Producer().submit_task("image_processing", "resize", {"image": "a.jpg"}, priority=8)

        kwargs = self.channel.basic_publish.call_args.kwargs
        self.assertEqual(kwargs["exchange"], EXCHANGE_NAME)
        self.assertEqual(kwargs["routing_key"], "image_processing")
        self.assertEqual(kwargs["properties"].delivery_mode, 2)
        self.assertEqual(kwargs["properties"].priority, 8)
        message = decode(kwargs["body"])
        self.assertEqual((message["task_id"], message["type"], message["retry_count"]), (task_id, "resize", 0))

    def test_unknown_queue_is_rejected_before_publishing(self):
        with self.assertRaises(ValueError):
            producer.Producer().submit_task("yoq_navbat", "resize", {})
        self.channel.basic_publish.assert_not_called()

    def test_publish_errors_are_propagated(self):
        self.channel.basic_publish.side_effect = pika.exceptions.UnroutableError([])
        with self.assertRaises(pika.exceptions.UnroutableError):
            producer.Producer().submit_task("image_processing", "resize", {})

    def test_reopens_channel_when_broker_rejects_queue_arguments(self):
        self.channel.queue_declare.side_effect = [pika.exceptions.ChannelClosedByBroker(406, "PRECONDITION_FAILED"), None]
        producer.Producer()
        self.assertGreaterEqual(self.connection.channel.call_count, 2)

    def test_close_closes_connection(self):
        producer.Producer().close()
        self.connection.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
