import json
import unittest
import zlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.client import TaskClient
from src.config import DEAD_LETTER_QUEUE, EXCHANGE_NAME
from tests.helpers import decode, delivery, encode, fake_connection


def dead_letter(task, source_queue="image_processing"):
    """Broker o'lik xabarga `x-death` sarlavhasini qo'shadi."""
    headers = {"x-death": [{"queue": source_queue, "reason": "rejected", "count": 1}]} if source_queue else {}
    return SimpleNamespace(headers=headers)


class ClientTests(unittest.TestCase):
    def setUp(self):
        self.connection, self.channel = fake_connection()
        patcher = patch("src.rabbit.connect", return_value=self.connection)
        self.connect = patcher.start()
        self.addCleanup(patcher.stop)
        self.client = TaskClient()

    def test_submit_task_returns_id_and_publishes(self):
        task_id = self.client.submit_task("image_processing", "resize", {"image": "a.jpg"}, priority=8)

        kwargs = self.channel.basic_publish.call_args.kwargs
        self.assertEqual((kwargs["exchange"], kwargs["routing_key"]), (EXCHANGE_NAME, "image_processing"))
        self.assertEqual(decode(kwargs["body"])["task_id"], task_id)
        self.assertEqual(kwargs["properties"].priority, 8)

    def test_unknown_queue_is_rejected(self):
        with self.assertRaises(ValueError):
            self.client.submit_task("yoq_navbat", "resize", {})
        self.channel.basic_publish.assert_not_called()

    def test_reconnects_when_connection_was_closed(self):
        self.connection.is_closed = True
        before = self.connect.call_count
        self.client.submit_task("image_processing", "resize", {})
        self.assertEqual(self.connect.call_count, before + 1)

    def test_get_metrics_uses_management_api(self):
        with patch("src.client.fetch_metrics", return_value={"workers": 2}) as fetch:
            self.assertEqual(self.client.get_metrics(), {"workers": 2})
        fetch.assert_called_once_with()

    def test_close(self):
        self.client.close()
        self.connection.close.assert_called_once()

    # ---- o'lik xabarlarni qayta yuborish
    def test_retry_dead_letters_requeues_to_source_queue_and_resets_retries(self):
        task = {"task_id": "t-1", "type": "resize", "payload": {}, "priority": 7, "retry_count": 3}
        self.channel.basic_get.side_effect = [
            (delivery(11), dead_letter(task), encode(task)),
            (None, None, None),
        ]

        self.assertEqual(self.client.retry_dead_letters(), 1)

        self.channel.basic_get.assert_called_with(queue=DEAD_LETTER_QUEUE, auto_ack=False)
        kwargs = self.channel.basic_publish.call_args.kwargs
        self.assertEqual((kwargs["exchange"], kwargs["routing_key"]), (EXCHANGE_NAME, "image_processing"))
        self.assertEqual(decode(kwargs["body"])["retry_count"], 0)
        self.assertEqual(kwargs["properties"].priority, 7)
        self.channel.basic_ack.assert_called_once_with(delivery_tag=11)

    def test_unroutable_and_corrupted_messages_stay_in_the_dead_letter_queue(self):
        good = {"task_id": "ok", "type": "x", "payload": {}, "priority": 5, "retry_count": 3}
        self.channel.basic_get.side_effect = [
            (delivery(1), dead_letter(good, source_queue=None), encode(good)),      # asl navbat noma'lum
            (delivery(2), dead_letter(good, source_queue="yoq_navbat"), encode(good)),
            (delivery(3), dead_letter(good), b"buzuq"),                             # buzuq xabar
            (delivery(4), dead_letter(good, source_queue="data_analysis"), encode(good)),
            (None, None, None),
        ]

        self.assertEqual(self.client.retry_dead_letters(), 1)

        self.assertEqual(self.channel.basic_publish.call_args.kwargs["routing_key"], "data_analysis")
        self.channel.basic_ack.assert_called_once_with(delivery_tag=4)
        requeued_tags = sorted(c.kwargs["delivery_tag"] for c in self.channel.basic_nack.call_args_list)
        self.assertEqual(requeued_tags, [1, 2, 3])
        self.assertTrue(all(c.kwargs["requeue"] is True for c in self.channel.basic_nack.call_args_list))

    def test_retry_dead_letters_respects_limit(self):
        task = {"task_id": "t", "type": "x", "payload": {}, "priority": 5, "retry_count": 3}
        self.channel.basic_get.side_effect = lambda **kw: (delivery(1), dead_letter(task), encode(task))
        self.assertEqual(self.client.retry_dead_letters(limit=3), 3)
        self.assertEqual(self.channel.basic_publish.call_count, 3)

    def test_retry_dead_letters_on_empty_queue(self):
        self.channel.basic_get.return_value = (None, None, None)
        self.assertEqual(self.client.retry_dead_letters(), 0)
        self.channel.basic_publish.assert_not_called()


if __name__ == "__main__":
    unittest.main()
