import json
import unittest
import zlib
from unittest.mock import MagicMock, patch

from src import worker as worker_module
from src.config import DEAD_LETTER_QUEUE, EXCHANGE_NAME, MAX_RETRIES, PREFETCH_COUNT, RETRY_BACKOFF
from tests.helpers import decode, delivery, encode, fake_connection

TASK = {"task_id": "t-1", "type": "resize", "payload": {}, "priority": 5, "retry_count": 0}


class WorkerSetupTests(unittest.TestCase):
    def setUp(self):
        self.connection, self.channel = fake_connection()
        patcher = patch("src.rabbit.connect", return_value=self.connection)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_setup_declares_dead_letter_queue_and_prefetch(self):
        worker_module.Worker("image_processing", lambda t: None)
        queues = [c.kwargs["queue"] for c in self.channel.queue_declare.call_args_list]
        self.assertIn(DEAD_LETTER_QUEUE, queues)
        self.assertIn("image_processing", queues)
        self.channel.basic_qos.assert_called_once_with(prefetch_count=PREFETCH_COUNT)

    def test_unknown_queue_is_rejected(self):
        with self.assertRaises(ValueError):
            worker_module.Worker("yoq_navbat", lambda t: None)

    def test_start_consumes_from_own_queue(self):
        w = worker_module.Worker("data_analysis", lambda t: None)
        w.start()
        kwargs = self.channel.basic_consume.call_args.kwargs
        self.assertEqual(kwargs["queue"], "data_analysis")
        self.assertEqual(kwargs["on_message_callback"], w.process_task)
        self.channel.start_consuming.assert_called_once()


class ProcessTaskTests(unittest.TestCase):
    def setUp(self):
        connection, self.channel = fake_connection()
        with patch("src.rabbit.connect", return_value=connection):
            self.calls = []
            self.worker = worker_module.Worker("image_processing", self.calls.append)
        self.ch = MagicMock(name="delivery_channel")
        self.props = MagicMock(name="properties")

    def run_task(self, body, callback=None):
        if callback:
            self.worker.callback = callback
        self.worker.process_task(self.ch, delivery(7), self.props, body)

    def test_success_acks_and_passes_decoded_task_to_callback(self):
        self.run_task(encode(TASK))
        self.assertEqual(self.calls, [TASK])
        self.ch.basic_ack.assert_called_once_with(delivery_tag=7)
        self.ch.basic_nack.assert_not_called()
        self.ch.basic_publish.assert_not_called()

    def test_failure_republishes_with_incremented_retry_and_acks_original(self):
        def boom(task):
            raise RuntimeError("xato")

        with patch("src.worker.time.sleep") as sleep:
            self.run_task(encode(TASK), boom)

        sleep.assert_called_once_with(RETRY_BACKOFF[0])
        kwargs = self.ch.basic_publish.call_args.kwargs
        self.assertEqual((kwargs["exchange"], kwargs["routing_key"], kwargs["properties"]),
                         (EXCHANGE_NAME, "image_processing", self.props))
        self.assertEqual(decode(kwargs["body"])["retry_count"], 1)
        # Asl xabar ack qilinadi: aks holda u har safar o'lik xabarlar navbatiga ham tushib qolardi
        self.ch.basic_ack.assert_called_once_with(delivery_tag=7)
        self.ch.basic_nack.assert_not_called()

    def test_backoff_grows_with_each_retry(self):
        def boom(task):
            raise RuntimeError("xato")

        for current_retry in range(MAX_RETRIES):
            self.ch.reset_mock()
            with patch("src.worker.time.sleep") as sleep:
                self.run_task(encode({**TASK, "retry_count": current_retry}), boom)
            sleep.assert_called_once_with(RETRY_BACKOFF[current_retry])

    def test_exhausted_retries_go_to_dead_letter_via_nack(self):
        def boom(task):
            raise RuntimeError("xato")

        with patch("src.worker.time.sleep") as sleep:
            self.run_task(encode({**TASK, "retry_count": MAX_RETRIES}), boom)

        sleep.assert_not_called()
        self.ch.basic_publish.assert_not_called()  # qo'lda DLX ga yuborilmaydi: broker o'zi yo'naltiradi
        self.ch.basic_nack.assert_called_once_with(delivery_tag=7, requeue=False)
        self.ch.basic_ack.assert_not_called()

    def test_corrupted_message_is_dead_lettered_without_crashing(self):
        self.run_task(b"zlib emas")
        self.assertEqual(self.calls, [])
        self.ch.basic_nack.assert_called_once_with(delivery_tag=7, requeue=False)

    def test_valid_zlib_but_invalid_json_is_dead_lettered(self):
        self.run_task(zlib.compress(b"{bu json emas"))
        self.ch.basic_nack.assert_called_once_with(delivery_tag=7, requeue=False)
        self.assertEqual(self.calls, [])

    def test_example_callback_accepts_a_task(self):
        with patch("src.worker.time.sleep"):
            worker_module.example_callback(TASK)


if __name__ == "__main__":
    unittest.main()
