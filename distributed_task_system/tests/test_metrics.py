import unittest
from unittest.mock import MagicMock, patch

import requests

from src import metrics
from src.config import DEAD_LETTER_QUEUE, TASK_QUEUES


def api_response(status=200, **body):
    response = MagicMock(status_code=status)
    response.json.return_value = body
    response.raise_for_status.side_effect = None if status < 400 else requests.HTTPError(f"{status}")
    return response


class MetricsTests(unittest.TestCase):
    def test_aggregates_queue_stats(self):
        data = {
            "image_processing": api_response(messages=5, consumers=2, message_stats={"ack_details": {"rate": 1.5}}),
            "data_analysis": api_response(messages=1, consumers=1, message_stats={"ack_details": {"rate": 0.25}}),
            DEAD_LETTER_QUEUE: api_response(messages=3, consumers=0),
        }

        def fake_get(url, **kwargs):
            self.assertEqual(kwargs["timeout"], 3.0)
            return data[url.rsplit("/", 1)[1]]

        with patch("src.metrics.requests.get", side_effect=fake_get):
            result = metrics.fetch_metrics()

        self.assertEqual(result["queues"]["image_processing"], {"length": 5, "consumers": 2, "ack_rate": 1.5})
        self.assertEqual(result["queues"][DEAD_LETTER_QUEUE]["length"], 3)
        self.assertEqual(result["workers"], 3)
        self.assertEqual(result["processed_per_second"], 1.75)

    def test_missing_queues_count_as_empty(self):
        with patch("src.metrics.requests.get", return_value=api_response(status=404)):
            result = metrics.fetch_metrics()
        self.assertEqual(set(result["queues"]), {*TASK_QUEUES, DEAD_LETTER_QUEUE})
        self.assertEqual((result["workers"], result["processed_per_second"]), (0, 0))

    def test_stats_without_message_stats_default_to_zero_rate(self):
        with patch("src.metrics.requests.get", return_value=api_response(messages=2, consumers=1)):
            result = metrics.fetch_metrics()
        self.assertEqual(result["processed_per_second"], 0)

    def test_connection_errors_are_raised(self):
        with patch("src.metrics.requests.get", side_effect=requests.ConnectionError("offline")):
            with self.assertRaises(requests.RequestException):
                metrics.fetch_metrics()

    def test_http_errors_other_than_404_are_raised(self):
        with patch("src.metrics.requests.get", return_value=api_response(status=401)):
            with self.assertRaises(requests.HTTPError):
                metrics.fetch_metrics()

    def test_request_uses_management_port_and_credentials(self):
        with patch("src.metrics.requests.get", return_value=api_response(status=404)) as get:
            metrics.fetch_metrics()
        url = get.call_args_list[0].args[0]
        self.assertIn(":15672/api/queues/%2F/", url)
        self.assertEqual(get.call_args_list[0].kwargs["auth"], ("guest", "guest"))


if __name__ == "__main__":
    unittest.main()
