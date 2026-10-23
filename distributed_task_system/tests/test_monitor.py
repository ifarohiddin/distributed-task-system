import unittest
from unittest.mock import patch

import requests

from src.monitor import app


class MonitorTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    def test_health(self):
        response = self.client.get("/health")
        self.assertEqual((response.status_code, response.get_json()), (200, {"status": "ok"}))

    def test_metrics_returns_real_data(self):
        payload = {"queues": {}, "workers": 2, "processed_per_second": 1.0}
        with patch("src.monitor.fetch_metrics", return_value=payload):
            response = self.client.get("/metrics")
        self.assertEqual((response.status_code, response.get_json()), (200, payload))

    def test_metrics_reports_503_when_rabbitmq_is_unreachable(self):
        with patch("src.monitor.fetch_metrics", side_effect=requests.ConnectionError("offline")):
            response = self.client.get("/metrics")
        self.assertEqual(response.status_code, 503)
        self.assertIn("error", response.get_json())


if __name__ == "__main__":
    unittest.main()
