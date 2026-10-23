import requests
from flask import Flask, jsonify

from src.metrics import fetch_metrics

app = Flask(__name__)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/metrics", methods=["GET"])
def get_metrics():
    try:
        return jsonify(fetch_metrics())
    except requests.RequestException as exc:
        return jsonify({"error": "RabbitMQ Management API bilan bog'lanib bo'lmadi", "detail": str(exc)}), 503


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
