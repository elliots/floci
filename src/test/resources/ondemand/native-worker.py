"""Disposable native integration fixture; all observations live in process RAM."""

import base64
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import threading
import time
from urllib.parse import urlencode
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import uuid
import xml.etree.ElementTree as ET

state = {"boot": str(uuid.uuid4()), "requests": 0, "sqs": [], "rabbitmq": [],
         "errors": {}, "polls": {"sqs": 0, "rabbitmq": 0}}
lock = threading.Lock()


def record_error(source, error):
    details = {"type": type(error).__name__, "message": str(error)[:512]}
    if isinstance(error, HTTPError):
        details["status"] = error.code
        details["body"] = error.read(1024).decode("utf-8", errors="replace")
    with lock:
        state["errors"][source] = details
    print(source + " reconnect:", json.dumps(details), flush=True)


def sqs(action, **params):
    body = urlencode(dict(Action=action, Version="2012-11-05", **params)).encode()
    request = Request(os.environ["SQS_ENDPOINT"], body, {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": "AWS4-HMAC-SHA256 Credential=000000000000/20261016/us-east-1/sqs/aws4_request, "
                         "SignedHeaders=host, Signature=test",
    })
    with urlopen(request, timeout=5) as response:
        return ET.fromstring(response.read())


def sqs_worker():
    while True:
        try:
            response = sqs("ReceiveMessage", QueueUrl=os.environ["SQS_QUEUE_URL"],
                           MaxNumberOfMessages=1, WaitTimeSeconds=1, VisibilityTimeout=30)
            with lock:
                state["polls"]["sqs"] += 1
            for message in response.findall(".//{*}Message"):
                payload = message.findtext("{*}Body")
                receipt = message.findtext("{*}ReceiptHandle")
                with lock:
                    state["sqs"].append(payload)
                sqs("DeleteMessage", QueueUrl=os.environ["SQS_QUEUE_URL"], ReceiptHandle=receipt)
        except Exception as error:
            record_error("sqs", error)
            time.sleep(0.25)


def rabbit_worker():
    # The separate AMQP integration test covers unacknowledged delivery. This
    # fixture uses RabbitMQ's real HTTP consumption API to avoid installing SDKs
    # in the guest, and opens fresh connections after every memory restore.
    credentials = base64.b64encode((os.environ["RABBIT_USER"] + ":" + os.environ["RABBIT_PASSWORD"]).encode()).decode()
    while True:
        try:
            request = Request(os.environ["RABBIT_URL"] + "/api/queues/%2F/native-jobs/get",
                              json.dumps({"count": 1, "ackmode": "ack_requeue_false",
                                          "encoding": "auto", "truncate": 65536}).encode(),
                              {"Content-Type": "application/json", "Authorization": "Basic " + credentials})
            with urlopen(request, timeout=5) as response:
                messages = json.load(response)
            with lock:
                state["polls"]["rabbitmq"] += 1
                state["rabbitmq"].extend(message["payload"] for message in messages)
        except Exception as error:
            record_error("rabbitmq", error)
        time.sleep(0.25)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        with lock:
            if self.path == "/invoke":
                state["requests"] += 1
            body = json.dumps(state).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


threading.Thread(target=sqs_worker, daemon=True).start()
threading.Thread(target=rabbit_worker, daemon=True).start()
ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
