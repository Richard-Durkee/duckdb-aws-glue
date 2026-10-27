"""Forwards Glue requests to moto, except:

- GetTables on a fault_* database, which fails with that fault's Glue error;
- partition lookups of a table named fault_refuse_*: BatchGetPartition, and GetPartitions with an Expression, fail with
  InvalidInputException, as Glue refuses a value or an expression it does not accept;
- BatchGetPartition of a table named fault_batch_denied_*, which fails with AccessDeniedException, as for a principal
  allowed glue:GetPartitions but not glue:BatchGetPartition;
- BatchGetPartition of a table named fault_unprocessed_*, which leaves every key unprocessed (UnprocessedKeys, Glue's
  throttling signal), and of a table named fault_halve_*, which processes only the first half of the keys of a request
  with more than one and leaves the others unprocessed, so that asking again for those eventually gets all;
- partition lookups (BatchGetPartition, GetPartitions with an Expression) of a table named fault_throttle_*,
  fault_unavailable_*, fault_expired_*, fault_forbidden_* or fault_validation_*, which fail with ThrottlingException
  (400), InternalServiceException (500), ExpiredTokenException (400), a 403 without a Glue error (an HTML body) or
  ValidationException (400, as Glue answers an expression longer than 2048 code points or with a character it does
  not accept in a literal, and a BatchGetPartition of more than 1000 keys).
"""

import argparse
import http.client
import json
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

FAULTS = {"fault_access_denied": "AccessDeniedException", "fault_invalid_input": "InvalidInputException"}
NOT_FORWARDED = {"connection", "keep-alive", "transfer-encoding", "te", "trailer", "upgrade", "content-length",
                 "server", "date"}


def injected_error(target, request):
    if target == "AWSGlue.GetTables":
        database = request.get("DatabaseName", "")
        return next((error for prefix, error in FAULTS.items() if database.startswith(prefix)), None)
    if target == "AWSGlue.BatchGetPartition" and request.get("TableName", "").startswith("fault_batch_denied_"):
        return "AccessDeniedException"
    if request.get("TableName", "").startswith("fault_refuse_"):
        if target == "AWSGlue.BatchGetPartition" or (target == "AWSGlue.GetPartitions" and request.get("Expression")):
            return "InvalidInputException"
    return None


LOOKUP_FAILURES = {"fault_throttle_": (400, "ThrottlingException"), "fault_unavailable_": (500, "InternalServiceException"),
                   "fault_expired_": (400, "ExpiredTokenException"), "fault_forbidden_": (403, None),
                   "fault_validation_": (400, "ValidationException")}


def injected_lookup_failure(target, request):
    if target != "AWSGlue.BatchGetPartition" and not (target == "AWSGlue.GetPartitions" and request.get("Expression")):
        return None
    table = request.get("TableName", "")
    return next((failure for prefix, failure in LOOKUP_FAILURES.items() if table.startswith(prefix)), None)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        self.relay()

    def do_POST(self):
        self.relay()

    def relay(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        target = self.headers.get("X-Amz-Target")
        try:
            request = json.loads(body or b"{}")
        except ValueError:
            request = {}
        if not isinstance(request, dict):
            request = {}
        failure = injected_lookup_failure(target, request)
        if failure:
            status, error = failure
            if error:
                payload = json.dumps({"__type": error, "Message": "injected by glue_fault_proxy"}).encode()
                headers = [("Content-Type", "application/x-amz-json-1.1"), ("x-amzn-ErrorType", error)]
                self.reply(status, headers, payload)
            else:
                self.reply(status, [("Content-Type", "text/html")], b"<html><body>Forbidden</body></html>")
            return
        error = injected_error(target, request)
        if error:
            payload = json.dumps({"__type": error, "Message": "injected by glue_fault_proxy"}).encode()
            self.reply(400, [("Content-Type", "application/x-amz-json-1.1"), ("x-amzn-ErrorType", error)], payload)
            return
        table = request.get("TableName", "")
        unprocessed = []
        if target == "AWSGlue.BatchGetPartition" and table.startswith("fault_unprocessed_"):
            payload = json.dumps({"Partitions": [], "UnprocessedKeys": request.get("PartitionsToGet", [])}).encode()
            self.reply(200, [("Content-Type", "application/x-amz-json-1.1")], payload)
            return
        if target == "AWSGlue.BatchGetPartition" and table.startswith("fault_halve_"):
            keys = request.get("PartitionsToGet", [])
            if len(keys) > 1:
                processed = (len(keys) + 1) // 2
                unprocessed = keys[processed:]
                request["PartitionsToGet"] = keys[:processed]
                body = json.dumps(request).encode()
        status, headers, data = self.forward(body)
        if unprocessed and status == 200:
            response = json.loads(data or b"{}")
            response["UnprocessedKeys"] = response.get("UnprocessedKeys", []) + unprocessed
            data = json.dumps(response).encode()
        self.reply(status, headers, data)

    def forward(self, body):
        upstream = http.client.HTTPConnection(self.server.upstream.hostname, self.server.upstream.port, timeout=60)
        try:
            headers = {k: v for k, v in self.headers.items() if k.lower() not in NOT_FORWARDED}
            upstream.request(self.command, self.path, body=body, headers=headers)
            response = upstream.getresponse()
            data = response.read()
            return response.status, [(k, v) for k, v in response.getheaders() if k.lower() not in NOT_FORWARDED], data
        finally:
            upstream.close()

    def reply(self, status, headers, body):
        self.send_response(status)
        for name, value in headers:
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5556)
    parser.add_argument("--upstream", default="http://localhost:5555")
    args = parser.parse_args()
    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    server.upstream = urllib.parse.urlsplit(args.upstream)
    server.serve_forever()


if __name__ == "__main__":
    main()
