"""Forwards Glue requests to moto, except GetTables on a fault_* database, which fails with that fault's Glue error.

UpdateTable is checked against its VersionId as Glue does (moto ignores it), and on tables named race_* (once, adding
the column 'racer') or contended_* (every time, bumping the parameter 'contention') another writer changes the table
right before the update arrives."""

import argparse
import http.client
import json
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

FAULTS = {"fault_access_denied": "AccessDeniedException", "fault_invalid_input": "InvalidInputException"}
TABLE_INPUT_FIELDS = ["Name", "Description", "Owner", "LastAccessTime", "LastAnalyzedTime", "Retention",
                      "StorageDescriptor", "PartitionKeys", "ViewOriginalText", "ViewExpandedText", "TableType",
                      "Parameters", "TargetTable"]
NOT_FORWARDED = {"connection", "keep-alive", "transfer-encoding", "te", "trailer", "upgrade", "content-length",
                 "server", "date"}


def injected_error(target, body):
    if target != "AWSGlue.GetTables":
        return None
    try:
        database = json.loads(body or b"{}").get("DatabaseName", "")
    except ValueError:
        return None
    return next((error for prefix, error in FAULTS.items() if database.startswith(prefix)), None)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        self.relay()

    def do_POST(self):
        self.relay()

    def relay(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        target = self.headers.get("X-Amz-Target")
        error = injected_error(target, body)
        if not error and target == "AWSGlue.UpdateTable":
            error = self.check_update_table(json.loads(body or b"{}"))
        if error:
            self.reply_error(error)
            return
        status, headers, data = self.forward(body)
        self.reply(status, headers, data)

    def forward(self, body, target=None):
        upstream = http.client.HTTPConnection(self.server.upstream.hostname, self.server.upstream.port, timeout=60)
        try:
            headers = {k: v for k, v in self.headers.items() if k.lower() not in NOT_FORWARDED}
            if target:
                headers["X-Amz-Target"] = target
            upstream.request(self.command, self.path, body=body, headers=headers)
            response = upstream.getresponse()
            data = response.read()
            return response.status, [(k, v) for k, v in response.getheaders() if k.lower() not in NOT_FORWARDED], data
        finally:
            upstream.close()

    def call(self, operation, request):
        status, _, data = self.forward(json.dumps(request).encode(), "AWSGlue." + operation)
        if status != 200:
            raise RuntimeError("%s failed in glue_fault_proxy: %s" % (operation, data))
        return json.loads(data or b"{}")

    def check_update_table(self, request):
        """The Glue error for an UpdateTable, after the concurrent change of race_* / contended_* tables"""
        name = request.get("TableInput", {}).get("Name", "")
        key = {k: request[k] for k in ("CatalogId", "DatabaseName") if k in request}
        key["Name"] = name
        table = self.call("GetTable", key)["Table"]
        concurrent = None
        if name.startswith("race_"):
            columns = table.get("StorageDescriptor", {}).get("Columns", [])
            if not any(column["Name"] == "racer" for column in columns):
                concurrent = lambda t: t["StorageDescriptor"].setdefault("Columns", []).append(
                    {"Name": "racer", "Type": "string"})
        elif name.startswith("contended_"):
            def concurrent(t):
                parameters = t.setdefault("Parameters", {})
                parameters["contention"] = str(int(parameters.get("contention", "0")) + 1)
        if concurrent:
            table_input = {field: table[field] for field in TABLE_INPUT_FIELDS if field in table}
            concurrent(table_input)
            self.call("UpdateTable", {**{k: v for k, v in key.items() if k != "Name"}, "TableInput": table_input})
            table = self.call("GetTable", key)["Table"]
        if "VersionId" in request and request["VersionId"] != table.get("VersionId"):
            return "ConcurrentModificationException"
        return None

    def reply_error(self, error):
        payload = json.dumps({"__type": error, "Message": "injected by glue_fault_proxy"}).encode()
        self.reply(400, [("Content-Type", "application/x-amz-json-1.1"), ("x-amzn-ErrorType", error)], payload)

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
