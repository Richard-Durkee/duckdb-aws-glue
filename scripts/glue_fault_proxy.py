"""Forwards Glue requests to moto, except on a fault_* database, where a request fails with the Glue error the fault
assigns to it: GetTables on fault_access_denied* / fault_invalid_input* fails outright, while CreateDatabase and
CreateTable of fault_already_exists* are forwarded (so the object does exist afterwards) and then answered with
AlreadyExistsException, as Glue answers a client that lost the race to create the same object. What the other client
created differs from what was asked for, so a test can tell the winner from a replacement: the database carries
the description 'created by another client', a table one more column 'created_by_another_client', a view reads
'SELECT 2 AS one'. An UpdateTable on a fault_concurrent_update* database is preceded by another client's update,
which sets the table parameter 'changed_by_another_client', and is then refused with ConcurrentModificationException
if its VersionId is not the table's current one, as Glue does (moto ignores VersionId); without a VersionId it is
forwarded and overwrites the other client's change. A BatchCreatePartition on a fault_partition_elsewhere* database is
preceded by another client's registration of the same partitions at '<location>_by_another_client', so that each is
refused with AlreadyExistsException while its files are not where Glue points. A CreatePartition on a
fault_noncanonical_values* database registers each value with a leading '0', as another engine may spell an integer
('01' for 1). A BatchCreatePartition on a fault_slow_partitions* database is
forwarded only after SLOW_PARTITIONS_SECONDS, so a statement timeout lands while partitions are registered; one on a
fault_recreated_during_write* database finds the table dropped and created again by another client, and then fails
with InternalServiceException, so the write that registers them fails with a table that is no longer its own. A
request on a fault_throttled* database is answered with ThrottlingException on its first attempt (the SDK's
amz-sdk-request header) and forwarded when the SDK retries it. UpdateTable and DeleteTable on a
fault_unfinished_ctas* database are answered with AccessDeniedException, so a CREATE TABLE AS there can neither finish
nor undo its table and leaves it as a crashed one would. An UpdateTable on a fault_lost_update_answer* database is
forwarded and then answered with InternalServiceException, as if its answer had been lost; on a
fault_lost_update_and_read* database the GetTable that follows it fails the same way too. A CreateTable on a
fault_lost_create_answer* database is answered with InternalFailure (HTTP 500), which the SDK sends the request
again for. Only the first UpdateTable of a table on a
fault_conflict_once* database races another client's update (the ones after it find the other client's change in
place). The second GetTable of a table after its CreateTable on a fault_recreated_before_finish* database finds the
table dropped and created again by another client."""

import argparse
import http.client
import json
import threading
import time
import uuid
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# GetTables on a database with this prefix fails with this error, nothing is forwarded
LISTING_FAULTS = {"fault_access_denied": "AccessDeniedException", "fault_invalid_input": "InvalidInputException"}
# a table with this prefix is one Lake Formation does not let the caller describe: GetTable on it fails with
# AccessDeniedException, and GetTables leaves it out
DENIED_TABLE_PREFIX = "fault_denied_"
# CreateDatabase / CreateTable on a database with this prefix is forwarded and then reported as lost to another client
RACED_CREATE_PREFIX = "fault_already_exists"
# UpdateTable on a database with this prefix races another client's update of the same table
RACED_UPDATE_PREFIX = "fault_concurrent_update"
# only the first UpdateTable of each table on a database with this prefix races another client's update
RACED_UPDATE_ONCE_PREFIX = "fault_conflict_once"
# the first attempt of a request on a database with this prefix is throttled, its retries are not
THROTTLED_PREFIX = "fault_throttled"
# BatchCreatePartition on a database with this prefix is forwarded after a delay
SLOW_PARTITIONS_PREFIX = "fault_slow_partitions"
SLOW_PARTITIONS_SECONDS = 4
# BatchCreatePartition on a database with this prefix finds its table recreated by another client, and then fails
RECREATED_PREFIX = "fault_recreated_during_write"
# BatchCreatePartition on a database with this prefix races another client registering the partitions elsewhere
RACED_PARTITION_PREFIX = "fault_partition_elsewhere"
# CreatePartition on a database with this prefix registers each value with a leading '0'
NONCANONICAL_PREFIX = "fault_noncanonical_values"
# on a database with this prefix, a CREATE TABLE AS cannot finish or undo its table: the UpdateTable that removes its
# duckdb.write_id and the DeleteTable of its undo are refused, which leaves the table as a crash would
UNFINISHED_CTAS_PREFIX = "fault_unfinished_ctas"
# an UpdateTable on a database with this prefix is applied, and then answered with an error
LOST_UPDATE_ANSWER_PREFIX = "fault_lost_update_answer"
# as LOST_UPDATE_ANSWER_PREFIX, and the next GetTable of the table fails too
LOST_UPDATE_AND_READ_PREFIX = "fault_lost_update_and_read"
# a CreateTable on a database with this prefix is applied, and then answered with an error
LOST_CREATE_ANSWER_PREFIX = "fault_lost_create_answer"
# the second GetTable of a table after its CreateTable, on a database with this prefix, finds it created again
RECREATED_BEFORE_FINISH_PREFIX = "fault_recreated_before_finish"
RECREATED_BEFORE_FINISH_READ = 2
# the TableInput fields of a Table that GetTable returns
TABLE_INPUT_FIELDS = ("Name", "Description", "Owner", "Retention", "StorageDescriptor", "PartitionKeys",
                      "ViewOriginalText", "ViewExpandedText", "TableType", "Parameters", "TargetTable")
NOT_FORWARDED = {"connection", "keep-alive", "transfer-encoding", "te", "trailer", "upgrade", "content-length",
                 "server", "date"}


def parse_request(body):
    try:
        return json.loads(body or b"{}")
    except ValueError:
        return {}


def request_database(target, request):
    if target == "AWSGlue.CreateDatabase":
        return request.get("DatabaseInput", {}).get("Name", "")
    return request.get("DatabaseName", "")


def listing_error(target, database):
    if target != "AWSGlue.GetTables":
        return None
    return next((error for prefix, error in LISTING_FAULTS.items() if database.startswith(prefix)), None)


def is_denied_table(target, request):
    return target == "AWSGlue.GetTable" and request.get("Name", "").startswith(DENIED_TABLE_PREFIX)


def without_denied_tables(data):
    result = json.loads(data)
    result["TableList"] = [t for t in result.get("TableList", []) if not t["Name"].startswith(DENIED_TABLE_PREFIX)]
    return json.dumps(result).encode()


def is_raced_create(target, database):
    return target in ("AWSGlue.CreateDatabase", "AWSGlue.CreateTable") and database.startswith(RACED_CREATE_PREFIX)


def as_other_clients_object(target, request):
    """The object the other client created: the same name, a definition the losing client did not ask for."""
    if target == "AWSGlue.CreateDatabase":
        request["DatabaseInput"]["Description"] = "created by another client"
    elif request.get("TableInput", {}).get("TableType") == "VIRTUAL_VIEW":
        request["TableInput"]["ViewOriginalText"] = "SELECT 2 AS one"
        request["TableInput"]["ViewExpandedText"] = "SELECT 2 AS one"
    else:
        columns = request["TableInput"].setdefault("StorageDescriptor", {}).setdefault("Columns", [])
        columns.append({"Name": "created_by_another_client", "Type": "int"})
        request["TableInput"].get("Parameters", {}).pop("duckdb.write_id", None)
    return json.dumps(request).encode()


def is_throttled(database, sdk_request):
    attempts = (sdk_request or "").replace(" ", "").split(";")
    return database.startswith(THROTTLED_PREFIX) and "attempt=1" in attempts


def is_raced_update(target, database):
    return target == "AWSGlue.UpdateTable" and database.startswith(RACED_UPDATE_PREFIX)


def is_raced_update_once(target, database):
    return target == "AWSGlue.UpdateTable" and database.startswith(RACED_UPDATE_ONCE_PREFIX)


def is_slow_partitions(target, database):
    return target == "AWSGlue.BatchCreatePartition" and database.startswith(SLOW_PARTITIONS_PREFIX)


def is_unfinished_ctas(target, database):
    return target in ("AWSGlue.UpdateTable", "AWSGlue.DeleteTable") and database.startswith(UNFINISHED_CTAS_PREFIX)


def is_lost_update_answer(target, database):
    return target == "AWSGlue.UpdateTable" and (database.startswith(LOST_UPDATE_ANSWER_PREFIX)
                                                or database.startswith(LOST_UPDATE_AND_READ_PREFIX))


def is_lost_create_answer(target, database):
    return target == "AWSGlue.CreateTable" and database.startswith(LOST_CREATE_ANSWER_PREFIX)


def table_key(target, request, database):
    table_input = request.get("TableInput", {})
    name = table_input.get("Name") if target in ("AWSGlue.UpdateTable", "AWSGlue.CreateTable") else request.get("Name")
    return (database, name)


def is_recreated_during_write(target, database):
    return target == "AWSGlue.BatchCreatePartition" and database.startswith(RECREATED_PREFIX)


def is_raced_partition_create(target, database):
    return target == "AWSGlue.BatchCreatePartition" and database.startswith(RACED_PARTITION_PREFIX)


def is_noncanonical_create(target, database):
    return target == "AWSGlue.CreatePartition" and database.startswith(NONCANONICAL_PREFIX)


def with_noncanonical_values(request):
    """The partition as another engine registers it: each value with a leading '0'."""
    partition = request.get("PartitionInput", {})
    partition["Values"] = ["0" + value for value in partition.get("Values", [])]
    return json.dumps(request).encode()


def as_other_clients_partitions(request):
    """The partitions the other client registered: the same values, at another location."""
    other = json.loads(json.dumps(request))
    for partition in other.get("PartitionInputList", []):
        descriptor = partition.setdefault("StorageDescriptor", {})
        descriptor["Location"] = descriptor.get("Location", "").rstrip("/") + "_by_another_client"
    return json.dumps(other).encode()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        self.relay()

    def do_POST(self):
        self.relay()

    def relay(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        target = self.headers.get("X-Amz-Target")
        request = parse_request(body)
        database = request_database(target, request)
        error = listing_error(target, database)
        if not error and is_denied_table(target, request):
            error = "AccessDeniedException"
        if not error and is_throttled(database, self.headers.get("amz-sdk-request")):
            error = "ThrottlingException"
        if error:
            self.reply_error(error)
            return
        if is_unfinished_ctas(target, database):
            self.reply_error("AccessDeniedException")
            return
        if is_slow_partitions(target, database):
            time.sleep(SLOW_PARTITIONS_SECONDS)
        if is_recreated_during_write(target, database):
            self.recreate_as_other_client(database, request["TableName"])
            self.reply_error("InternalServiceException")
            return
        if target == "AWSGlue.GetTable":
            key = table_key(target, request, database)
            with self.server.lock:
                failing = key in self.server.failing_reads
                self.server.failing_reads.discard(key)
                reads = self.server.reads_after_create.get(key)
                if reads is not None:
                    reads += 1
                    self.server.reads_after_create[key] = reads
            if failing:
                self.reply_error("InternalServiceException")
                return
            if database.startswith(RECREATED_BEFORE_FINISH_PREFIX) and reads == RECREATED_BEFORE_FINISH_READ:
                self.recreate_as_other_client(database, key[1])
        # only the first UpdateTable of a table races: the table then carries the other client's change
        raced_once = is_raced_update_once(target, database) and "changed_by_another_client" not in (
            self.get_table(database, request["TableInput"]["Name"]).get("Parameters", {}))
        if is_raced_update(target, database) or raced_once:
            current_version = self.update_as_other_client(database, request["TableInput"]["Name"])
            if "VersionId" in request and request["VersionId"] != current_version:
                self.reply_error("ConcurrentModificationException")
                return
        if is_raced_partition_create(target, database):
            status, _, data = self.forward(target, as_other_clients_partitions(request))
            if status != 200:
                raise RuntimeError(f"BatchCreatePartition as another client failed: {data!r}")
        if is_noncanonical_create(target, database):
            body = with_noncanonical_values(request)
        raced = is_raced_create(target, database)
        if raced:
            body = as_other_clients_object(target, request)
        status, headers, data = self.forward(target, body)
        if is_lost_update_answer(target, database) and status == 200:
            if database.startswith(LOST_UPDATE_AND_READ_PREFIX):
                with self.server.lock:
                    self.server.failing_reads.add(table_key(target, request, database))
            self.reply_error("InternalServiceException")
            return
        if target == "AWSGlue.CreateTable" and status == 200:
            with self.server.lock:
                self.server.reads_after_create[table_key(target, request, database)] = 0
            if is_lost_create_answer(target, database):
                # an error the SDK sends the request again for
                self.reply_error("InternalFailure", 500)
                return
        if raced and status == 200:
            # the object exists now, but as far as this client can tell another client created it first
            self.reply_error("AlreadyExistsException")
            return
        if target == "AWSGlue.GetTables" and status == 200:
            data = without_denied_tables(data)
        self.reply(status, headers, data)

    def forward(self, target, body):
        upstream = http.client.HTTPConnection(self.server.upstream.hostname, self.server.upstream.port, timeout=60)
        try:
            # moto takes the account from the credentials and does not check the signature
            headers = {k: v for k, v in self.headers.items() if k.lower() not in NOT_FORWARDED}
            headers["X-Amz-Target"] = target
            upstream.request(self.command, self.path, body=body, headers=headers)
            response = upstream.getresponse()
            data = response.read()
        finally:
            upstream.close()
        return response.status, [(k, v) for k, v in response.getheaders() if k.lower() not in NOT_FORWARDED], data

    def get_table(self, database, name):
        status, _, data = self.forward("AWSGlue.GetTable",
                                       json.dumps({"DatabaseName": database, "Name": name}).encode())
        if status != 200:
            raise RuntimeError(f"GetTable {database}.{name} failed: {data!r}")
        return json.loads(data)["Table"]

    def update_as_other_client(self, database, name):
        """Change the table as another client would, returns the VersionId the change left it at."""
        table = self.get_table(database, name)
        table_input = {field: table[field] for field in TABLE_INPUT_FIELDS if field in table}
        table_input.setdefault("Parameters", {})["changed_by_another_client"] = "true"
        status, _, data = self.forward("AWSGlue.UpdateTable",
                                       json.dumps({"DatabaseName": database, "TableInput": table_input}).encode())
        if status != 200:
            raise RuntimeError(f"UpdateTable {database}.{name} failed: {data!r}")
        return self.get_table(database, name)["VersionId"]

    def recreate_as_other_client(self, database, name):
        """Drop the table and create it again, as another client would: a table of the same name, created later."""
        table = self.get_table(database, name)
        table_input = {field: table[field] for field in TABLE_INPUT_FIELDS if field in table}
        # another client's table does not carry the write id of the statement that created the dropped one
        table_input.get("Parameters", {}).pop("duckdb.write_id", None)
        status, _, data = self.forward("AWSGlue.DeleteTable",
                                       json.dumps({"DatabaseName": database, "Name": name}).encode())
        if status != 200:
            raise RuntimeError(f"DeleteTable {database}.{name} failed: {data!r}")
        status, _, data = self.forward("AWSGlue.CreateTable",
                                       json.dumps({"DatabaseName": database, "TableInput": table_input}).encode())
        if status != 200:
            raise RuntimeError(f"CreateTable {database}.{name} failed: {data!r}")

    def reply_error(self, error, status=400):
        payload = json.dumps({"__type": error, "Message": "injected by glue_fault_proxy"}).encode()
        headers = [("Content-Type", "application/x-amz-json-1.1"), ("x-amzn-ErrorType", error)]
        self.reply(status, headers + [("x-amzn-RequestId", str(uuid.uuid4()))], payload)

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
    server.lock = threading.Lock()
    # (database, table) whose next GetTable fails, after a fault_lost_update_and_read* UpdateTable
    server.failing_reads = set()
    # (database, table) -> the GetTables of it since it was created
    server.reads_after_create = {}
    server.serve_forever()


if __name__ == "__main__":
    main()
