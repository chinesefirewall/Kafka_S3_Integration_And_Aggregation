#!/usr/bin/env python
# coding: utf-8


import json
import socket
import time

import requests
from ksqldb import KSQLdbClient

host = 'localhost'  # Example: 'www.example.com'
port = 8088  # Example: 80
ksql_url = (f"http://{host}:{port}")

retry_interval_seconds = 5  # Time between connection attempts


def check_connection(host, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((host, port)) == 0


while True:
    if check_connection(host, port):
        print(f"Connection to {host} on port {port} succeeded.")
        break
    else:
        print(f"Unable to connect to {host} on port {port}. Retrying in {retry_interval_seconds} seconds...")
        time.sleep(retry_interval_seconds)

client = KSQLdbClient(ksql_url)

# Headers for the HTTP request
headers = {
    "Content-Type": "application/vnd.ksql.v1+json; charset=utf-8",
    "Accept": "application/vnd.ksql.v1+json"
}


def execute_ksql_statement(ksql_statement):
    # The payload containing the KSQL statement
    payload = {
        "ksql": ksql_statement,
        "streamsProperties": {
            "ksql.streams.auto.offset.reset": "earliest"
        }
    }

    # Send the request to the ksqlDB server
    response = requests.post(f"{ksql_url}/ksql", headers=headers, data=json.dumps(payload))

    # Check the response
    if response.status_code == 200:
        print(response.json())
    else:
        print(response.text)


def execute_ksql_query(ksql_query):
    payload = {
        "ksql": ksql_query,
        "streamsProperties": {
            "ksql.streams.auto.offset.reset": "earliest"
        }
    }

    response = requests.post(f"{ksql_url}/query", headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        results = response.json()
        print(results)
    else:
        print("Failed to query count.")
        print(response.text)


# client.paid_loans.count

ksql_statement = """
CREATE STREAM loan_stream (
    payload STRUCT<
        after STRUCT<
            id INT,
            client_id INT,
            status STRING
        >
    >
) WITH (
    KAFKA_TOPIC='creditstar.public.loan',
    VALUE_FORMAT='JSON'
);
"""
execute_ksql_statement(ksql_statement)

ksql_statement = """
CREATE STREAM loan_stream_rekeyed AS
SELECT payload->after->id AS id,
       payload->after->client_id AS client_id,
       payload->after->status AS status
FROM loan_stream
EMIT CHANGES;
"""

execute_ksql_statement(ksql_statement)

time.sleep(5)

q = """
CREATE TABLE paid_loans_count
WITH (KAFKA_TOPIC='client.paid_loans.count', VALUE_FORMAT='JSON') AS
SELECT client_id, COUNT(*) AS total_paid_loans
FROM loan_stream_rekeyed
WHERE status = 'paid'
GROUP BY client_id;
"""

client.ksql(q)
