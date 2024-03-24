#!/usr/bin/env python
# coding: utf-8


import json
import os
from datetime import datetime, timedelta
from io import StringIO
from struct import unpack

import boto3
import pandas as pd
from kafka import KafkaConsumer

# Get S3 client
s3_client = boto3.client('s3')

topic_client_paid_loans_count = 'client.paid_loans.count'

topics = ['creditstar.public.user', 'creditstar.public.loan', 'creditstar.public.payment',
          topic_client_paid_loans_count]

# Configuration
# bootstrap_servers = ['b-1.kafkacluster.ygad26.c2.kafka.eu-west-1.amazonaws.com:9092']  # Adjust this to your Kafka brokers
# bucket_name = 'creditstar-home-work'  # Replace with your bucket name

key_BOOTSTRAP_SERVERS = 'BOOTSTRAP_SERVERS'
key_BUCKET_NAME = 'BUCKET_NAME'

bootstrap_servers = os.getenv(key_BOOTSTRAP_SERVERS)  # Adjust this to your Kafka brokers
bucket_name = os.getenv(key_BUCKET_NAME)  # Replace with your bucket name

assert bootstrap_servers is not None, f"ENV variable ´{key_BOOTSTRAP_SERVERS}´ is not set"
assert bucket_name is not None, f"ENV variable ´{key_BUCKET_NAME}´ is not set"

date_cols = {
    'creditstar.public.user': ['birth_date', 'created_on'],
    'creditstar.public.loan': ['created_on', 'matured_on', 'updated_on'],
    'creditstar.public.payment': ['created_on']
}


def convert_epoch_days_to_date(epoch_days):
    epoch_start = datetime(1970, 1, 1)
    return epoch_start + timedelta(days=int(epoch_days))


def save_df_to_s3(df: pd.DataFrame, object_name):
    # Convert DataFrame to CSV string
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Save CSV to S3
    s3_client.put_object(Bucket=bucket_name, Body=csv_buffer.getvalue(), Key=object_name)


def read_csv_from_s3(object_name: str):
    """
    Read a CSV file from an S3 bucket into a pandas DataFrame.

    :param bucket_name: Name of the S3 bucket.
    :param object_name: S3 object name, including path.
    :return: pandas DataFrame.
    """
    # Get S3 client
    s3_client = boto3.client('s3')

    # Get object from S3
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_name)
    except Exception as e:
        return False

    # Read CSV content
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        csv_string = response["Body"].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_string))
        return df
    else:
        return False


def topic_name_to_s3_object_name(topic_name: str):
    return topic_name.replace('.', '/') + '.csv'


def kafka_messages_into_dataframe(messages, topic_name):
    if topic_name == topic_client_paid_loans_count:
        ld = []
        for message in messages:
            d = {
                'TOTAL_PAID_LOANS': json.loads(message.value.decode())['TOTAL_PAID_LOANS'],
                'offset': message.offset,
                'client_id': unpack('>I', message.key)[0]
            }
            ld.append(d)

        df_message = pd.DataFrame(ld)
        df_message = df_message.sort_values(['client_id', 'offset']).drop_duplicates(subset=['client_id'],
                                                                                     keep='last').drop(
            columns=['offset'])

        return df_message
    else:
        # Convert messages into dataframe
        ld = [json.loads(x.value.decode())['payload']['after'] for x in messages]
        df_new = pd.DataFrame(ld)

        print("df_new.shape", df_new.shape)

        for col in date_cols[topic_name]:
            old_col = f"old_{col}"
            df_new = df_new.rename(columns={col: old_col})
            idx_not_nan = df_new[~df_new[old_col].isna()].index
            df_new.loc[idx_not_nan, col] = (df_new.loc[idx_not_nan, old_col].apply(convert_epoch_days_to_date)).dt.date
            df_new = df_new.drop(columns=[old_col])

        return df_new


def consumer_with_group(topic_name):
    # Initialize KafkaConsumer
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',  # Start reading at the earliest message
        enable_auto_commit=True,  # Disable auto commit to manually control offsets
        consumer_timeout_ms=10000,  # Stop waiting for new messages after 10 second of inactivity
        group_id="kafka_to_s3"
    )
    return consumer


def consumer_without_group(topic_name):
    # Initialize KafkaConsumer
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',  # Start reading at the earliest message
        enable_auto_commit=False,  # Disable auto commit to manually control offsets
        consumer_timeout_ms=10000,  # Stop waiting for new messages after 10 second of inactivity
    )
    return consumer


def read_messages_from_topic(topic_name: str):
    """
    Read a CSV file from an S3 bucket into a pandas DataFrame.

    :param topic_name: Kafka topic name
    """
    messages = []
    object_name = topic_name_to_s3_object_name(topic_name)

    if topic_name == topic_client_paid_loans_count:
        consumer = consumer_without_group(topic_name)
    else:
        consumer = consumer_with_group(topic_name)

    # Fetch all messages
    print(f"Reading messages from topic: {topic_name}")
    for message in consumer:
        messages.append(message)

    # Close the consumer
    consumer.close()
    print("Finished reading messages. Nr of messages", len(messages))

    if len(messages) > 0:
        # Convert messages into dataframe
        df_new = kafka_messages_into_dataframe(messages, topic_name)

        # Read existing data, concat and drop duplicates
        # Should be done in an improved way in production environment
        if topic_name != topic_client_paid_loans_count:
            df_old = read_csv_from_s3(object_name)
            if df_old is not False and topic_name != topic_client_paid_loans_count:
                df = pd.concat([df_new, df_old])
            else:
                df = df_new
        else:
            df = df_new

        save_df_to_s3(df, object_name)


print('Starting to read messages from kafka topics')
while True:
    for topic in topics:
        # for topic in [topic_client_paid_loans_count]:
        read_messages_from_topic(topic)
