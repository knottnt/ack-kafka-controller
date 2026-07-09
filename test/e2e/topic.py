# Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may
# not use this file except in compliance with the License. A copy of the
# License is located at
#
# 	 http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Utilities for working with Topic resources"""

import datetime
import time
import typing

import boto3
import pytest

DEFAULT_WAIT_UNTIL_TIMEOUT_SECONDS = 60 * 10
DEFAULT_WAIT_UNTIL_INTERVAL_SECONDS = 15
DEFAULT_WAIT_UNTIL_DELETED_TIMEOUT_SECONDS = 60 * 10
DEFAULT_WAIT_UNTIL_DELETED_INTERVAL_SECONDS = 15

TopicMatchFunc = typing.NewType(
    "TopicMatchFunc",
    typing.Callable[[dict], bool],
)


class StatusMatcher:
    def __init__(self, status):
        self.match_on = status

    def __call__(self, record: dict) -> bool:
        return record is not None and record.get("Status") == self.match_on


def status_matches(status: str) -> TopicMatchFunc:
    return StatusMatcher(status)


def wait_until(
    cluster_arn: str,
    topic_name: str,
    match_fn: TopicMatchFunc,
    timeout_seconds: int = DEFAULT_WAIT_UNTIL_TIMEOUT_SECONDS,
    interval_seconds: int = DEFAULT_WAIT_UNTIL_INTERVAL_SECONDS,
) -> None:
    """Waits until a Topic matches the supplied match function.

    Raises:
        pytest.fail upon timeout
    """
    now = datetime.datetime.now()
    timeout = now + datetime.timedelta(seconds=timeout_seconds)

    while not match_fn(get(cluster_arn, topic_name)):
        if datetime.datetime.now() >= timeout:
            pytest.fail("failed to match Topic before timeout")
        time.sleep(interval_seconds)


def wait_until_deleted(
    cluster_arn: str,
    topic_name: str,
    timeout_seconds: int = DEFAULT_WAIT_UNTIL_DELETED_TIMEOUT_SECONDS,
    interval_seconds: int = DEFAULT_WAIT_UNTIL_DELETED_INTERVAL_SECONDS,
) -> None:
    """Waits until a Topic is no longer returned from the MSK API.

    Raises:
        pytest.fail upon timeout
    """
    now = datetime.datetime.now()
    timeout = now + datetime.timedelta(seconds=timeout_seconds)

    while True:
        if datetime.datetime.now() >= timeout:
            pytest.fail(
                "Timed out waiting for Topic to be deleted in MSK API"
            )
        time.sleep(interval_seconds)

        latest = get(cluster_arn, topic_name)
        if latest is None:
            break


def get(cluster_arn, topic_name):
    """Returns a dict containing the Topic record with the supplied cluster ARN
    and topic name from the MSK API.

    If no such Topic exists, returns None.
    """
    c = boto3.client("kafka")
    try:
        resp = c.describe_topic(ClusterArn=cluster_arn, TopicName=topic_name)
        return resp
    except c.exceptions.NotFoundException:
        return None
