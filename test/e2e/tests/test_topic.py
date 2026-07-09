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

"""Integration tests for the MSK Topic resource"""

import base64
import json
import time

import pytest

from acktest.k8s import condition
from acktest.k8s import resource as k8s
from acktest.resources import random_suffix_name
from e2e import service_marker, CRD_GROUP, CRD_VERSION, load_resource
from e2e.bootstrap_resources import get_bootstrap_resources
from e2e.common.types import CLUSTER_RESOURCE_PLURAL, TOPIC_RESOURCE_PLURAL
from e2e.replacement_values import REPLACEMENT_VALUES
from e2e import cluster
from e2e import topic

CLUSTER_CREATE_WAIT_AFTER_SECONDS = 180
CLUSTER_DELETE_WAIT_SECONDS = 300
CREATE_WAIT_AFTER_SECONDS = 30
MODIFY_WAIT_AFTER_SECONDS = 30
DELETE_WAIT_SECONDS = 60
CHECK_STATUS_WAIT_SECONDS = 30


@pytest.fixture(scope="module")
def topic_cluster():
    """Provisions an MSK Cluster that Topics can be created against."""
    cluster_name = random_suffix_name("ack-topic-cluster", 24)

    resources = get_bootstrap_resources()
    vpc = resources.ClusterVPC
    subnet_id_1 = vpc.public_subnets.subnet_ids[0]
    subnet_id_2 = vpc.public_subnets.subnet_ids[1]

    replacements = REPLACEMENT_VALUES.copy()
    replacements["CLUSTER_NAME"] = cluster_name
    replacements["SUBNET_ID_1"] = subnet_id_1
    replacements["SUBNET_ID_2"] = subnet_id_2
    replacements["SECRET_ARN"] = resources.SCRAMSecret1.arn

    resource_data = load_resource(
        "cluster_simple",
        additional_replacements=replacements,
    )

    ref = k8s.CustomResourceReference(
        CRD_GROUP,
        CRD_VERSION,
        CLUSTER_RESOURCE_PLURAL,
        cluster_name,
        namespace="default",
    )
    k8s.create_custom_resource(ref, resource_data)
    cr = k8s.wait_resource_consumed_by_controller(ref)

    assert cr is not None
    assert k8s.get_resource_exists(ref)

    time.sleep(CLUSTER_CREATE_WAIT_AFTER_SECONDS)

    cr = k8s.get_resource(ref)
    cluster_arn = cr["status"]["ackResourceMetadata"]["arn"]

    cluster.wait_until(
        cluster_arn,
        cluster.state_matches("ACTIVE"),
    )

    yield (ref, cluster_arn)

    _, deleted = k8s.delete_custom_resource(
        ref,
        period_length=CLUSTER_DELETE_WAIT_SECONDS,
    )
    assert deleted
    cluster.wait_until_deleted(cluster_name)


@pytest.fixture(scope="module")
def simple_topic(topic_cluster):
    _, cluster_arn = topic_cluster
    topic_name = random_suffix_name("ack-topic", 24)

    replacements = REPLACEMENT_VALUES.copy()
    replacements["TOPIC_NAME"] = topic_name
    replacements["CLUSTER_ARN"] = cluster_arn

    resource_data = load_resource(
        "topic_simple",
        additional_replacements=replacements,
    )

    ref = k8s.CustomResourceReference(
        CRD_GROUP,
        CRD_VERSION,
        TOPIC_RESOURCE_PLURAL,
        topic_name,
        namespace="default",
    )
    k8s.create_custom_resource(ref, resource_data)
    cr = k8s.wait_resource_consumed_by_controller(ref)

    assert cr is not None
    assert k8s.get_resource_exists(ref)

    yield (ref, cr, cluster_arn, topic_name)

    try:
        _, deleted = k8s.delete_custom_resource(ref, DELETE_WAIT_SECONDS)
        assert deleted
        topic.wait_until_deleted(cluster_arn, topic_name)
    except Exception:
        pass


@service_marker
@pytest.mark.canary
class TestTopic:
    def test_crud(self, simple_topic):
        ref, _, cluster_arn, topic_name = simple_topic

        time.sleep(CREATE_WAIT_AFTER_SECONDS)

        topic.wait_until(
            cluster_arn,
            topic_name,
            topic.status_matches("ACTIVE"),
        )

        time.sleep(CHECK_STATUS_WAIT_SECONDS)
        condition.assert_synced(ref)

        cr = k8s.get_resource(ref)
        assert cr["status"]["status"] == "ACTIVE"

        latest = topic.get(cluster_arn, topic_name)
        assert latest is not None
        assert latest["PartitionCount"] == 1
        assert latest["ReplicationFactor"] == 2

        # Increase the partition count (partition increase is supported)
        updates = {
            "spec": {"partitionCount": 2},
        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        assert k8s.wait_on_condition(
            ref,
            "ACK.ResourceSynced",
            "True",
            wait_periods=MODIFY_WAIT_AFTER_SECONDS,
        )

        topic.wait_until(
            cluster_arn,
            topic_name,
            topic.status_matches("ACTIVE"),
        )

        latest = topic.get(cluster_arn, topic_name)
        assert latest is not None
        assert latest["PartitionCount"] == 2

        # Update a topic configuration property. Configs is intentionally
        # excluded from the generated string delta (DescribeTopic returns the
        # full effective config); the customPostCompare hook must still detect
        # this genuine change and reconcile it, and the update must NOT be
        # silently dropped. We also verify a Configs-only update does not
        # re-send the unchanged PartitionCount (which MSK would reject).
        new_retention_ms = "111111111"
        configs = base64.b64encode(
            json.dumps({"retention.ms": new_retention_ms}).encode()
        ).decode()
        updates = {
            "spec": {"configs": configs},
        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        assert k8s.wait_on_condition(
            ref,
            "ACK.ResourceSynced",
            "True",
            wait_periods=MODIFY_WAIT_AFTER_SECONDS,
        )

        # The server's effective configuration must reflect the override,
        # proving the UpdateTopic call was actually issued.
        latest = topic.get(cluster_arn, topic_name)
        assert latest is not None
        effective = json.loads(
            base64.b64decode(latest["Configs"]).decode()
        )
        assert effective.get("retention.ms") == new_retention_ms

        # delete the CR
        _, deleted = k8s.delete_custom_resource(ref, DELETE_WAIT_SECONDS)
        assert deleted

        topic.wait_until_deleted(cluster_arn, topic_name)

        latest = topic.get(cluster_arn, topic_name)
        assert latest is None
