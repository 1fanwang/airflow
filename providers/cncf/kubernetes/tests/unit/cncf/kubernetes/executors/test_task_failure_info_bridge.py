# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import logging

import pytest
from kubernetes import client as k8s

from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils import (
    collect_pod_failure_details,
    to_task_failure_info,
)


class TestToTaskFailureInfo:
    """The K8s bridge maps a pod/container failure to a source the retry decision can trust."""

    @pytest.mark.parametrize(
        ("pod_reason", "container_reason", "expected_source"),
        [
            # Node/platform ended the pod -> infra (earns a refund).
            ("Evicted", None, "infra"),
            ("Preempting", None, "infra"),
            ("NodeShutdown", None, "infra"),
            ("NodeLost", None, "infra"),
            ("DisruptionTarget", None, "infra"),
            ("TerminationByKubelet", None, "infra"),
            # Container ended on its own -> user (no refund). The key one: an OOM against
            # the container's OWN limit is the app's memory problem, not an infra disruption.
            (None, "OOMKilled", "user"),
            (None, "Error", "user"),
            (None, "ContainerCannotRun", "user"),
            # A node eviction that also shows a container OOM is still infra (the node acted).
            ("Evicted", "OOMKilled", "infra"),
        ],
    )
    def test_source_classification(self, pod_reason, container_reason, expected_source):
        details = {"pod_status": "Failed", "pod_reason": pod_reason, "container_reason": container_reason}
        tfi = to_task_failure_info(details)
        assert tfi is not None
        assert tfi.source == expected_source
        assert tfi.executor_kind == "kubernetes"
        assert tfi.infra_reason == (pod_reason or container_reason)
        assert tfi.infra_metadata == details

    def test_none_when_nothing_to_classify(self):
        assert to_task_failure_info(None) is None
        assert to_task_failure_info({}) is None

    def test_end_to_end_oomkilled_pod_is_user(self):
        # Mirrors the live kind result: a real OOMKilled container (exit 137) flows through
        # collect_pod_failure_details -> to_task_failure_info and classifies as user, not infra,
        # so an app OOM does not earn an infra refund.
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name="aip97-oom"),
            status=k8s.V1PodStatus(
                phase="Failed",
                container_statuses=[
                    k8s.V1ContainerStatus(
                        name="base",
                        image="python:3.11-slim",
                        image_id="",
                        ready=False,
                        restart_count=0,
                        state=k8s.V1ContainerState(
                            terminated=k8s.V1ContainerStateTerminated(reason="OOMKilled", exit_code=137)
                        ),
                    )
                ],
            ),
        )
        details = collect_pod_failure_details(pod, logging.getLogger("test"))
        assert details is not None
        assert details["container_reason"] == "OOMKilled"
        assert details["exit_code"] == 137

        tfi = to_task_failure_info(details)
        assert tfi.source == "user"
        assert tfi.infra_reason == "OOMKilled"


class TestExecutorFailureInfoSeam:
    """The executor stashes a TaskFailureInfo; the scheduler reads it once (AIP-97 wiring)."""

    def test_base_executor_seam_round_trips_once(self):
        from airflow.executors.local_executor import LocalExecutor

        ex = LocalExecutor()
        key = ("dag", "task", "run", 1, -1)
        tfi = to_task_failure_info({"pod_status": "Failed", "pod_reason": "Evicted"})
        ex.task_failure_info[key] = tfi
        # first read returns it, and clears it so a later event can't reuse stale context
        assert ex.get_task_failure_info(key) is tfi
        assert ex.get_task_failure_info(key) is None
