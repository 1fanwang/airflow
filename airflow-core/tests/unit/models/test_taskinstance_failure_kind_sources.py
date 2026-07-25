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
"""
AIP-97 SIGTERM-source classification safety.

A SIGTERM to a running task is ambiguous: a user mark-failed and an infra
eviction both deliver one. The extra infra retry must never fire for a
user-initiated stop. The single gate is ``failure_kind``: only ``INFRA`` is
granted one. These tests pin that the kind each termination source carries
maps to the right decision, independent of the signal.
"""

from __future__ import annotations

import pytest

from airflow._shared.state import TaskFailureKind
from airflow.models.taskinstance import _maybe_grant_infra_retry

from tests_common.test_utils.config import conf_vars

ENABLED = {("core", "infra_failure_refund_retries"): "True", ("core", "max_infra_refunds"): "3"}


class _TI:
    def __init__(self):
        self.infra_retry_count = 0
        self.infra_reason = None

    def __str__(self):
        return "<TI>"


class _Task:
    retries = 1


@pytest.mark.parametrize(
    ("source", "failure_kind", "should_grant"),
    [
        # non-infra causes — never granted an infra retry, whatever signal did the killing
        ("mark_task_failed", TaskFailureKind.MANUAL, False),
        ("mark_dagrun_failed", TaskFailureKind.MANUAL, False),
        ("app_exception", TaskFailureKind.APPLICATION, False),
        ("own_limit_oom", TaskFailureKind.APPLICATION, False),
        ("execution_timeout", TaskFailureKind.TIMEOUT, False),
        ("unclassified", None, False),
        # infrastructure disruption — the only granted cause
        ("pod_evicted", TaskFailureKind.INFRA, True),
    ],
)
@conf_vars(ENABLED)
def test_only_infra_source_grants_retry(source, failure_kind, should_grant):
    ti = _TI()
    granted = _maybe_grant_infra_retry(task_instance=ti, task=_Task(), failure_kind=failure_kind)
    assert granted is should_grant, source
    assert ti.infra_retry_count == (1 if should_grant else 0)
