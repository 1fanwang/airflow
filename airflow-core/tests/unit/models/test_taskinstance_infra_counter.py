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
"""AIP-97 POC: dedicated infra counter — max_tries stays pristine, a separate bounded counter tracks infra."""

from __future__ import annotations

from airflow.listeners.types import TaskFailureInfo
from airflow.models.taskinstance import _maybe_refund_infra_attempt

from tests_common.test_utils.config import conf_vars


class _FakeTI:
    def __init__(self, max_tries: int, infra_retry_count: int = 0):
        self.max_tries = max_tries
        self.infra_retry_count = infra_retry_count

    def __str__(self) -> str:
        return "<FakeTI>"


class _FakeTask:
    def __init__(self, retries):
        self.retries = retries


def _infra(reason: str = "Evicted") -> TaskFailureInfo:
    return TaskFailureInfo(source="infra", executor_kind="kubernetes", infra_reason=reason)


ENABLED = {("core", "infra_failure_refund_retries"): "True", ("core", "max_infra_refunds"): "3"}


class TestDedicatedInfraCounter:
    """The counter increments on infra; ``max_tries`` is never touched; the cap bounds it."""

    @conf_vars(ENABLED)
    def test_infra_bumps_counter_not_max_tries(self):
        ti, task = _FakeTI(max_tries=1), _FakeTask(retries=1)
        assert _maybe_refund_infra_attempt(task_instance=ti, task=task, failure_details=_infra()) is True
        assert ti.infra_retry_count == 1
        assert ti.max_tries == 1  # pristine — still == retries

    @conf_vars(ENABLED)
    def test_cap_bounds_the_counter_and_leaves_max_tries_pristine(self):
        # retries=1, cap=3 -> counter goes 1,2,3 then stops; max_tries never moves.
        ti, task = _FakeTI(max_tries=1), _FakeTask(retries=1)
        results = [
            _maybe_refund_infra_attempt(task_instance=ti, task=task, failure_details=_infra())
            for _ in range(5)
        ]
        assert results == [True, True, True, False, False]
        assert ti.infra_retry_count == 3
        assert ti.max_tries == 1  # never inflated, unlike the refund approach

    @conf_vars(ENABLED)
    def test_app_and_user_and_timeout_never_count(self):
        ti, task = _FakeTI(max_tries=1), _FakeTask(retries=1)
        assert _maybe_refund_infra_attempt(task_instance=ti, task=task, failure_details=None) is False
        for source in ("user", "timeout"):
            assert (
                _maybe_refund_infra_attempt(
                    task_instance=ti, task=task, failure_details=TaskFailureInfo(source=source)
                )
                is False
            )
        assert ti.infra_retry_count == 0
        assert ti.max_tries == 1

    @conf_vars({("core", "infra_failure_refund_retries"): "False"})
    def test_disabled_by_default_does_not_count(self):
        ti, task = _FakeTI(max_tries=1), _FakeTask(retries=1)
        assert _maybe_refund_infra_attempt(task_instance=ti, task=task, failure_details=_infra()) is False
        assert ti.infra_retry_count == 0

    @conf_vars(ENABLED)
    def test_none_retries_does_not_crash_or_count(self):
        ti, task = _FakeTI(max_tries=0), _FakeTask(retries=None)
        assert _maybe_refund_infra_attempt(task_instance=ti, task=task, failure_details=_infra()) is False
        assert ti.infra_retry_count == 0
