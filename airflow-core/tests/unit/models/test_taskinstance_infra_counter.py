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

from types import SimpleNamespace

import pytest

from airflow.models.taskinstance import TaskInstance, _maybe_grant_infra_retry

from tests_common.test_utils.config import conf_vars


class _FakeTI:
    def __init__(self, infra_retry_count: int = 0):
        self.infra_retry_count = infra_retry_count
        self.infra_reason = "Evicted"

    def __str__(self) -> str:
        return "<FakeTI>"


class _FakeTask:
    def __init__(self, retries: int):
        self.retries = retries


ENABLED = {("core", "infra_failure_refund_retries"): "True", ("core", "max_infra_refunds"): "3"}


class TestMaybeGrantInfraRetry:
    """The counter alternative: an infra failure bumps infra_retry_count, never max_tries."""

    @conf_vars(ENABLED)
    def test_infra_increments_counter(self):
        ti, task = _FakeTI(), _FakeTask(retries=1)
        assert _maybe_grant_infra_retry(task_instance=ti, task=task, failure_kind="infra") is True
        assert ti.infra_retry_count == 1

    @conf_vars(ENABLED)
    def test_retries_zero_still_granted(self):
        # The whole point of the dedicated counter: it is NOT gated on task.retries, so a
        # retries=0 task — which bumping max_tries can never rescue — still gets an infra retry.
        ti, task = _FakeTI(), _FakeTask(retries=0)
        assert _maybe_grant_infra_retry(task_instance=ti, task=task, failure_kind="infra") is True
        assert ti.infra_retry_count == 1

    @conf_vars(ENABLED)
    @pytest.mark.parametrize("failure_kind", [None, "application", "manual", "timeout"])
    def test_non_infra_does_not_increment(self, failure_kind):
        ti, task = _FakeTI(), _FakeTask(retries=1)
        assert _maybe_grant_infra_retry(task_instance=ti, task=task, failure_kind=failure_kind) is False
        assert ti.infra_retry_count == 0

    @conf_vars({("core", "infra_failure_refund_retries"): "False"})
    def test_disabled_does_not_increment(self):
        ti, task = _FakeTI(), _FakeTask(retries=1)
        assert _maybe_grant_infra_retry(task_instance=ti, task=task, failure_kind="infra") is False
        assert ti.infra_retry_count == 0

    @conf_vars(ENABLED)
    def test_cap_bounds_the_counter(self):
        ti, task = _FakeTI(), _FakeTask(retries=1)
        results = [
            _maybe_grant_infra_retry(task_instance=ti, task=task, failure_kind="infra") for _ in range(5)
        ]
        assert results == [True, True, True, False, False]
        assert ti.infra_retry_count == 3  # capped at max_infra_refunds


def _eligible(retries: int, try_number: int, max_tries: int, infra_retry_count: int) -> bool:
    fake = SimpleNamespace(
        state=None,
        task=SimpleNamespace(retries=retries),
        try_number=try_number,
        max_tries=max_tries,
        infra_retry_count=infra_retry_count,
    )
    return TaskInstance.is_eligible_to_retry(fake)


class TestEligibilityWithCounter:
    """is_eligible_to_retry adds an infra clause that is independent of the user's retries."""

    def test_retries_zero_gets_exactly_the_granted_infra_attempts(self):
        # retries=0 normally never retries; each infra grant extends the ceiling by one.
        assert _eligible(retries=0, try_number=1, max_tries=0, infra_retry_count=0) is False
        assert _eligible(retries=0, try_number=1, max_tries=0, infra_retry_count=1) is True
        assert _eligible(retries=0, try_number=2, max_tries=0, infra_retry_count=1) is False
        assert _eligible(retries=0, try_number=2, max_tries=0, infra_retry_count=2) is True

    def test_infra_counter_stacks_on_top_of_the_normal_budget(self):
        # retries=2 (max_tries=2): the counter adds attempts beyond the normal ceiling.
        assert _eligible(retries=2, try_number=3, max_tries=2, infra_retry_count=0) is False
        assert _eligible(retries=2, try_number=3, max_tries=2, infra_retry_count=1) is True
        assert _eligible(retries=2, try_number=4, max_tries=2, infra_retry_count=1) is False
