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

import pytest

from airflow.models.taskinstance import _maybe_refund_infra_attempt

from tests_common.test_utils.config import conf_vars


class _FakeTI:
    def __init__(self, max_tries: int):
        self.max_tries = max_tries
        self.infra_reason = "Evicted"

    def __str__(self) -> str:
        return "<FakeTI>"


class _FakeTask:
    def __init__(self, retries: int):
        self.retries = retries


ENABLED = {("core", "infra_failure_refund_retries"): "True", ("core", "max_infra_refunds"): "3"}


class TestMaybeRefundInfraAttempt:
    """The single safety gate: only ``failure_kind == "infra"``, with the flag on and under the cap, refunds."""

    @conf_vars(ENABLED)
    def test_infra_failure_refunds_one_attempt(self):
        ti, task = _FakeTI(max_tries=1), _FakeTask(retries=1)
        assert _maybe_refund_infra_attempt(task_instance=ti, task=task, failure_kind="infra") is True
        assert ti.max_tries == 2

    @conf_vars(ENABLED)
    def test_app_failure_does_not_refund(self):
        # failure_kind=None is the ordinary worker-exception path — a real bug must spend the budget.
        ti, task = _FakeTI(max_tries=1), _FakeTask(retries=1)
        assert _maybe_refund_infra_attempt(task_instance=ti, task=task, failure_kind=None) is False
        assert ti.max_tries == 1

    @conf_vars(ENABLED)
    @pytest.mark.parametrize("failure_kind", ["user", "timeout"])
    def test_non_infra_kind_does_not_refund(self, failure_kind):
        ti, task = _FakeTI(max_tries=1), _FakeTask(retries=1)
        assert _maybe_refund_infra_attempt(task_instance=ti, task=task, failure_kind=failure_kind) is False
        assert ti.max_tries == 1

    @conf_vars(ENABLED)
    def test_none_retries_does_not_crash_or_refund(self):
        # retries=None (unset) must not raise TypeError in the cap math; treated as no budget.
        ti, task = _FakeTI(max_tries=0), _FakeTask(retries=None)
        assert _maybe_refund_infra_attempt(task_instance=ti, task=task, failure_kind="infra") is False
        assert ti.max_tries == 0

    @conf_vars({("core", "infra_failure_refund_retries"): "False"})
    def test_disabled_by_default_does_not_refund(self):
        ti, task = _FakeTI(max_tries=1), _FakeTask(retries=1)
        assert _maybe_refund_infra_attempt(task_instance=ti, task=task, failure_kind="infra") is False
        assert ti.max_tries == 1

    @conf_vars(ENABLED)
    def test_cap_bounds_the_refunds(self):
        # retries=1, cap=3 → refunds allowed only while (max_tries - retries) < 3, i.e. max_tries in {1,2,3}.
        ti, task = _FakeTI(max_tries=1), _FakeTask(retries=1)
        assert [
            _maybe_refund_infra_attempt(task_instance=ti, task=task, failure_kind="infra")
            for _ in range(5)
        ] == [
            True,
            True,
            True,
            False,
            False,
        ]
        assert ti.max_tries == 4  # 1 + three refunds, then capped
