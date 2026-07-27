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

from airflow._shared.state import TaskFailureKind
from airflow.models.taskinstance import _maybe_refund_infra_attempt

from tests_common.test_utils.config import conf_vars

MAX_REFUNDS = 3
INFRA_REASON = "Evicted"
ENABLED = {
    ("core", "infra_failure_refund_retries"): "True",
    ("core", "max_infra_refunds"): str(MAX_REFUNDS),
}
DISABLED = {("core", "infra_failure_refund_retries"): "False"}


class _FakeTI:
    def __init__(self, max_tries: int) -> None:
        self.max_tries = max_tries
        self.infra_reason = INFRA_REASON

    def __str__(self) -> str:
        return "<FakeTI>"


class _FakeTask:
    def __init__(self, retries: int | None) -> None:
        self.retries = retries


class TestMaybeRefundInfraAttempt:
    """The single safety gate: only ``TaskFailureKind.INFRA``, with the flag on and under the cap, refunds."""

    @conf_vars(ENABLED)
    def test_infra_failure_refunds_one_attempt(self) -> None:
        ti, task = _FakeTI(max_tries=1), _FakeTask(retries=1)
        assert _maybe_refund_infra_attempt(task_instance=ti, task=task, failure_kind=TaskFailureKind.INFRA)
        assert ti.max_tries == 2

    @conf_vars(ENABLED)
    @pytest.mark.parametrize(
        "failure_kind",
        [None, TaskFailureKind.APPLICATION, TaskFailureKind.MANUAL, TaskFailureKind.TIMEOUT],
        ids=["unclassified", "application", "manual", "timeout"],
    )
    def test_non_infra_kind_does_not_refund(self, failure_kind: TaskFailureKind | None) -> None:
        # Only INFRA refunds; a real bug (application/timeout), a manual stop, or an unclassified
        # (None) worker exception must all spend the user's retry.
        ti, task = _FakeTI(max_tries=1), _FakeTask(retries=1)
        assert not _maybe_refund_infra_attempt(task_instance=ti, task=task, failure_kind=failure_kind)
        assert ti.max_tries == 1

    @conf_vars(ENABLED)
    def test_none_retries_does_not_crash_or_refund(self) -> None:
        # retries=None (unset) must not raise TypeError in the cap math; treated as no budget.
        ti, task = _FakeTI(max_tries=0), _FakeTask(retries=None)
        assert not _maybe_refund_infra_attempt(
            task_instance=ti, task=task, failure_kind=TaskFailureKind.INFRA
        )
        assert ti.max_tries == 0

    @conf_vars(DISABLED)
    def test_disabled_by_default_does_not_refund(self) -> None:
        ti, task = _FakeTI(max_tries=1), _FakeTask(retries=1)
        assert not _maybe_refund_infra_attempt(
            task_instance=ti, task=task, failure_kind=TaskFailureKind.INFRA
        )
        assert ti.max_tries == 1

    @conf_vars(ENABLED)
    def test_cap_bounds_the_refunds(self) -> None:
        # retries=1 => refunds allowed only while (max_tries - retries) < MAX_REFUNDS.
        ti, task = _FakeTI(max_tries=1), _FakeTask(retries=1)
        outcomes = [
            _maybe_refund_infra_attempt(task_instance=ti, task=task, failure_kind=TaskFailureKind.INFRA)
            for _ in range(MAX_REFUNDS + 2)
        ]
        assert outcomes == [True] * MAX_REFUNDS + [False, False]
        assert ti.max_tries == 1 + MAX_REFUNDS  # user's one try plus the refunds, then capped

    @conf_vars(ENABLED)
    def test_cap_reached_is_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        # Once the cap is hit the refund declines, but says why, so an infra failure that
        # spends a real retry is never silent in the logs even with the feature on.
        at_cap = 1 + MAX_REFUNDS  # max_tries where (max_tries - retries) == MAX_REFUNDS
        ti, task = _FakeTI(max_tries=at_cap), _FakeTask(retries=1)
        with caplog.at_level(logging.INFO, logger="airflow.models.taskinstance"):
            assert not _maybe_refund_infra_attempt(
                task_instance=ti, task=task, failure_kind=TaskFailureKind.INFRA
            )
        assert f"cap ({MAX_REFUNDS}) reached" in caplog.text
        assert INFRA_REASON in caplog.text
        assert ti.max_tries == at_cap  # unchanged
