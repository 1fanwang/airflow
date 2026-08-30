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

from airflow._shared.state import TaskFailureKind
from airflow.models.taskinstance import TaskInstance, _maybe_use_infra_retry, clear_task_instances
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test

INFRA_RETRIES = {("core", "max_infra_retries"): "3"}


def _use_infra_retry(*, ti, task, failure_kind) -> bool:
    return _maybe_use_infra_retry(
        task_instance=ti,
        task=task,
        failure_kind=failure_kind,
        reason="Evicted",
    )


class TestMaybeUseInfraRetry:
    @pytest.mark.parametrize(
        "failure_kind",
        [None, TaskFailureKind.APPLICATION, TaskFailureKind.MANUAL, TaskFailureKind.TIMEOUT],
    )
    @conf_vars(INFRA_RETRIES)
    def test_only_infra_uses_extra_attempts(self, failure_kind, dag_maker, session):
        with dag_maker(dag_id=f"failure_kind_{failure_kind or 'none'}"):
            task = EmptyOperator(task_id="task", retries=0)
        ti = dag_maker.create_dagrun().get_task_instance(task.task_id, session=session)
        ti.task = task

        assert not _use_infra_retry(
            ti=ti,
            task=task,
            failure_kind=failure_kind,
        )
        assert ti.max_tries == 0

    @conf_vars(INFRA_RETRIES)
    def test_infra_attempts_are_inferred_from_max_tries(self, dag_maker, session):
        with dag_maker(dag_id="infra_budget"):
            task = EmptyOperator(task_id="task", retries=0)
        ti = dag_maker.create_dagrun().get_task_instance(task.task_id, session=session)
        ti.task = task

        assert [
            _use_infra_retry(
                ti=ti,
                task=task,
                failure_kind=TaskFailureKind.INFRA,
            )
            for _ in range(5)
        ] == [True, True, True, False, False]
        assert ti.max_tries == 3

    def test_zero_budget_preserves_current_behavior(self, dag_maker, session):
        with dag_maker(dag_id="zero_infra_budget"):
            task = EmptyOperator(task_id="task", retries=1)
        ti = dag_maker.create_dagrun().get_task_instance(task.task_id, session=session)
        ti.task = task

        assert not _use_infra_retry(
            ti=ti,
            task=task,
            failure_kind=TaskFailureKind.INFRA,
        )
        assert ti.max_tries == 1

    @conf_vars({("core", "max_infra_retries"): "1"})
    def test_none_max_tries_uses_zero(self, dag_maker, session):
        with dag_maker(dag_id="none_max_tries"):
            task = EmptyOperator(task_id="task", retries=0)
        ti = dag_maker.create_dagrun().get_task_instance(task.task_id, session=session)
        ti.task = task
        ti.max_tries = None

        assert _use_infra_retry(
            ti=ti,
            task=task,
            failure_kind=TaskFailureKind.INFRA,
        )
        assert ti.max_tries == 1

    @conf_vars({("core", "max_infra_retries"): "1"})
    def test_retry_increase_cannot_reopen_budget(self, dag_maker, session):
        with dag_maker(dag_id="retries_changed"):
            task = EmptyOperator(task_id="task", retries=0)
        ti = dag_maker.create_dagrun().get_task_instance(task.task_id, session=session)
        ti.task = task
        ti.try_number = 1

        assert _use_infra_retry(
            ti=ti,
            task=task,
            failure_kind=TaskFailureKind.INFRA,
        )

        task.retries = 2
        ti.try_number = 2

        assert not _use_infra_retry(
            ti=ti,
            task=task,
            failure_kind=TaskFailureKind.INFRA,
        )
        assert ti.max_tries == 1

    @conf_vars(INFRA_RETRIES)
    def test_task_clear_can_exhaust_inferred_budget(self, dag_maker, session):
        with dag_maker(dag_id="infra_budget_clear"):
            task = EmptyOperator(task_id="task", retries=2)
        ti = dag_maker.create_dagrun().get_task_instance(task.task_id, session=session)
        ti.task = task

        for _ in range(2):
            assert _use_infra_retry(
                ti=ti,
                task=task,
                failure_kind=TaskFailureKind.INFRA,
            )
        ti.state = TaskInstanceState.FAILED
        ti.try_number = 3
        session.flush()

        clear_task_instances([ti], session=session)
        ti.task = task

        assert ti.max_tries == 5
        assert not _use_infra_retry(
            ti=ti,
            task=task,
            failure_kind=TaskFailureKind.INFRA,
        )


class TestIsEligibleToRetryUsesMaxTries:
    @staticmethod
    def _eligible(*, max_tries: int | None, try_number: int, state: TaskInstanceState | None = None) -> bool:
        stub = SimpleNamespace(state=state, max_tries=max_tries, try_number=try_number)
        return TaskInstance.is_eligible_to_retry(stub)  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        ("max_tries", "try_number", "expected"),
        [
            (None, 1, False),
            (0, 1, False),
            (1, 1, True),
            (2, 2, True),
            (2, 3, False),
        ],
    )
    def test_eligibility_uses_effective_max_tries(self, max_tries, try_number, expected):
        assert self._eligible(max_tries=max_tries, try_number=try_number) is expected

    def test_restarting_is_always_eligible(self):
        assert (
            self._eligible(
                max_tries=0,
                try_number=9,
                state=TaskInstanceState.RESTARTING,
            )
            is True
        )
