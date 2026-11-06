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

from typing import TYPE_CHECKING
from unittest import mock

import pytest
from sqlalchemy import select

from airflow._shared.state import TaskFailureKind
from airflow._shared.timezones import timezone
from airflow.models.taskinstance import (
    TaskInstance,
    _reschedule_infra_failure_if_eligible,
    clear_task_instances,
)
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.models.taskreschedule import TaskReschedule
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.config import conf_vars

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.orm import Session

    from tests_common.pytest_plugin import DagMaker

pytestmark = pytest.mark.db_test


def _running_ti(*, dag_maker: DagMaker, session: Session, retries: int = 0) -> TaskInstance:
    with dag_maker(dag_id="infra_continuation"):
        task = EmptyOperator(task_id="task", retries=retries)
    ti: TaskInstance | None = dag_maker.create_dagrun().get_task_instance(
        task_id=task.task_id, session=session
    )
    assert ti is not None
    ti.task = dag_maker.serialized_dag.get_task(task.task_id)
    ti.state = TaskInstanceState.RUNNING
    ti.try_number = 1
    ti.max_tries = retries
    ti.start_date = timezone.utcnow()
    ti.end_date = timezone.utcnow()
    session.flush()
    return ti


@pytest.mark.parametrize(
    ("cap", "failure_kind", "prior_continuations", "expected_metric"),
    [
        pytest.param(0, TaskFailureKind.INFRA, 0, None, id="disabled"),
        pytest.param(-1, TaskFailureKind.INFRA, 0, None, id="negative_cap"),
        pytest.param(1, None, 0, None, id="unclassified"),
        pytest.param(1, TaskFailureKind.APPLICATION, 0, None, id="application"),
        pytest.param(1, TaskFailureKind.TIMEOUT, 0, None, id="timeout"),
        pytest.param(1, TaskFailureKind.MANUAL, 0, None, id="manual"),
        pytest.param(1, TaskFailureKind.INFRA, 0, "ti_infra_retry_granted", id="granted"),
        pytest.param(3, TaskFailureKind.INFRA, 2, "ti_infra_retry_granted", id="last_allowed"),
        pytest.param(1, TaskFailureKind.INFRA, 1, "ti_infra_retry_denied", id="exhausted"),
        pytest.param(3, TaskFailureKind.INFRA, 5, "ti_infra_retry_denied", id="cap_lowered"),
    ],
)
@mock.patch("airflow.models.taskinstance.stats.incr", autospec=True)
def test_policy_outcome_metrics(
    mock_incr: mock.MagicMock,
    cap: int,
    failure_kind: TaskFailureKind | None,
    prior_continuations: int,
    expected_metric: str | None,
    dag_maker: DagMaker,
    session: Session,
) -> None:
    ti: TaskInstance = _running_ti(dag_maker=dag_maker, session=session)
    ti.infra_reschedule_count = prior_continuations
    mock_incr.reset_mock()

    with conf_vars({("core", "max_infra_retries"): str(cap)}):
        granted: bool = _reschedule_infra_failure_if_eligible(
            task_instance=ti,
            task=ti.task,
            failure_kind=failure_kind,
            session=session,
            reason="PreemptionByScheduler",
        )

    assert granted is (expected_metric == "ti_infra_retry_granted")
    assert ti.infra_reschedule_count == prior_continuations + int(granted)
    if expected_metric is None:
        mock_incr.assert_not_called()
    else:
        mock_incr.assert_called_once_with(expected_metric, tags=ti.stats_tags)


@conf_vars({("core", "max_infra_retries"): "2"})
def test_grant_continues_the_same_try_with_a_new_execution(dag_maker: DagMaker, session: Session) -> None:
    ti: TaskInstance = _running_ti(dag_maker=dag_maker, session=session, retries=1)
    interrupted_id: UUID = ti.id
    ti.external_executor_id = "pod-1"
    ti.pid = 1234

    assert _reschedule_infra_failure_if_eligible(
        task_instance=ti, task=ti.task, failure_kind=TaskFailureKind.INFRA, session=session
    )
    session.flush()

    assert (ti.state, ti.try_number, ti.max_tries) == (TaskInstanceState.UP_FOR_RESCHEDULE, 1, 1)
    assert (ti.infra_reschedule_count, ti.infra_continuation_number) == (1, 1)
    assert ti.id != interrupted_id
    assert (ti.external_executor_id, ti.pid) == (None, None)
    history: TaskInstanceHistory = session.scalars(
        select(TaskInstanceHistory).where(TaskInstanceHistory.task_instance_id == interrupted_id)
    ).one()
    assert (history.try_number, history.infra_continuation_number, history.state) == (
        1,
        0,
        TaskInstanceState.UP_FOR_RESCHEDULE,
    )
    assert session.scalars(select(TaskReschedule.ti_id)).all() == [ti.id]


@pytest.mark.parametrize(
    ("state", "try_number", "has_task"),
    [
        (TaskInstanceState.RUNNING, 0, True),
        (TaskInstanceState.RUNNING, -1, True),
        (TaskInstanceState.RUNNING, 1, False),
        (TaskInstanceState.QUEUED, 1, True),
        (TaskInstanceState.SCHEDULED, 1, True),
        (TaskInstanceState.RESTARTING, 1, True),
        (TaskInstanceState.UP_FOR_RESCHEDULE, 1, True),
        (TaskInstanceState.UP_FOR_RETRY, 1, True),
        (TaskInstanceState.FAILED, 1, True),
        (TaskInstanceState.SUCCESS, 1, True),
    ],
)
@conf_vars({("core", "max_infra_retries"): "3"})
@mock.patch("airflow.models.taskinstance.stats.incr", autospec=True)
def test_unstarted_or_handled_attempt_has_no_policy_decision(
    mock_incr: mock.MagicMock,
    state: TaskInstanceState,
    try_number: int,
    has_task: bool,
    dag_maker: DagMaker,
    session: Session,
) -> None:
    ti: TaskInstance = _running_ti(dag_maker=dag_maker, session=session)
    ti.state = state
    ti.try_number = try_number
    mock_incr.reset_mock()

    assert not _reschedule_infra_failure_if_eligible(
        task_instance=ti,
        task=ti.task if has_task else None,
        failure_kind=TaskFailureKind.INFRA,
        session=session,
    )
    assert (ti.state, ti.infra_reschedule_count) == (state, 0)
    mock_incr.assert_not_called()


@conf_vars({("core", "max_infra_retries"): "1"})
def test_clear_resets_the_allowance(dag_maker: DagMaker, session: Session) -> None:
    ti: TaskInstance = _running_ti(dag_maker=dag_maker, session=session)
    assert _reschedule_infra_failure_if_eligible(
        task_instance=ti, task=ti.task, failure_kind=TaskFailureKind.INFRA, session=session
    )
    ti.state = TaskInstanceState.RUNNING
    assert not _reschedule_infra_failure_if_eligible(
        task_instance=ti, task=ti.task, failure_kind=TaskFailureKind.INFRA, session=session
    )
    ti.state = TaskInstanceState.FAILED
    session.flush()

    clear_task_instances(tis=[ti], session=session)
    ti.state = TaskInstanceState.RUNNING
    ti.try_number = 2
    max_tries_after_clear: int = ti.max_tries

    assert (ti.infra_reschedule_count, ti.infra_continuation_number) == (0, 0)
    assert _reschedule_infra_failure_if_eligible(
        task_instance=ti, task=ti.task, failure_kind=TaskFailureKind.INFRA, session=session
    )
    assert (ti.try_number, ti.max_tries) == (2, max_tries_after_clear)
