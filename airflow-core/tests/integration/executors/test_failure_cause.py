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

from airflow._shared.state import TaskFailureKind
from airflow.callbacks.callback_requests import EmailRequest, TaskCallbackRequest
from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.listeners import hookimpl
from airflow.models.dagrun import DagRunState
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.session import create_session
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.mock_executor import MockExecutor

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.orm import Session

    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstancekey import TaskInstanceKey

    from tests_common.pytest_plugin import DagMaker

pytestmark = pytest.mark.db_test


@pytest.mark.parametrize(
    ("cap", "retries", "max_tries", "try_number", "expected_state", "expected_max_tries"),
    [
        (0, 0, 0, 1, TaskInstanceState.FAILED, 0),
        (0, 0, 3, 1, TaskInstanceState.FAILED, 3),
        (1, 0, 3, 1, TaskInstanceState.FAILED, 3),
        (1, 0, 0, 1, TaskInstanceState.UP_FOR_RETRY, 1),
        (3, 0, 0, 2, TaskInstanceState.UP_FOR_RETRY, 1),
        (1, 0, 1, 2, TaskInstanceState.FAILED, 1),
        (1, 2, 2, 2, TaskInstanceState.UP_FOR_RETRY, 2),
    ],
)
def test_infrastructure_retry_precedes_all_failure_consumers(
    cap: int,
    retries: int,
    max_tries: int,
    try_number: int,
    expected_state: TaskInstanceState,
    expected_max_tries: int,
    dag_maker: DagMaker,
    listener_manager: Callable[[object], None],
    session: Session,
) -> None:
    received: list[tuple[str | None, int, TaskFailureKind | None, str | None]] = []

    class FailureListener:
        @hookimpl
        def on_task_instance_failed(
            self,
            task_instance: TaskInstance,
            failure_kind: TaskFailureKind | None,
            reason: str | None,
        ) -> None:
            received.append((task_instance.state, task_instance.max_tries, failure_kind, reason))

    listener_manager(FailureListener())
    with dag_maker(dag_id="infra_retry_consumers", fileloc="/infra_retry/"):
        task = EmptyOperator(
            task_id="task",
            retries=retries,
            on_retry_callback=lambda context: None,
            on_failure_callback=lambda context: None,
            email="notification@example.invalid",
            email_on_retry=True,
            email_on_failure=True,
        )
    ti: TaskInstance | None = dag_maker.create_dagrun(state=DagRunState.RUNNING).get_task_instance(
        task_id=task.task_id, session=session
    )
    assert ti is not None
    ti.state = TaskInstanceState.RUNNING
    ti.try_number = try_number
    ti.max_tries = max_tries
    ti.queued_by_job_id = 1
    session.flush()
    executor = MockExecutor(do_update=False)
    runner = SchedulerJobRunner(job=Job(), executors=[executor])
    executor.fail(key=ti.key, failure_kind=TaskFailureKind.INFRA, reason="PreemptionByScheduler")

    with conf_vars({("core", "max_infra_retries"): str(cap)}):
        SchedulerJobRunner.process_executor_events(
            executor=executor,
            job_id=1,
            scheduler_dag_bag=runner.scheduler_dag_bag,
            session=session,
        )
    ti.refresh_from_db(session=session)

    assert (ti.state, ti.max_tries) == (expected_state, expected_max_tries)
    assert received == [(expected_state, expected_max_tries, TaskFailureKind.INFRA, "PreemptionByScheduler")]
    assert isinstance(executor.callback_sink, mock.MagicMock)
    callback: TaskCallbackRequest = executor.callback_sink.send.call_args_list[0].args[0]
    email: EmailRequest = executor.callback_sink.send.call_args_list[1].args[0]
    assert executor.callback_sink.send.call_count == 2
    assert isinstance(callback, TaskCallbackRequest)
    assert callback.task_callback_type == expected_state
    assert callback.context_from_server is not None
    assert callback.context_from_server.max_tries == expected_max_tries
    assert isinstance(email, EmailRequest)
    assert email.email_type == ("retry" if expected_state == TaskInstanceState.UP_FOR_RETRY else "failure")
    assert email.context_from_server.max_tries == expected_max_tries


@pytest.mark.parametrize("stale", [False, True], ids=["duplicate", "earlier_attempt"])
@conf_vars({("core", "max_infra_retries"): "3"})
@mock.patch("airflow.models.taskinstance.stats.incr", autospec=True)
def test_executor_event_does_not_grant_a_second_replacement(
    mock_incr: mock.MagicMock, stale: bool, dag_maker: DagMaker, session: Session
) -> None:
    with dag_maker(dag_id="infra_duplicate"):
        task = EmptyOperator(task_id="task", retries=0)
    ti: TaskInstance | None = dag_maker.create_dagrun(state=DagRunState.RUNNING).get_task_instance(
        task_id=task.task_id, session=session
    )
    assert ti is not None
    ti.state = TaskInstanceState.RUNNING
    ti.try_number = 1
    ti.queued_by_job_id = 1
    session.flush()
    key: TaskInstanceKey = ti.key
    executor = MockExecutor(do_update=False)
    runner = SchedulerJobRunner(job=Job(), executors=[executor])
    executor.fail(key=key, failure_kind=TaskFailureKind.INFRA)
    SchedulerJobRunner.process_executor_events(
        executor=executor, job_id=1, scheduler_dag_bag=runner.scheduler_dag_bag, session=session
    )
    ti.refresh_from_db(session=session)
    assert (ti.state, ti.max_tries) == (TaskInstanceState.UP_FOR_RETRY, 1)
    if stale:
        ti.state = TaskInstanceState.RUNNING
        ti.try_number = 2
        session.flush()
    mock_incr.reset_mock()
    executor.fail(key=key, failure_kind=TaskFailureKind.INFRA)

    SchedulerJobRunner.process_executor_events(
        executor=executor, job_id=1, scheduler_dag_bag=runner.scheduler_dag_bag, session=session
    )
    ti.refresh_from_db(session=session)

    assert ti.max_tries == 1
    assert ti.state == (TaskInstanceState.RUNNING if stale else TaskInstanceState.UP_FOR_RETRY)
    assert not any(call.args[0].startswith("ti_infra_retry_") for call in mock_incr.call_args_list)


@pytest.mark.backend("postgres")
@pytest.mark.parametrize("terminal_state", [TaskInstanceState.FAILED, TaskInstanceState.SUCCESS])
@conf_vars({("core", "max_infra_retries"): "1"})
def test_event_batch_preserves_a_manual_terminal_state_after_an_earlier_commit(
    terminal_state: TaskInstanceState, dag_maker: DagMaker, session: Session
) -> None:
    with dag_maker(dag_id="failure_batch_manual_stop"):
        for task_id in ("first", "second"):
            EmptyOperator(
                task_id=task_id,
                retries=0,
                on_retry_callback=lambda context: None,
                on_failure_callback=lambda context: None,
            )
    dag_run: DagRun = dag_maker.create_dagrun(state=DagRunState.RUNNING)
    task_instances: list[TaskInstance] = dag_run.get_task_instances(session=session)
    executor = MockExecutor(do_update=False)
    runner = SchedulerJobRunner(job=Job(), executors=[executor])
    assert isinstance(executor.callback_sink, mock.MagicMock)
    for ti in task_instances:
        ti.state = TaskInstanceState.RUNNING
        ti.try_number = 1
        ti.queued_by_job_id = 1
        executor.fail(key=ti.key, failure_kind=TaskFailureKind.INFRA, reason="PreemptionByScheduler")
    session.commit()
    stopped_tasks: list[str] = []

    def stop_other_task(request: TaskCallbackRequest) -> None:
        if stopped_tasks:
            return
        other_task_id: str = next(ti.task_id for ti in task_instances if ti.task_id != request.ti.task_id)
        with create_session(scoped=False) as other_session:
            assert other_session is not session
            stopped_ti: TaskInstance | None = TaskInstance.get_task_instance(
                dag_id=dag_run.dag_id,
                run_id=dag_run.run_id,
                task_id=other_task_id,
                map_index=-1,
                lock_for_update=True,
                session=other_session,
            )
            assert stopped_ti is not None
            stopped_ti.set_state(state=terminal_state, session=other_session)
        stopped_tasks.append(other_task_id)

    executor.callback_sink.send.side_effect = stop_other_task
    SchedulerJobRunner.process_executor_events(
        executor=executor, job_id=1, scheduler_dag_bag=runner.scheduler_dag_bag, session=session
    )

    assert len(stopped_tasks) == 1
    stopped_ti: TaskInstance = next(ti for ti in task_instances if ti.task_id == stopped_tasks[0])
    stopped_ti.refresh_from_db(session=session)
    assert (stopped_ti.state, stopped_ti.max_tries) == (terminal_state, 0)
    executor.callback_sink.send.assert_called_once()
