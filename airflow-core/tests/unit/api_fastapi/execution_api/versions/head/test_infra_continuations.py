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

import json
import multiprocessing
import shutil
from datetime import timedelta
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

import httpx
import pytest
from sqlalchemy import delete, select

from airflow._shared.state import TaskFailureKind
from airflow._shared.timezones import timezone
from airflow.api_fastapi.core_api.routes.public.task_instances import get_task_instance_tries
from airflow.api_fastapi.execution_api.app import InProcessExecutionAPI
from airflow.api_fastapi.execution_api.security import require_auth
from airflow.callbacks.database_callback_sink import DatabaseCallbackSink
from airflow.executors.base_executor import BaseExecutor, TaskInstanceEvent
from airflow.executors.local_executor import LocalExecutor
from airflow.executors.workloads import ExecuteTask, WorkloadType
from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.models.callback import DagProcessorCallback
from airflow.models.dagbag import DBDagBag
from airflow.models.taskinstance import TaskInstance, clear_task_instances
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.models.taskreschedule import TaskReschedule
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.log.file_task_handler import FileTaskHandler, LogMetadata
from airflow.utils.log.log_reader import TaskLogReader
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, TaskInstanceState

from tests_common.test_utils.config import conf_vars

if TYPE_CHECKING:
    from pathlib import Path
    from uuid import UUID

    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from sqlalchemy.orm import Session
    from time_machine import TimeMachineFixture

    from airflow.sdk import DAG

    from tests_common.pytest_plugin import DagMaker

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def real_auth(exec_app: FastAPI) -> None:
    exec_app.dependency_overrides.pop(require_auth, None)


@pytest.fixture
def continuation_ti(
    dag_maker: DagMaker[DAG], session: Session, request: pytest.FixtureRequest
) -> TaskInstance:
    with dag_maker(dag_id="infra_continuation"):
        EmptyOperator(
            task_id="task",
            retries=getattr(request, "param", 0),
            retry_delay=timedelta(0),
            on_execute_callback=lambda context: None,
        )
    ti = dag_maker.create_dagrun(state=DagRunState.RUNNING).get_task_instance(task_id="task", session=session)
    assert ti is not None
    session.execute(delete(DagProcessorCallback))
    session.commit()
    return ti


def _start_attempt(*, ti: TaskInstance, client: TestClient, session: Session) -> dict[str, Any]:
    dag = DBDagBag().get_dag_for_run(dag_run=ti.dag_run, session=session)
    assert dag is not None
    ti.task = dag.get_task(ti.task_id)
    assert ti.dag_run.schedule_tis(schedulable_tis=[ti], session=session) == 1
    ti.refresh_from_db(session=session)
    assert ti.state == TaskInstanceState.SCHEDULED
    ti.set_state(state=TaskInstanceState.QUEUED, session=session)
    session.commit()
    token: str = BaseExecutor().jwt_generator.generate(extras={"sub": str(ti.id), "scope": "execution"})
    client.headers["Authorization"] = f"Bearer {token}"
    response = client.patch(
        url=f"/execution/task-instances/{ti.id}/run",
        json={
            "state": "running",
            "hostname": "continuation-local",
            "unixname": "airflow",
            "pid": 100,
            "start_date": timezone.utcnow().isoformat(),
        },
    )
    assert response.status_code == HTTPStatus.OK, response.text
    ti.refresh_from_db(session=session)
    return response.json()


def _executor_failure(
    *,
    ti: TaskInstance,
    session: Session,
    task_instance_id: UUID | None = None,
    failure_kind: TaskFailureKind | None = TaskFailureKind.INFRA,
) -> None:
    executor = BaseExecutor()
    executor.fail(
        key=ti.key,
        task_instance_id=task_instance_id or ti.id,
        failure_kind=failure_kind,
        reason="TerminationByKubelet",
    )
    SchedulerJobRunner.process_executor_events(
        executor=executor,
        job_id=ti.queued_by_job_id,
        scheduler_dag_bag=DBDagBag(),
        session=session,
    )
    session.commit()
    ti.refresh_from_db(session=session)


@conf_vars({("core", "max_infra_retries"): "1"})
def test_zero_retry_continuation_preserves_logical_try(
    continuation_ti: TaskInstance, client: TestClient, session: Session
) -> None:
    ti = continuation_ti
    _start_attempt(ti=ti, client=client, session=session)
    original_id = ti.id
    _executor_failure(ti=ti, session=session)
    assert ti.state == TaskInstanceState.UP_FOR_RESCHEDULE
    assert (ti.try_number, ti.max_tries) == (1, 0)
    assert ti.id != original_id
    assert (ti.infra_reschedule_count, ti.infra_continuation_number) == (1, 1)
    histories = session.scalars(select(TaskInstanceHistory)).all()
    assert [(row.id, row.state, row.try_number) for row in histories] == [
        (original_id, TaskInstanceState.UP_FOR_RESCHEDULE, 1)
    ]
    reschedules = session.scalars(select(TaskReschedule)).all()
    assert len(reschedules) == 1
    assert reschedules[0].ti_id == ti.id
    assert session.scalars(select(DagProcessorCallback)).all() == []

    context = _start_attempt(ti=ti, client=client, session=session)
    assert (ti.try_number, ti.max_tries) == (1, 0)
    assert context["should_retry"] is False


@conf_vars({("core", "max_infra_retries"): "2"})
def test_application_failure_after_continuation_has_no_retry_credit(
    continuation_ti: TaskInstance, client: TestClient, session: Session
) -> None:
    ti = continuation_ti
    _start_attempt(ti=ti, client=client, session=session)
    _executor_failure(ti=ti, session=session)
    assert ti.state == TaskInstanceState.UP_FOR_RESCHEDULE
    assert _start_attempt(ti=ti, client=client, session=session)["should_retry"] is False

    response = client.patch(
        url=f"/execution/task-instances/{ti.id}/state",
        json={"state": "failed", "end_date": timezone.utcnow().isoformat()},
    )
    assert response.status_code == HTTPStatus.NO_CONTENT, response.text
    ti.refresh_from_db(session=session)
    assert (ti.state, ti.try_number, ti.max_tries, ti.infra_reschedule_count) == (
        TaskInstanceState.FAILED,
        1,
        0,
        1,
    )


@pytest.mark.parametrize("budget", [0, 1])
def test_disabled_or_exhausted_continuation_fails_zero_retry_task(
    continuation_ti: TaskInstance, client: TestClient, session: Session, budget: int
) -> None:
    ti = continuation_ti
    with conf_vars({("core", "max_infra_retries"): str(budget)}):
        _start_attempt(ti=ti, client=client, session=session)
        if budget:
            _executor_failure(ti=ti, session=session)
            _start_attempt(ti=ti, client=client, session=session)
        _executor_failure(ti=ti, session=session)
    assert ti.state == TaskInstanceState.FAILED
    assert (ti.try_number, ti.max_tries, ti.infra_reschedule_count) == (1, 0, budget)


@conf_vars({("core", "max_infra_retries"): "1"})
def test_allowance_survives_new_scheduler_session(
    continuation_ti: TaskInstance, client: TestClient, session: Session
) -> None:
    ti = continuation_ti
    _start_attempt(ti=ti, client=client, session=session)
    _executor_failure(ti=ti, session=session)
    replacement_id = ti.id
    session.expunge_all()

    with create_session() as fresh_session:
        restored = fresh_session.get(TaskInstance, replacement_id)
        assert restored is not None
        assert restored.infra_reschedule_count == 1
        _start_attempt(ti=restored, client=client, session=fresh_session)
        _executor_failure(ti=restored, session=fresh_session)
        assert restored.state == TaskInstanceState.FAILED
        assert (restored.try_number, restored.max_tries, restored.infra_reschedule_count) == (1, 0, 1)


@pytest.mark.parametrize("continuation_ti", [1], indirect=True)
@conf_vars({("core", "max_infra_retries"): "1"})
def test_application_retry_does_not_consume_infrastructure_allowance(
    continuation_ti: TaskInstance, client: TestClient, session: Session
) -> None:
    ti = continuation_ti
    _start_attempt(ti=ti, client=client, session=session)
    _executor_failure(ti=ti, session=session, failure_kind=TaskFailureKind.APPLICATION)
    assert ti.state == TaskInstanceState.UP_FOR_RETRY
    _start_attempt(ti=ti, client=client, session=session)
    assert ti.try_number == 2
    _executor_failure(ti=ti, session=session)
    assert ti.state == TaskInstanceState.UP_FOR_RESCHEDULE
    assert (ti.try_number, ti.max_tries, ti.infra_reschedule_count) == (2, 1, 1)


@pytest.mark.parametrize("continuation_ti", [1], indirect=True)
@conf_vars({("core", "max_infra_retries"): "1"})
def test_application_retry_does_not_reset_infrastructure_allowance(
    continuation_ti: TaskInstance, client: TestClient, session: Session
) -> None:
    ti = continuation_ti
    _start_attempt(ti=ti, client=client, session=session)
    _executor_failure(ti=ti, session=session)
    _start_attempt(ti=ti, client=client, session=session)
    _executor_failure(ti=ti, session=session, failure_kind=TaskFailureKind.APPLICATION)
    assert ti.state == TaskInstanceState.UP_FOR_RETRY
    assert ti.infra_continuation_number == 0
    _start_attempt(ti=ti, client=client, session=session)
    _executor_failure(ti=ti, session=session)
    assert (ti.state, ti.try_number, ti.max_tries, ti.infra_reschedule_count) == (
        TaskInstanceState.FAILED,
        2,
        1,
        1,
    )


@pytest.mark.parametrize(
    "state",
    [
        TaskInstanceState.DEFERRED,
        TaskInstanceState.UP_FOR_RESCHEDULE,
        TaskInstanceState.SUCCESS,
        TaskInstanceState.FAILED,
    ],
)
@conf_vars({("core", "max_infra_retries"): "2"})
def test_infrastructure_event_does_not_change_nonrunning_execution(
    continuation_ti: TaskInstance, client: TestClient, session: Session, state: TaskInstanceState
) -> None:
    ti = continuation_ti
    _start_attempt(ti=ti, client=client, session=session)
    original_id = ti.id
    ti.set_state(state=state, session=session)
    session.commit()
    ti.handle_failure(
        error="Late backend event",
        failure_kind=TaskFailureKind.INFRA,
        session=session,
    )
    assert (ti.id, ti.state, ti.try_number, ti.max_tries) == (original_id, state, 1, 0)
    assert ti.infra_reschedule_count == 0


@pytest.mark.parametrize("state", [TaskInstanceState.SCHEDULED, TaskInstanceState.QUEUED])
@pytest.mark.parametrize("budget", [0, 2])
def test_unstarted_failure_uses_ordinary_failure_handling(
    continuation_ti: TaskInstance,
    client: TestClient,
    session: Session,
    state: TaskInstanceState,
    budget: int,
) -> None:
    ti = continuation_ti
    _start_attempt(ti=ti, client=client, session=session)
    ti.set_state(state=state, session=session)
    session.commit()
    with conf_vars({("core", "max_infra_retries"): str(budget)}):
        ti.handle_failure(
            error="Worker stopped before starting", failure_kind=TaskFailureKind.INFRA, session=session
        )
    assert (ti.state, ti.max_tries, ti.infra_reschedule_count) == (TaskInstanceState.FAILED, 0, 0)


@pytest.mark.parametrize(
    ("event_state", "expected_state"),
    [
        (TaskInstanceState.SUCCESS, None),
        (TaskInstanceState.FAILED, TaskInstanceState.UP_FOR_RETRY),
        (TaskInstanceState.RESTARTING, TaskInstanceState.UP_FOR_RETRY),
    ],
)
@conf_vars({("core", "max_infra_retries"): "2"})
def test_running_clear_accepts_its_worker_completion_without_infra_credit(
    continuation_ti: TaskInstance,
    client: TestClient,
    session: Session,
    event_state: TaskInstanceState,
    expected_state: TaskInstanceState | None,
) -> None:
    ti = continuation_ti
    _start_attempt(ti=ti, client=client, session=session)
    old_id = ti.id
    clear_task_instances(tis=[ti], session=session)
    session.commit()
    cleared_id = ti.id
    assert ti.state == TaskInstanceState.RESTARTING
    executor = BaseExecutor()
    if event_state == TaskInstanceState.FAILED:
        executor.fail(key=ti.key, task_instance_id=old_id, failure_kind=TaskFailureKind.INFRA)
    else:
        executor.change_state(
            key=ti.key,
            state=event_state,
            info=TaskInstanceEvent(task_instance_id=old_id),
        )
    SchedulerJobRunner.process_executor_events(
        executor=executor, job_id=ti.queued_by_job_id, scheduler_dag_bag=DBDagBag(), session=session
    )
    session.commit()
    ti.refresh_from_db(session=session)
    assert (ti.id, ti.state, ti.infra_reschedule_count) == (cleared_id, expected_state, 0)
    assert _start_attempt(ti=ti, client=client, session=session)["should_retry"] is False
    assert ti.try_number == 2


@pytest.mark.parametrize("failure_kind", [None, TaskFailureKind.APPLICATION, TaskFailureKind.TIMEOUT])
@conf_vars({("core", "max_infra_retries"): "2"})
def test_only_confirmed_infrastructure_failure_can_continue(
    continuation_ti: TaskInstance,
    client: TestClient,
    session: Session,
    failure_kind: TaskFailureKind | None,
) -> None:
    ti = continuation_ti
    _start_attempt(ti=ti, client=client, session=session)
    _executor_failure(ti=ti, session=session, failure_kind=failure_kind)
    assert (ti.state, ti.max_tries, ti.infra_reschedule_count) == (TaskInstanceState.FAILED, 0, 0)


@conf_vars({("core", "max_infra_retries"): "1"})
def test_manual_clear_resets_allowance_without_application_retry_credit(
    continuation_ti: TaskInstance, client: TestClient, session: Session
) -> None:
    ti = continuation_ti
    _start_attempt(ti=ti, client=client, session=session)
    _executor_failure(ti=ti, session=session)
    clear_task_instances(tis=[ti], session=session)
    session.commit()
    assert (ti.infra_reschedule_count, ti.infra_continuation_number) == (0, 0)

    assert _start_attempt(ti=ti, client=client, session=session)["should_retry"] is False
    assert ti.try_number == 2
    _executor_failure(ti=ti, session=session)
    assert ti.state == TaskInstanceState.UP_FOR_RESCHEDULE
    assert _start_attempt(ti=ti, client=client, session=session)["should_retry"] is False
    _executor_failure(ti=ti, session=session, failure_kind=TaskFailureKind.APPLICATION)
    assert (ti.state, ti.try_number, ti.infra_reschedule_count) == (TaskInstanceState.FAILED, 2, 1)


@conf_vars({("core", "max_infra_retries"): "2"})
def test_late_failure_from_previous_execution_cannot_change_replacement(
    continuation_ti: TaskInstance, client: TestClient, session: Session
) -> None:
    ti = continuation_ti
    _start_attempt(ti=ti, client=client, session=session)
    old_id = ti.id
    _executor_failure(ti=ti, session=session)
    new_id = ti.id
    _start_attempt(ti=ti, client=client, session=session)
    _executor_failure(ti=ti, session=session, task_instance_id=old_id)
    assert (ti.id, ti.state, ti.try_number, ti.max_tries, ti.infra_reschedule_count) == (
        new_id,
        TaskInstanceState.RUNNING,
        1,
        0,
        1,
    )


@pytest.mark.parametrize(
    "state",
    [
        TaskInstanceState.QUEUED,
        TaskInstanceState.RUNNING,
        TaskInstanceState.SUCCESS,
        TaskInstanceState.FAILED,
    ],
)
@conf_vars({("core", "max_infra_retries"): "2"})
def test_unidentified_event_cannot_change_same_try_replacement(
    continuation_ti: TaskInstance, client: TestClient, session: Session, state: TaskInstanceState
) -> None:
    ti = continuation_ti
    _start_attempt(ti=ti, client=client, session=session)
    _executor_failure(ti=ti, session=session)
    _start_attempt(ti=ti, client=client, session=session)
    original_executor_id = ti.external_executor_id
    executor = BaseExecutor()
    executor.change_state(key=ti.key, state=state, info="stale-backend-id")
    SchedulerJobRunner.process_executor_events(
        executor=executor, job_id=ti.queued_by_job_id, scheduler_dag_bag=DBDagBag(), session=session
    )
    session.commit()
    ti.refresh_from_db(session=session)
    assert ti.state == TaskInstanceState.RUNNING
    assert ti.external_executor_id == original_executor_id
    assert ti.infra_reschedule_count == 1


@conf_vars({("core", "max_infra_retries"): "2"})
def test_retired_execution_token_cannot_mutate_logical_task_data(
    continuation_ti: TaskInstance, client: TestClient, session: Session
) -> None:
    ti = continuation_ti
    _start_attempt(ti=ti, client=client, session=session)
    old_id = ti.id
    old_token = client.headers["Authorization"]
    _executor_failure(ti=ti, session=session)
    _start_attempt(ti=ti, client=client, session=session)
    new_id = ti.id
    client.headers["Authorization"] = old_token

    state_response = client.patch(
        url=f"/execution/task-instances/{old_id}/state",
        json={"state": "failed", "end_date": timezone.utcnow().isoformat()},
    )
    xcom_response = client.post(
        url=f"/execution/xcoms/{ti.dag_id}/{ti.run_id}/{ti.task_id}/stale",
        json="must not be written",
    )
    store_response = client.put(
        url=f"/execution/store/ti/{old_id}/stale",
        json={"value": "must not be written"},
    )
    for response in (state_response, xcom_response, store_response):
        assert response.status_code in {
            HTTPStatus.FORBIDDEN,
            HTTPStatus.NOT_FOUND,
            HTTPStatus.CONFLICT,
        }, response.text

    ti.refresh_from_db(session=session)
    assert (ti.id, ti.state, ti.max_tries) == (new_id, TaskInstanceState.RUNNING, 0)


@conf_vars({("core", "max_infra_retries"): "1"})
def test_retired_execution_remains_fenced_after_history_cleanup(
    continuation_ti: TaskInstance, client: TestClient, session: Session
) -> None:
    ti = continuation_ti
    _start_attempt(ti=ti, client=client, session=session)
    old_id = ti.id
    old_token = client.headers["Authorization"]
    _executor_failure(ti=ti, session=session)
    _start_attempt(ti=ti, client=client, session=session)
    session.execute(delete(TaskInstanceHistory).where(TaskInstanceHistory.task_instance_id == old_id))
    session.commit()
    client.headers["Authorization"] = old_token

    response = client.post(
        url=f"/execution/xcoms/{ti.dag_id}/{ti.run_id}/{ti.task_id}/after_history_cleanup",
        json="stale execution",
    )
    assert response.status_code in {
        HTTPStatus.FORBIDDEN,
        HTTPStatus.NOT_FOUND,
        HTTPStatus.CONFLICT,
    }, response.text


@conf_vars({("core", "max_infra_retries"): "2"})
def test_generic_reschedule_keeps_identity_and_allowance(
    continuation_ti: TaskInstance, client: TestClient, session: Session
) -> None:
    ti = continuation_ti
    _start_attempt(ti=ti, client=client, session=session)
    original_id = ti.id
    response = client.patch(
        url=f"/execution/task-instances/{ti.id}/state",
        json={
            "state": "up_for_reschedule",
            "end_date": timezone.utcnow().isoformat(),
            "reschedule_date": timezone.utcnow().isoformat(),
        },
    )
    assert response.status_code == HTTPStatus.NO_CONTENT, response.text
    ti.refresh_from_db(session=session)
    _start_attempt(ti=ti, client=client, session=session)
    assert (ti.id, ti.try_number, ti.max_tries, ti.infra_reschedule_count) == (original_id, 1, 0, 0)
    _executor_failure(ti=ti, session=session)
    assert len(session.scalars(select(TaskReschedule)).all()) == 2
    assert ti.infra_reschedule_count == 1


@conf_vars({("core", "max_infra_retries"): "2"})
def test_continuation_history_does_not_add_visible_failed_tries(
    continuation_ti: TaskInstance, client: TestClient, session: Session
) -> None:
    ti = continuation_ti
    _start_attempt(ti=ti, client=client, session=session)
    _executor_failure(ti=ti, session=session)
    _start_attempt(ti=ti, client=client, session=session)
    _executor_failure(ti=ti, session=session)
    result = get_task_instance_tries(
        dag_id=ti.dag_id,
        dag_run_id=ti.run_id,
        task_id=ti.task_id,
        map_index=ti.map_index,
        session=session,
    )
    assert result.total_entries == 1
    assert result.task_instances[0].try_number == 1
    assert result.task_instances[0].state == TaskInstanceState.UP_FOR_RESCHEDULE
    assert len(session.scalars(select(TaskInstanceHistory)).all()) == 2


@conf_vars({("core", "max_infra_retries"): "1"})
def test_continuation_log_segments_remain_readable(
    continuation_ti: TaskInstance, client: TestClient, session: Session, tmp_path: Path
) -> None:
    ti = continuation_ti
    handler = FileTaskHandler(base_log_folder=str(tmp_path))
    _start_attempt(ti=ti, client=client, session=session)
    first_id = ti.id
    first_path = tmp_path / handler._render_filename(ti=ti, try_number=1, session=session)
    first_path.parent.mkdir(parents=True, exist_ok=True)
    first_path.write_text(json.dumps({"event": "first execution"}) + "\n")
    _executor_failure(ti=ti, session=session)
    first_segment = session.get(TaskInstanceHistory, first_id)
    assert first_segment is not None
    first_stream, first_metadata = handler.read(task_instance=first_segment, try_number=1)
    assert "first execution" in [entry.event for entry in first_stream]
    _start_attempt(ti=ti, client=client, session=session)
    last_id = ti.id
    last_path = tmp_path / handler._render_filename(ti=ti, try_number=1, session=session)
    assert first_path != last_path
    last_path.parent.mkdir(parents=True, exist_ok=True)
    last_path.write_text(json.dumps({"event": "continued execution"}) + "\n")
    ti.set_state(state=TaskInstanceState.SUCCESS, session=session)
    session.commit()

    reader = TaskLogReader()
    reader.log_handler = handler
    stream, metadata = reader.read_log_chunks(ti=ti, try_number=1, metadata=LogMetadata(end_of_log=False))
    events = [entry.event for entry in stream]
    assert "first execution" in events
    assert "continued execution" in events
    assert any(str(first_id) in event for event in events)
    assert any(str(last_id) in event for event in events)
    assert metadata["end_of_log"] is True
    stream, _ = reader.read_log_chunks(ti=ti, try_number=1, metadata=metadata)
    assert list(stream) == []
    stream, _ = reader.read_log_chunks(ti=ti, try_number=1, metadata=first_metadata)
    continued_events = [entry.event for entry in stream]
    assert "first execution" not in continued_events
    assert "continued execution" in continued_events
    download = "".join(reader.read_log_stream(ti=ti, try_number=1, metadata=metadata.copy()))
    assert "first execution" in download
    assert "continued execution" in download
    shutil.rmtree(first_path.parent)
    assert "continued execution" in last_path.read_text()


@conf_vars({("core", "max_infra_retries"): "1"})
def test_local_executor_consumes_current_continuation_result(
    continuation_ti: TaskInstance, client: TestClient, session: Session
) -> None:
    ti = continuation_ti
    _start_attempt(ti=ti, client=client, session=session)
    _executor_failure(ti=ti, session=session)
    _start_attempt(ti=ti, client=client, session=session)
    executor = LocalExecutor()
    executor.queue_workload(workload=ExecuteTask.make(ti=ti), session=session)
    executor.executor_queues[WorkloadType.EXECUTE_TASK].pop(ti.key)
    executor.running.add(ti.key)
    executor.result_queue = multiprocessing.SimpleQueue()
    executor.result_queue.put((ti.key, TaskInstanceState.SUCCESS, TaskInstanceEvent(task_instance_id=ti.id)))
    executor._read_results()
    assert ti.key not in executor.running
    state, info = executor.get_event_buffer()[ti.key]
    assert state == TaskInstanceState.SUCCESS
    assert info.task_instance_id == ti.id


@pytest.mark.parametrize("cleanup", ["heartbeat", "queued"])
@conf_vars({("core", "max_infra_retries"): "2", ("scheduler", "num_stuck_in_queued_retries"): "0"})
def test_scheduler_cleanup_fences_its_continuation(
    continuation_ti: TaskInstance,
    client: TestClient,
    session: Session,
    time_machine: TimeMachineFixture,
    cleanup: str,
) -> None:
    ti = continuation_ti
    _start_attempt(ti=ti, client=client, session=session)
    _executor_failure(ti=ti, session=session)
    if cleanup == "heartbeat":
        _start_attempt(ti=ti, client=client, session=session)
    else:
        assert ti.dag_run.schedule_tis(schedulable_tis=[ti], session=session) == 1
        ti.refresh_from_db(session=session)
        ti.set_state(state=TaskInstanceState.QUEUED, session=session)

    executor = BaseExecutor()
    executor.callback_sink = DatabaseCallbackSink()
    job = Job()
    session.add(job)
    session.flush()
    runner = SchedulerJobRunner(job=job, executors=[executor])
    ti.queued_by_job_id = job.id
    session.commit()
    executor.queue_workload(workload=ExecuteTask.make(ti=ti), session=session)
    executor.executor_queues[WorkloadType.EXECUTE_TASK].pop(ti.key)
    executor.running.add(ti.key)
    if cleanup == "heartbeat":
        time_machine.move_to(timezone.utcnow() + timedelta(minutes=10))
        runner._find_and_purge_task_instances_without_heartbeats()
    else:
        runner._maybe_requeue_stuck_ti(ti=ti, session=session, executor=executor)

    assert ti.key not in executor.running
    assert executor.event_buffer[ti.key][1].task_instance_id == ti.id
    runner._process_executor_events(executor=executor, session=session)
    session.commit()
    ti.refresh_from_db(session=session)
    assert (ti.state, ti.try_number, ti.max_tries, ti.infra_reschedule_count) == (
        TaskInstanceState.FAILED,
        1,
        0,
        1,
    )


@pytest.mark.parametrize("operation", ["variable", "xcom", "dag_run", "task_state"])
def test_inprocess_execution_api_keeps_trusted_writes(continuation_ti: TaskInstance, operation: str) -> None:
    ti = continuation_ti
    api = InProcessExecutionAPI()
    with httpx.Client(transport=api.transport, base_url="http://in-process.invalid") as client:
        if operation == "variable":
            response = client.put(url="/variables/continuation_inprocess", json={"value": "saved"})
            assert response.is_success, response.text
            assert client.get(url="/variables/continuation_inprocess").json()["value"] == "saved"
            assert client.delete(url="/variables/continuation_inprocess").is_success
        elif operation == "xcom":
            url = f"/xcoms/{ti.dag_id}/{ti.run_id}/{ti.task_id}/inprocess"
            response = client.post(url=url, json="saved")
            assert response.is_success, response.text
            assert client.get(url=url).json()["value"] == "saved"
            assert client.delete(url=url).is_success
        elif operation == "dag_run":
            response = client.post(
                url=f"/dag-runs/{ti.dag_id}/manual__inprocess",
                json={"logical_date": None, "conf": {}},
            )
            assert response.status_code == HTTPStatus.NO_CONTENT, response.text
        else:
            response = client.put(url=f"/store/ti/{ti.id}/inprocess", json={"value": "saved"})
            assert response.status_code == HTTPStatus.NO_CONTENT, response.text
            assert client.get(url=f"/store/ti/{ti.id}/inprocess").json()["value"] == "saved"


@pytest.mark.parametrize("continuation_ti", [1], indirect=True)
def test_logs_exist_for_submission_retry(
    continuation_ti: TaskInstance, test_client: TestClient, session: Session
) -> None:
    ti = continuation_ti
    dag = DBDagBag().get_dag_for_run(dag_run=ti.dag_run, session=session)
    assert dag is not None
    ti.task = dag.get_task(ti.task_id)
    assert ti.dag_run.schedule_tis(schedulable_tis=[ti], session=session) == 1
    ti.refresh_from_db(session=session)
    ti.set_state(state=TaskInstanceState.QUEUED, session=session)
    session.commit()
    workload = ExecuteTask.make(ti=ti)
    executor = BaseExecutor()
    executor.fail(
        key=workload.ti.key,
        task_instance_id=workload.ti.id,
        info="Synthetic executor submission error before the worker started",
    )
    SchedulerJobRunner.process_executor_events(
        executor=executor, job_id=None, scheduler_dag_bag=DBDagBag(), session=session
    )
    session.commit()
    ti.refresh_from_db(session=session)
    assert ti.state == TaskInstanceState.UP_FOR_RETRY
    assert [(tih.try_number, tih.state) for tih in session.scalars(select(TaskInstanceHistory))] == [
        (ti.try_number, TaskInstanceState.FAILED)
    ]

    response = test_client.get(
        url=f"/api/v2/dags/{ti.dag_id}/dagRuns/{ti.run_id}/taskInstances/{ti.task_id}/logs/{ti.try_number}"
    )
    assert response.status_code == HTTPStatus.OK, response.text


def _interrupt(*, ti: TaskInstance, client: TestClient, session: Session) -> str | None:
    response = client.post(
        url=f"/execution/task-instances/{ti.id}/interrupt", json={"reason": "WorkerSIGTERM"}
    )
    assert response.status_code == HTTPStatus.OK, response.text
    ti.refresh_from_db(session=session)
    return response.json()["state"]


@conf_vars({("core", "max_infra_retries"): "1"})
def test_worker_interrupt_continues_same_try(
    continuation_ti: TaskInstance, client: TestClient, session: Session
) -> None:
    ti = continuation_ti
    _start_attempt(ti=ti, client=client, session=session)
    original_id = ti.id

    assert _interrupt(ti=ti, client=client, session=session) == TaskInstanceState.UP_FOR_RESCHEDULE
    assert ti.id != original_id
    assert (ti.state, ti.try_number, ti.max_tries, ti.infra_reschedule_count) == (
        TaskInstanceState.UP_FOR_RESCHEDULE,
        1,
        0,
        1,
    )
    histories = session.scalars(select(TaskInstanceHistory)).all()
    assert [(row.id, row.state, row.try_number) for row in histories] == [
        (original_id, TaskInstanceState.UP_FOR_RESCHEDULE, 1)
    ]


@pytest.mark.parametrize("budget", [0, 1])
def test_worker_interrupt_without_allowance_leaves_try_running(
    continuation_ti: TaskInstance, client: TestClient, session: Session, budget: int
) -> None:
    ti = continuation_ti
    with conf_vars({("core", "max_infra_retries"): str(budget)}):
        _start_attempt(ti=ti, client=client, session=session)
        if budget:
            _executor_failure(ti=ti, session=session)
            _start_attempt(ti=ti, client=client, session=session)
        running_id = ti.id

        assert _interrupt(ti=ti, client=client, session=session) == TaskInstanceState.RUNNING
    assert (ti.id, ti.state, ti.try_number, ti.infra_reschedule_count) == (
        running_id,
        TaskInstanceState.RUNNING,
        1,
        budget,
    )
    assert len(session.scalars(select(TaskInstanceHistory)).all()) == budget


@pytest.mark.parametrize("state", [TaskInstanceState.SUCCESS, TaskInstanceState.FAILED])
@conf_vars({("core", "max_infra_retries"): "1"})
def test_worker_interrupt_does_not_change_finished_execution(
    continuation_ti: TaskInstance, client: TestClient, session: Session, state: TaskInstanceState
) -> None:
    ti = continuation_ti
    _start_attempt(ti=ti, client=client, session=session)
    original_id = ti.id
    ti.set_state(state=state, session=session)
    session.commit()

    assert _interrupt(ti=ti, client=client, session=session) == state
    assert (ti.id, ti.state, ti.try_number, ti.infra_reschedule_count) == (original_id, state, 1, 0)
