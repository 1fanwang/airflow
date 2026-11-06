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
Add infrastructure continuation tracking to task instances.

Revision ID: 4e7b2c9a1d35
Revises: b6a9c2e7d410
Create Date: 2026-11-08 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.utils.state import TaskInstanceState

revision = "4e7b2c9a1d35"
down_revision = "b6a9c2e7d410"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade() -> None:
    is_mysql: bool = op.get_bind().dialect.name == "mysql"
    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.add_column(
            sa.Column("infra_reschedule_count", sa.Integer(), nullable=False, server_default="0")
        )
        batch_op.add_column(
            sa.Column("infra_continuation_number", sa.Integer(), nullable=False, server_default="0")
        )
    with op.batch_alter_table("task_instance_history") as batch_op:
        batch_op.add_column(
            sa.Column("infra_continuation_number", sa.Integer(), nullable=False, server_default="0")
        )
        if is_mysql:
            # MySQL needs an index for the foreign key while the unique index is replaced.
            batch_op.create_index(
                "idx_tih_migration_fk", ["dag_id", "task_id", "run_id", "map_index"], unique=False
            )
        batch_op.drop_constraint("task_instance_history_dtrt_uq", type_="unique")
        batch_op.create_unique_constraint(
            "task_instance_history_dtrt_uq",
            ["dag_id", "task_id", "run_id", "map_index", "try_number", "infra_continuation_number"],
        )
        if is_mysql:
            batch_op.drop_index("idx_tih_migration_fk")
    with op.batch_alter_table("task_reschedule") as batch_op:
        batch_op.drop_constraint("task_reschedule_ti_fkey", type_="foreignkey")
        batch_op.create_foreign_key(
            "task_reschedule_ti_fkey",
            "task_instance",
            ["ti_id"],
            ["id"],
            ondelete="CASCADE",
            onupdate="CASCADE",
        )


def downgrade() -> None:
    is_mysql: bool = op.get_bind().dialect.name == "mysql"
    op.execute(
        sa.table("task_instance_history", sa.column("state"))
        .delete()
        .where(sa.column("state") == TaskInstanceState.UP_FOR_RESCHEDULE)
    )
    with op.batch_alter_table("task_reschedule") as batch_op:
        batch_op.drop_constraint("task_reschedule_ti_fkey", type_="foreignkey")
        batch_op.create_foreign_key(
            "task_reschedule_ti_fkey", "task_instance", ["ti_id"], ["id"], ondelete="CASCADE"
        )
    with op.batch_alter_table("task_instance_history") as batch_op:
        if is_mysql:
            batch_op.create_index(
                "idx_tih_migration_fk", ["dag_id", "task_id", "run_id", "map_index"], unique=False
            )
        batch_op.drop_constraint("task_instance_history_dtrt_uq", type_="unique")
        batch_op.create_unique_constraint(
            "task_instance_history_dtrt_uq", ["dag_id", "task_id", "run_id", "map_index", "try_number"]
        )
        if is_mysql:
            batch_op.drop_index("idx_tih_migration_fk")
        batch_op.drop_column("infra_continuation_number")
    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.drop_column("infra_continuation_number")
        batch_op.drop_column("infra_reschedule_count")
