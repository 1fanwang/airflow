#
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
Add infra_retry_count to task_instance.

Counts the extra attempts granted for infra-classified failures. Extends the retry
ceiling without inflating ``max_tries`` (which stays == the user's ``retries``), so the
user's retry number stays honest and an infra disruption is retried even at retries=0.

Revision ID: c8e4b1f7a3d2
Revises: b7f3a9e2d6c1
Create Date: 2026-07-25 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "c8e4b1f7a3d2"
down_revision = "b7f3a9e2d6c1"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Add infra_retry_count column to task_instance."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.add_column(sa.Column("infra_retry_count", sa.Integer(), nullable=False, server_default="0"))


def downgrade():
    """Remove infra_retry_count column from task_instance."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.drop_column("infra_retry_count")
