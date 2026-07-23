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
Add ``infra_retry_count`` to ``task_instance`` (AIP-97 dedicated infra counter POC).

Revision ID: f1a2b3c4d5e6
Revises: c4e7a1f9b2d0
Create Date: 2026-07-23 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "f1a2b3c4d5e6"
down_revision = "c4e7a1f9b2d0"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    op.add_column(
        "task_instance",
        sa.Column("infra_retry_count", sa.Integer(), server_default="0", nullable=False),
    )


def downgrade():
    op.drop_column("task_instance", "infra_retry_count")
