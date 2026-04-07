"""
BigQuery schema evolution helpers.

Idempotent: ADD COLUMN IF NOT EXISTS only runs when the table already exists,
so first-write (Spark creates table) is not broken by ALTER on a missing table.
"""

from __future__ import annotations

import logging

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logger = logging.getLogger(__name__)


def ensure_bq_table_columns(
    client: bigquery.Client,
    table_ref: str,
    columns: list[tuple[str, str]],
) -> None:
    """
    Add missing columns to an existing BigQuery table.

    Skips entirely if the table does not exist yet (next append/create will
    define the schema). Safe to call on every job run.
    """
    try:
        client.get_table(table_ref)
    except NotFound:
        logger.info("BQ table %s not found; skipping ALTER (schema set on first write).", table_ref)
        return
    except Exception as e:
        logger.warning("Could not inspect BQ table %s (%s); skipping ALTER.", table_ref, e)
        return

    for name, typ in columns:
        try:
            client.query(
                f"ALTER TABLE `{table_ref}` ADD COLUMN IF NOT EXISTS {name} {typ}"
            ).result()
        except Exception as exc:
            logger.warning(
                "Could not add column %s to %s (%s); continuing.",
                name,
                table_ref,
                exc,
            )
