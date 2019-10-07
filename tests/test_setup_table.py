import pytest

from mod_ngarn.connection import get_connection
from mod_ngarn.utils import create_table
from mod_ngarn.worker import JobRunner
from tests.utils import insert_job
from datetime import datetime


@pytest.mark.asyncio
async def test_can_rerun_create_table():
    cnx = await get_connection()

    await create_table("public", "modngarn_job")
    await create_table("public", "modngarn_job")

    await cnx.execute(f"DROP TABLE public.mod_ngarn_migration;")
    await cnx.execute(f"DROP TABLE public.modngarn_job;")
    await cnx.execute(f"DROP TABLE public.modngarn_job_error;")
    await cnx.close()


@pytest.mark.asyncio
async def test_can_create_table_with_specific_name():
    cnx = await get_connection()

    await create_table("public", "modngarn_job")

    migration_table = await cnx.fetchval(
        """SELECT EXISTS (SELECT 1 FROM pg_tables
            WHERE schemaname || '.' || tablename = 'public.mod_ngarn_migration');
        """
    )
    assert migration_table == True

    job_table = await cnx.fetchval(
        """SELECT EXISTS (SELECT 1 FROM pg_tables
            WHERE schemaname || '.' || tablename = 'public.modngarn_job');
        """
    )
    assert job_table == True

    log_table = await cnx.fetchval(
        """SELECT EXISTS (SELECT 1 FROM pg_tables
            WHERE schemaname || '.' || tablename = 'public.modngarn_job_error');
        """
    )
    assert log_table == True

    migration = await cnx.fetchval(
        """SELECT EXISTS (SELECT 1 FROM public.mod_ngarn_migration
            WHERE queue_table='public.modngarn_job');
        """
    )
    assert migration == True

    await cnx.execute(f"DROP TABLE public.mod_ngarn_migration;")
    await cnx.execute(f"DROP TABLE public.modngarn_job;")
    await cnx.execute(f"DROP TABLE public.modngarn_job_error;")
    await cnx.close()


@pytest.mark.asyncio
async def test_can_create_table_with_specific_special_character_name():
    cnx = await get_connection()

    await create_table("public", "modnga/rn_job")

    migration_table = await cnx.fetchval(
        """SELECT EXISTS (SELECT 1 FROM pg_tables
            WHERE schemaname || '.' || tablename = 'public.mod_ngarn_migration');
        """
    )
    assert migration_table == True

    job_table = await cnx.fetchval(
        """SELECT EXISTS (SELECT 1 FROM pg_tables
            WHERE schemaname || '.' || tablename = 'public.modnga/rn_job');
        """
    )
    assert job_table == True

    log_table = await cnx.fetchval(
        """SELECT EXISTS (SELECT 1 FROM pg_tables
            WHERE schemaname || '.' || tablename = 'public.modnga/rn_job_error');
        """
    )
    assert log_table == True

    migration = await cnx.fetchval(
        """SELECT EXISTS (SELECT 1 FROM public.mod_ngarn_migration
            WHERE queue_table='public.modnga/rn_job');
        """
    )
    assert migration == True

    await cnx.execute(f"DROP TABLE public.mod_ngarn_migration;")
    await cnx.execute(f'DROP TABLE public."modnga/rn_job";')
    await cnx.execute(f'DROP TABLE public."modnga/rn_job_error";')
    await cnx.close()
