import pytest

from mod_ngarn.utils import notify_channel


@pytest.mark.asyncio
async def test_notify_channel_should_return_correctly():
    assert "public_mod_ngarn_job" == notify_channel('"public"."mod_ngarn_job"')
    assert "mod_ngarn_job" == notify_channel("mod_ngarn_job")
