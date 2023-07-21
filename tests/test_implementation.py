from youtube00e1f862e5eff import query
from exorde_data.models import Item
import pytest


@pytest.mark.asyncio
async def test_query():
    async for result in query():
        assert isinstance(result, Item)