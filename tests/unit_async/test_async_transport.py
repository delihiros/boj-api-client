from __future__ import annotations

import pytest

from boj_api_client.core.async_transport import AsyncTransport
from boj_api_client.core.errors import BojServerError, BojTransportError
from tests.shared.transport import AsyncSequencedClient, Response, Step, build_config


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("steps", "max_attempts", "expected_status", "expected_exception", "expected_calls"),
    [
        (
            [
                Response(200, {"STATUS": 500, "MESSAGEID": "E", "MESSAGE": "x"}),
                Response(200, {"STATUS": 200, "MESSAGEID": "M181000I", "RESULTSET": []}),
            ],
            2,
            200,
            None,
            2,
        ),
        (
            [Response(200, {"STATUS": 500, "MESSAGEID": "E", "MESSAGE": "x"})],
            1,
            None,
            BojServerError,
            1,
        ),
        (
            [
                RuntimeError("network down"),
                Response(200, {"STATUS": 200, "MESSAGEID": "M181000I", "RESULTSET": []}),
            ],
            2,
            200,
            None,
            2,
        ),
        (
            [RuntimeError("network down")],
            1,
            None,
            BojTransportError,
            1,
        ),
    ],
    ids=[
        "status-500-then-success",
        "status-500-exhausted",
        "network-then-success",
        "network-exhausted",
    ],
)
async def test_async_transport_retry_matrix(
    steps: list[Step],
    max_attempts: int,
    expected_status: int | None,
    expected_exception: type[Exception] | None,
    expected_calls: int,
):
    client = AsyncSequencedClient(steps)
    transport = AsyncTransport(build_config(max_attempts=max_attempts), client=client)

    if expected_exception is not None:
        with pytest.raises(expected_exception):
            await transport.request("/getMetadata", params={"db": "FM08"})
    else:
        payload = await transport.request("/getMetadata", params={"db": "FM08"})
        assert payload["STATUS"] == expected_status

    assert client.calls == expected_calls


@pytest.mark.asyncio
async def test_async_transport_can_initialize_and_close_with_real_httpx_client():
    transport = AsyncTransport(build_config(max_attempts=1))
    await transport.close()

