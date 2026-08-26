from __future__ import annotations

import unittest

from fastapi import WebSocketDisconnect

from gateway.config import CONFIG
from gateway.main import market_socket, service_status


class FakeWebSocket:
    def __init__(self, origin: str = "https://profitparty.online") -> None:
        self.headers = {"origin": origin}
        self.accepted = False
        self.closed: tuple[int, str] | None = None
        self.snapshot: dict[str, object] | None = None

    async def accept(self) -> None:
        self.accepted = True

    async def close(self, code: int, reason: str) -> None:
        self.closed = (code, reason)

    async def send_json(self, payload: dict[str, object]) -> None:
        self.snapshot = payload

    async def receive_text(self) -> str:
        raise WebSocketDisconnect()


class PublicGatewayTests(unittest.IsolatedAsyncioTestCase):
    def test_public_mode_has_no_visitor_credentials(self) -> None:
        self.assertEqual(CONFIG.missing_credentials(), [])

    async def test_root_reports_read_only_public_service(self) -> None:
        payload = await service_status()
        self.assertTrue(payload["online"])
        self.assertEqual(payload["mode"], "public-read-only")
        self.assertFalse(payload["orderExecution"])
        self.assertEqual(payload["websocket"], "/ws")

    async def test_allowed_site_connects_without_query_token(self) -> None:
        websocket = FakeWebSocket()
        await market_socket(websocket)  # type: ignore[arg-type]
        self.assertTrue(websocket.accepted)
        self.assertIsNotNone(websocket.snapshot)
        self.assertIsNone(websocket.closed)

    async def test_unknown_browser_origin_is_rejected(self) -> None:
        websocket = FakeWebSocket("https://example.com")
        await market_socket(websocket)  # type: ignore[arg-type]
        self.assertFalse(websocket.accepted)
        self.assertEqual(websocket.closed, (1008, "Origin is not allowed"))


if __name__ == "__main__":
    unittest.main()
