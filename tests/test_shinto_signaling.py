import asyncio
import os
import sys
import types
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

sys.modules.setdefault(
    "selkies.webrtc_utils",
    types.SimpleNamespace(
        generate_rtc_config=lambda *args, **kwargs: "{}",
        _is_trusted_config_file=lambda path: True,
    ),
)

from selkies.signaling_server import Peer, WebRTCPeerManagement


class FakeOptions:
    keepalive_timeout = 1
    turn_shared_secret = None
    turn_host = None
    turn_port = None
    turn_protocol = "udp"
    turn_tls = False
    turn_auth_header_name = "X-User"
    stun_host = None
    stun_port = None
    enable_sharing = True
    enable_shared = True
    enable_player2 = True
    enable_player3 = True
    enable_player4 = True
    rtc_config_file = "/tmp/selkies-test-missing-rtc-config.json"


class FakeWebSocket:
    def __init__(self):
        self.closed = False
        self.close_calls = []
        self.sent = []

    async def close(self, code=1000, message=b""):
        self.closed = True
        self.close_calls.append((code, message))

    async def send_str(self, message):
        self.sent.append(message)


def make_peer(uid, ws, peer_type, client_type=None):
    return Peer(
        uid=uid,
        ws=ws,
        raddr="127.0.0.1",
        peer_type=peer_type,
        client_type=client_type,
        client_slot=-1,
        client_strict_viewer=False,
    )


class ShintoPersistentSessionTests(unittest.IsolatedAsyncioTestCase):
    def test_server_peer_readiness_tracks_registered_server_peer(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            manager = WebRTCPeerManagement(FakeOptions(), close_peer=mock.AsyncMock())

        self.assertFalse(manager.has_server_peer())

        server_ws = FakeWebSocket()
        manager.peers = {
            "client-1": make_peer("client-1", FakeWebSocket(), "client", "controller"),
            "server-1": make_peer("server-1", server_ws, "server"),
        }

        self.assertTrue(manager.has_server_peer())

        server_ws.closed = True
        self.assertFalse(manager.has_server_peer())

    def test_server_peer_readiness_route_is_registered(self):
        source = (SRC / "selkies" / "webrtc_mode.py").read_text()

        self.assertIn('add_get(f"{api_prefix}/shinto/server-ready"', source)
        self.assertIn("handle_server_ready", source)

    async def test_persistent_session_retains_server_when_controller_disconnects(self):
        with mock.patch.dict(os.environ, {"SHINTO_PERSISTENT_SESSION": "1"}):
            manager = WebRTCPeerManagement(FakeOptions(), close_peer=mock.AsyncMock())

        controller_ws = FakeWebSocket()
        server_ws = FakeWebSocket()
        manager.peers = {
            "client-1": make_peer("client-1", controller_ws, "client", "controller"),
            "server-1": make_peer("server-1", server_ws, "server"),
        }
        manager.sessions = {"client-1": "server-1"}

        await manager.cleanup_session("client-1")

        self.assertEqual({}, manager.sessions)
        self.assertEqual([], server_ws.close_calls)
        self.assertFalse(server_ws.closed)
        manager.close_peer.assert_awaited_once_with("client-1")

    async def test_persistent_session_does_not_send_viewer_session_end(self):
        with mock.patch.dict(os.environ, {"SHINTO_PERSISTENT_SESSION": "true"}):
            manager = WebRTCPeerManagement(FakeOptions(), close_peer=mock.AsyncMock())

        viewer_ws = FakeWebSocket()
        server_ws = FakeWebSocket()
        manager.peers = {
            "client-1": make_peer("client-1", viewer_ws, "client", "viewer"),
            "server-1": make_peer("server-1", server_ws, "server"),
        }
        manager.sessions = {"client-1": "server-1"}

        await manager.cleanup_session("client-1")

        self.assertEqual([], server_ws.sent)
        manager.close_peer.assert_awaited_once_with("client-1")

    async def test_peer_cleanup_joins_exact_rtc_closure(self):
        entered, release = asyncio.Event(), asyncio.Event()
        closed = []

        async def close_peer(uid):
            entered.set()
            await release.wait()
            closed.append(uid)

        with mock.patch.dict(os.environ, {"SHINTO_PERSISTENT_SESSION": "1"}):
            manager = WebRTCPeerManagement(FakeOptions(), close_peer=close_peer)
        current_ws, other_ws, server_ws = FakeWebSocket(), FakeWebSocket(), FakeWebSocket()
        manager.peers = {
            "client-1": make_peer("client-1", current_ws, "client", "controller"),
            "client-2": make_peer("client-2", other_ws, "client", "viewer"),
            "server-1": make_peer("server-1", server_ws, "server"),
        }
        manager.sessions = {"client-1": "server-1", "client-2": "server-1"}
        cleanup = asyncio.create_task(manager.remove_peer("client-1"))
        try:
            await entered.wait()
            self.assertFalse(cleanup.done())
            self.assertFalse(current_ws.closed)
        finally:
            release.set()
            await cleanup
        self.assertEqual(["client-1"], closed)
        self.assertTrue(current_ws.closed)
        self.assertFalse(other_ws.closed)
        self.assertFalse(server_ws.closed)
        self.assertEqual({"client-2": "server-1"}, manager.sessions)

    async def test_failed_peer_cleanup_retains_session_for_retry(self):
        permit_close = False

        async def close_peer(uid):
            if not permit_close:
                raise RuntimeError("RTC closure failed")

        with mock.patch.dict(os.environ, {"SHINTO_PERSISTENT_SESSION": "1"}):
            manager = WebRTCPeerManagement(FakeOptions(), close_peer=close_peer)
        client_ws, server_ws = FakeWebSocket(), FakeWebSocket()
        manager.peers = {
            "client-1": make_peer("client-1", client_ws, "client", "controller"),
            "server-1": make_peer("server-1", server_ws, "server"),
        }
        manager.sessions = {"client-1": "server-1"}

        with self.assertRaisesRegex(RuntimeError, "RTC closure failed"):
            await manager.remove_peer("client-1")
        self.assertFalse(client_ws.closed)
        self.assertFalse(server_ws.closed)
        self.assertIn("client-1", manager.peers)
        self.assertEqual({"client-1": "server-1"}, manager.sessions)

        permit_close = True
        await manager.remove_peer("client-1")
        self.assertTrue(client_ws.closed)
        self.assertFalse(server_ws.closed)
        self.assertNotIn("client-1", manager.peers)
        self.assertEqual({}, manager.sessions)

    async def test_default_controller_disconnect_closes_server(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            manager = WebRTCPeerManagement(FakeOptions(), close_peer=mock.AsyncMock())

        controller_ws = FakeWebSocket()
        server_ws = FakeWebSocket()
        manager.peers = {
            "client-1": make_peer("client-1", controller_ws, "client", "controller"),
            "server-1": make_peer("server-1", server_ws, "server"),
        }
        manager.sessions = {"client-1": "server-1"}

        await manager.cleanup_session("client-1")

        self.assertEqual([(1000, b"Connection closed")], server_ws.close_calls)


if __name__ == "__main__":
    unittest.main()
