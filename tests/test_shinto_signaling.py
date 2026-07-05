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
            manager = WebRTCPeerManagement(FakeOptions())

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
            manager = WebRTCPeerManagement(FakeOptions())

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

    async def test_persistent_session_does_not_send_viewer_session_end(self):
        with mock.patch.dict(os.environ, {"SHINTO_PERSISTENT_SESSION": "true"}):
            manager = WebRTCPeerManagement(FakeOptions())

        viewer_ws = FakeWebSocket()
        server_ws = FakeWebSocket()
        manager.peers = {
            "client-1": make_peer("client-1", viewer_ws, "client", "viewer"),
            "server-1": make_peer("server-1", server_ws, "server"),
        }
        manager.sessions = {"client-1": "server-1"}

        await manager.cleanup_session("client-1")

        self.assertEqual([], server_ws.sent)

    async def test_default_controller_disconnect_closes_server(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            manager = WebRTCPeerManagement(FakeOptions())

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
