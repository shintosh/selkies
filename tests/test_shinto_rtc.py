import asyncio
import sys
import types
import unittest
from unittest import mock
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class FakeTrack:
    def __init__(self, *args, **kwargs):
        pass


class FakeRTCPeerConnection:
    pass


class FakeRTCDataChannel:
    pass


class FakeRTCRtpSender:
    @staticmethod
    def getCapabilities(kind):
        return types.SimpleNamespace(codecs=[])


fake_webrtc = types.ModuleType("selkies.webrtc")
fake_webrtc.RTCPeerConnection = FakeRTCPeerConnection
fake_webrtc.RTCIceCandidate = object
fake_webrtc.RTCRtpSender = FakeRTCRtpSender
fake_webrtc.RTCSessionDescription = object
fake_webrtc.VideoStreamTrack = FakeTrack
fake_webrtc.RTCConfiguration = object
fake_webrtc.RTCIceServer = object
fake_webrtc.AudioStreamTrack = FakeTrack
fake_webrtc.RTCDataChannel = FakeRTCDataChannel
fake_webrtc.RTCBundlePolicy = types.SimpleNamespace(MAX_BUNDLE="max-bundle")

fake_ice = types.ModuleType("selkies.webrtc.rtcicetransport")
fake_ice.Candidate = types.SimpleNamespace(from_sdp=lambda value: value)
fake_ice.candidate_from_aioice = lambda candidate: candidate

fake_media = types.ModuleType("selkies.webrtc.contrib.media")
fake_media.MediaRelay = object

fake_pipeline = types.ModuleType("selkies.media_pipeline")
fake_pipeline.MediaPipeline = object

fake_av = types.ModuleType("av")
fake_av.Packet = lambda data: types.SimpleNamespace(data=data)

sys.modules.setdefault("selkies.webrtc", fake_webrtc)
sys.modules.setdefault("selkies.webrtc.rtcicetransport", fake_ice)
sys.modules.setdefault("selkies.webrtc.contrib.media", fake_media)
sys.modules.setdefault("selkies.media_pipeline", fake_pipeline)
sys.modules.setdefault("av", fake_av)

from selkies.rtc import ClientType, RTCApp


class FakePeerConnection:
    def __init__(self, state):
        self.connectionState = state


class FakeDataChannel:
    def __init__(self, state):
        self.readyState = state


class ShintoRTCTests(unittest.IsolatedAsyncioTestCase):
    def make_app(self):
        return RTCApp(asyncio.get_running_loop(), encoder="x264enc")

    async def test_pending_offer_is_closed_without_late_registration(self):
        entered, release = asyncio.Event(), asyncio.Event()
        offers = []

        class NegotiatingPeer:
            connectionState = "new"

            def on(self, event, callback):
                pass

            def addTrack(self, track):
                self.sender = types.SimpleNamespace(track=track, on=self.on)
                return self.sender

            def createDataChannel(self, *args, **kwargs):
                return types.SimpleNamespace(on=self.on)

            def getTransceivers(self):
                return [types.SimpleNamespace(sender=self.sender, setCodecPreferences=lambda codecs: None)]

            async def createOffer(self):
                entered.set()
                await release.wait()
                return types.SimpleNamespace(sdp="v=0\r\n")

            async def setLocalDescription(self, offer):
                self.localDescription = offer

            async def close(self):
                self.connectionState = "closed"

        async def publish_offer(kind, sdp, peer_id):
            offers.append(peer_id)

        app = self.make_app()
        app.shinto_audio_enabled = False
        app.stun_servers, app.turn_servers = [], []
        app.video_media = types.SimpleNamespace(kind="video")
        app.media_relay = types.SimpleNamespace(subscribe=lambda track: track)
        app.on_sdp = publish_offer
        peer = NegotiatingPeer()
        with mock.patch("selkies.rtc.RTCPeerConnection", return_value=peer), \
             mock.patch("selkies.rtc.RTCConfiguration", side_effect=lambda **kwargs: kwargs), \
             mock.patch("selkies.rtc.RTCRtpSender.getCapabilities", return_value=types.SimpleNamespace(codecs=[types.SimpleNamespace(mimeType="video/H264")])):
            starting = asyncio.create_task(app._start_rtc_pipeline("pending-viewer", "viewer"))
            try:
                await asyncio.wait_for(entered.wait(), 5)
                await app._stop_rtc_pipeline("pending-viewer")
                self.assertEqual("closed", peer.connectionState)
            finally:
                release.set()
                await starting
                await app._stop_rtc_pipeline("pending-viewer")
        self.assertEqual({}, app.peer_connections)
        self.assertEqual([], offers)

    async def test_should_accept_input_only_for_controller(self):
        app = self.make_app()
        app.peer_connections = {
            "secret-controller-id": {
                "peer_conn": FakePeerConnection("connected"),
                "data_channel": FakeDataChannel("open"),
                "client_type": ClientType.CONTROLLER,
            },
            "secret-viewer-id": {
                "peer_conn": FakePeerConnection("connected"),
                "data_channel": FakeDataChannel("open"),
                "client_type": ClientType.VIEWER,
            },
        }

        self.assertTrue(app.should_accept_input("secret-controller-id"))
        self.assertFalse(app.should_accept_input("secret-viewer-id"))
        self.assertFalse(app.should_accept_input("missing"))

    async def test_viewer_input_message_is_dropped(self):
        app = self.make_app()
        received = []

        async def on_message(msg):
            received.append(msg)

        app.on_data_message = on_message
        app.peer_connections = {
            "viewer": {
                "peer_conn": FakePeerConnection("connected"),
                "data_channel": FakeDataChannel("open"),
                "client_type": ClientType.VIEWER,
            },
        }

        await app.handle_input_data_message("mouse-click", "viewer")

        self.assertEqual([], received)

    async def test_stats_snapshot_is_bounded_and_omits_peer_ids(self):
        app = self.make_app()
        app.peer_connections = {
            "secret-controller-id": {
                "peer_conn": FakePeerConnection("connected"),
                "data_channel": FakeDataChannel("open"),
                "client_type": ClientType.CONTROLLER,
            },
            "secret-viewer-id": {
                "peer_conn": FakePeerConnection("disconnected"),
                "data_channel": FakeDataChannel("closed"),
                "client_type": ClientType.VIEWER,
            },
        }

        snapshot = app.get_shinto_stats_snapshot()
        encoded = repr(snapshot)

        self.assertNotIn("secret-controller-id", encoded)
        self.assertNotIn("secret-viewer-id", encoded)
        self.assertEqual(
            {
                "controller_present": True,
                "peer_count": 2,
                "viewer_count": 1,
                "connection_states": {"connected": 1, "disconnected": 1},
                "data_channel_states": {"open": 1, "closed": 1},
            },
            snapshot,
        )


if __name__ == "__main__":
    unittest.main()
