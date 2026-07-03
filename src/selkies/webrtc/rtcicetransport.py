# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# This file incorporates work covered by the following copyright and
# permission notice:
#
#   Copyright (c) Jeremy Lainé.
#   All rights reserved.
#
#   Redistribution and use in source and binary forms, with or without
#   modification, are permitted provided that the following conditions are met:
#
#       * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#       * Redistributions in binary form must reproduce the above copyright notice,
#       this list of conditions and the following disclaimer in the documentation
#       and/or other materials provided with the distribution.
#       * Neither the name of aiortc nor the names of its contributors may
#       be used to endorse or promote products derived from this software without
#       specific prior written permission.
#
#   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
#   ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
#   WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#   DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
#   FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
#   DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
#   SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
#   CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
#   OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
#   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import asyncio
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Optional

import aioice.ice as aioice_ice
from aioice import Candidate, Connection as AioIceConnection, ConnectionClosed
from pyee.asyncio import AsyncIOEventEmitter

from .exceptions import InvalidStateError
from .rtcconfiguration import RTCIceServer

# See https://datatracker.ietf.org/doc/html/rfc7064
# transport is not defined by RFC 7064 and rejected by browsers.
STUN_REGEX = re.compile(
    r"(?P<scheme>stun|stuns)\:(?P<host>[^?:]+)(\:(?P<port>[0-9]+?))?"
    r"(\?transport=(?P<transport>.*))?"
)
# See https://datatracker.ietf.org/doc/html/rfc7065
TURN_REGEX = re.compile(
    r"(?P<scheme>turn|turns)\:(?P<host>[^?:]+)(\:(?P<port>[0-9]+?))?"
    r"(\?transport=(?P<transport>.*))?"
)

logger = logging.getLogger(__name__)


@dataclass
class RTCIceCandidate:
    """
    The :class:`RTCIceCandidate` interface represents a candidate Interactive
    Connectivity Establishment (ICE) configuration which may be used to
    establish an RTCPeerConnection.
    """

    component: int
    foundation: str
    ip: str
    port: int
    priority: int
    protocol: str
    type: str
    relatedAddress: Optional[str] = None
    relatedPort: Optional[int] = None
    sdpMid: Optional[str] = None
    sdpMLineIndex: Optional[int] = None
    tcpType: Optional[str] = None


@dataclass
class RTCIceParameters:
    """
    The :class:`RTCIceParameters` dictionary includes the ICE username
    fragment and password and other ICE-related parameters.
    """

    usernameFragment: Optional[str] = None
    "ICE username fragment."

    password: Optional[str] = None
    "ICE password."

    iceLite: bool = False


def candidate_from_aioice(x: Candidate) -> RTCIceCandidate:
    return RTCIceCandidate(
        component=x.component,
        foundation=x.foundation,
        ip=x.host,
        port=x.port,
        priority=x.priority,
        protocol=x.transport,
        relatedAddress=x.related_address,
        relatedPort=x.related_port,
        tcpType=x.tcptype,
        type=x.type,
    )


def candidate_to_aioice(x: RTCIceCandidate) -> Candidate:
    return Candidate(
        component=x.component,
        foundation=x.foundation,
        host=x.ip,
        port=x.port,
        priority=x.priority,
        related_address=x.relatedAddress,
        related_port=x.relatedPort,
        transport=x.protocol,
        tcptype=x.tcpType,
        type=x.type,
    )


def connection_kwargs(servers: list[RTCIceServer]) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}

    for server in servers:
        if isinstance(server.urls, list):
            uris = server.urls
        else:
            uris = [server.urls]

        for uri in uris:
            parsed = parse_stun_turn_uri(uri)

            if parsed["scheme"] == "stun":
                # only a single STUN server is supported
                if "stun_server" in kwargs:
                    continue

                kwargs["stun_server"] = (parsed["host"], parsed["port"])
            elif parsed["scheme"] in ["turn", "turns"]:
                # only a single TURN server is supported
                if "turn_server" in kwargs:
                    continue

                # only 'udp' and 'tcp' transports are supported
                if parsed["scheme"] == "turn" and parsed["transport"] not in [
                    "udp",
                    "tcp",
                ]:
                    continue
                elif parsed["scheme"] == "turns" and parsed["transport"] != "tcp":
                    continue

                # only 'password' credentialType is supported
                if server.credentialType != "password":
                    continue

                kwargs["turn_server"] = (parsed["host"], parsed["port"])
                kwargs["turn_ssl"] = parsed["scheme"] == "turns"
                kwargs["turn_transport"] = parsed["transport"]
                kwargs["turn_username"] = server.username
                kwargs["turn_password"] = server.credential

    return kwargs


def parse_stun_turn_uri(uri: str) -> dict[str, Any]:
    if uri.startswith("stun"):
        match = STUN_REGEX.fullmatch(uri)
    elif uri.startswith("turn"):
        match = TURN_REGEX.fullmatch(uri)
    else:
        raise ValueError("malformed uri: invalid scheme")

    if not match:
        raise ValueError("malformed uri")

    # set port
    parsed: dict[str, Any] = match.groupdict()
    if parsed["port"]:
        parsed["port"] = int(parsed["port"])
    elif parsed["scheme"] in ["stuns", "turns"]:
        parsed["port"] = 5349
    else:
        parsed["port"] = 3478

    # set transport
    if parsed["scheme"] == "turn" and not parsed["transport"]:
        parsed["transport"] = "udp"
    elif parsed["scheme"] == "turns" and not parsed["transport"]:
        parsed["transport"] = "tcp"
    elif parsed["scheme"] in ["stun", "stuns"]:
        if parsed["transport"] is not None:
            raise ValueError(
                "malformed uri: " + parsed["scheme"] + " must not contain transport"
            )
        del parsed["transport"]

    return parsed


def _shinto_port_env(name: str) -> Optional[int]:
    raw = os.environ.get(name, "").strip()
    if raw == "":
        return None
    try:
        port = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if port < 1 or port > 65535:
        raise ValueError(f"{name} must be between 1 and 65535")
    return port


def _shinto_local_port_range() -> Optional[range]:
    min_port = _shinto_port_env("SELKIES_MIN_RTP_PORT")
    max_port = _shinto_port_env("SELKIES_MAX_RTP_PORT")
    if min_port is None and max_port is None:
        return None
    if min_port is None or max_port is None:
        raise ValueError("SELKIES_MIN_RTP_PORT and SELKIES_MAX_RTP_PORT must be set together")
    if min_port > max_port:
        raise ValueError("SELKIES_MIN_RTP_PORT must be less than or equal to SELKIES_MAX_RTP_PORT")
    return range(min_port, max_port + 1)


class ShintoBoundPortConnection(AioIceConnection):
    _shinto_bounded_rtp_ports = True

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._shinto_local_port_range = _shinto_local_port_range()
        self._shinto_next_port_index = 0
        if self._shinto_local_port_range is not None:
            logger.info(
                "Shinto bounded RTP local port range configured: %s-%s",
                self._shinto_local_port_range.start,
                self._shinto_local_port_range.stop - 1,
            )

    async def _shinto_create_host_endpoint(self, loop: Any, address: str) -> Any:
        if self._shinto_local_port_range is None:
            return await loop.create_datagram_endpoint(
                lambda: aioice_ice.StunProtocol(self), local_addr=(address, 0)
            )

        last_error = None
        ports = self._shinto_local_port_range
        for offset in range(len(ports)):
            index = (self._shinto_next_port_index + offset) % len(ports)
            port = ports[index]
            try:
                transport, protocol = await loop.create_datagram_endpoint(
                    lambda: aioice_ice.StunProtocol(self), local_addr=(address, port)
                )
            except OSError as exc:
                last_error = exc
                continue
            self._shinto_next_port_index = (index + 1) % len(ports)
            return transport, protocol
        if last_error is not None:
            raise last_error
        raise OSError("no ports available in Shinto bounded RTP range")

    async def get_component_candidates(
        self, component: int, addresses: list[str], timeout: int = 5
    ) -> list[Candidate]:
        candidates = []
        loop = asyncio.get_event_loop()

        host_protocols = []
        for address in addresses:
            try:
                transport, protocol = await self._shinto_create_host_endpoint(loop, address)
                sock = transport.get_extra_info("socket")
                if sock is not None:
                    sock.setsockopt(
                        aioice_ice.socket.SOL_SOCKET,
                        aioice_ice.socket.SO_RCVBUF,
                        aioice_ice.turn.UDP_SOCKET_BUFFER_SIZE,
                    )
            except OSError as exc:
                self._Connection__log_info("Could not bind to %s - %s", address, exc)
                continue
            host_protocols.append(protocol)

            candidate_address = protocol.transport.get_extra_info("sockname")
            protocol.local_candidate = Candidate(
                foundation=aioice_ice.candidate_foundation("host", "udp", candidate_address[0]),
                component=component,
                transport="udp",
                priority=aioice_ice.candidate_priority(component, "host"),
                host=candidate_address[0],
                port=candidate_address[1],
                type="host",
            )
            if self._transport_policy == aioice_ice.TransportPolicy.ALL:
                candidates.append(protocol.local_candidate)
        self._protocols += host_protocols

        tasks: list[asyncio.Task[tuple[Candidate, Optional[aioice_ice.StunProtocol]]]] = []
        if self.stun_server:
            for protocol in host_protocols:
                if aioice_ice.ipaddress.ip_address(protocol.local_candidate.host).version == 4:
                    tasks.append(asyncio.create_task(aioice_ice.server_reflexive_candidate(protocol, self.stun_server)))
        if self.turn_server:
            tasks.append(asyncio.create_task(aioice_ice.relayed_candidate(
                component=component,
                protocol_factory=lambda: aioice_ice.StunProtocol(self),
                turn_server=self.turn_server,
                turn_username=self.turn_username,
                turn_password=self.turn_password,
                turn_ssl=self.turn_ssl,
                turn_transport=self.turn_transport,
            )))

        if len(tasks):
            done, pending = await asyncio.wait(tasks, timeout=timeout)
            for task in done:
                if task.exception() is None:
                    candidate, protocol = task.result()
                    candidates.append(candidate)
                    if protocol:
                        self._protocols.append(protocol)
            for task in pending:
                task.cancel()
        return candidates


class RTCIceGatherer(AsyncIOEventEmitter):
    """
    The :class:`RTCIceGatherer` interface gathers local host, server reflexive
    and relay candidates, as well as enabling the retrieval of local
    Interactive Connectivity Establishment (ICE) parameters which can be
    exchanged in signaling.
    """

    def __init__(
        self,
        iceServers: Optional[list[RTCIceServer]] = None,
        local_username: Optional[str] = None,
        local_password: Optional[str] = None,
    ) -> None:
        super().__init__()

        if iceServers is None:
            iceServers = self.getDefaultIceServers()
        ice_kwargs = connection_kwargs(iceServers)

        self._connection = ShintoBoundPortConnection(ice_controlling=False, **ice_kwargs)
        self._remote_candidates_end = False
        self.__state = "new"

    @property
    def state(self) -> str:
        """
        The current state of the ICE gatherer.
        """
        return self.__state

    async def gather(self) -> None:
        """
        Gather ICE candidates.
        """
        if self.__state == "new":
            self.__setState("gathering")
            await self._connection.gather_candidates()
            self.__setState("completed")

    @classmethod
    def getDefaultIceServers(cls) -> list[RTCIceServer]:
        """
        Return the list of default :class:`RTCIceServer`.
        """
        return [RTCIceServer("stun:stun.l.google.com:19302")]

    def getLocalCandidates(self) -> list[RTCIceCandidate]:
        """
        Retrieve the list of valid local candidates associated with the ICE
        gatherer.
        """
        return [candidate_from_aioice(x) for x in self._connection.local_candidates]

    def getLocalParameters(self) -> RTCIceParameters:
        """
        Retrieve the ICE parameters of the ICE gatherer.

        :rtype: RTCIceParameters
        """
        return RTCIceParameters(
            usernameFragment=self._connection.local_username,
            password=self._connection.local_password,
        )

    def __setState(self, state: str) -> None:
        self.__state = state
        self.emit("statechange")


class RTCIceTransport(AsyncIOEventEmitter):
    """
    The :class:`RTCIceTransport` interface allows an application access to
    information about the Interactive Connectivity Establishment (ICE)
    transport over which packets are sent and received.

    :param gatherer: An :class:`RTCIceGatherer`.
    """

    def __init__(self, gatherer: RTCIceGatherer) -> None:
        super().__init__()
        self.__iceGatherer = gatherer
        self.__monitor_task: Optional[asyncio.Future[None]] = None
        self.__start: Optional[asyncio.Event] = None
        self.__state = "new"
        self._connection = gatherer._connection
        self._role_set = False

        # expose recv / send methods
        self._recv = self._connection.recv
        self._send = self._connection.send

    @property
    def iceGatherer(self) -> RTCIceGatherer:
        """
        The ICE gatherer passed in the constructor.
        """
        return self.__iceGatherer

    @property
    def role(self) -> str:
        """
        The current role of the ICE transport.

        Either `'controlling'` or `'controlled'`.
        """
        if self._connection.ice_controlling:
            return "controlling"
        else:
            return "controlled"

    @property
    def state(self) -> str:
        """
        The current state of the ICE transport.
        """
        return self.__state

    async def addRemoteCandidate(self, candidate: Optional[RTCIceCandidate]) -> None:
        """
        Add a remote candidate.

        :param candidate: The new candidate or `None` to signal end of candidates.
        """
        if not self.__iceGatherer._remote_candidates_end:
            if candidate is None:
                self.__iceGatherer._remote_candidates_end = True
                await self._connection.add_remote_candidate(None)
            else:
                await self._connection.add_remote_candidate(
                    candidate_to_aioice(candidate)
                )

    def getRemoteCandidates(self) -> list[RTCIceCandidate]:
        """
        Retrieve the list of candidates associated with the remote
        :class:`RTCIceTransport`.
        """
        return [candidate_from_aioice(x) for x in self._connection.remote_candidates]

    async def start(self, remoteParameters: RTCIceParameters) -> None:
        """
        Initiate connectivity checks.

        :param remoteParameters: The :class:`RTCIceParameters` associated with
                                  the remote :class:`RTCIceTransport`.
        """
        if self.state == "closed":
            raise InvalidStateError("RTCIceTransport is closed")

        # handle the case where start is already in progress
        if self.__start is not None:
            await self.__start.wait()
            return
        self.__start = asyncio.Event()
        self.__monitor_task = asyncio.ensure_future(self._monitor())

        self.__setState("checking")
        self._connection.remote_is_lite = remoteParameters.iceLite
        self._connection.remote_username = remoteParameters.usernameFragment
        self._connection.remote_password = remoteParameters.password
        try:
            await self._connection.connect()
        except ConnectionError:
            self.__setState("failed")
        else:
            self.__setState("completed")
        self.__start.set()

    async def stop(self) -> None:
        """
        Irreversibly stop the :class:`RTCIceTransport`.
        """
        if self.state != "closed":
            self.__setState("closed")
            await self._connection.close()
            if self.__monitor_task is not None:
                await self.__monitor_task
                self.__monitor_task = None

    async def _monitor(self) -> None:
        while True:
            event = await self._connection.get_event()
            if isinstance(event, ConnectionClosed):
                if self.state == "completed":
                    self.__setState("failed")
                return

    def __log_debug(self, msg: str, *args: object) -> None:
        logger.debug(f"RTCIceTransport(%s) {msg}", self.role, *args)

    def __setState(self, state: str) -> None:
        if state != self.__state:
            self.__log_debug("- %s -> %s", self.__state, state)
            self.__state = state
            self.emit("statechange")

            # no more events will be emitted, so remove all event listeners
            # to facilitate garbage collection.
            if state == "closed":
                self.iceGatherer.remove_all_listeners()
                self.remove_all_listeners()
