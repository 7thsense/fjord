#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

"""Compare a broker's ApiVersions v0 response with the public capability data."""

from __future__ import annotations

import argparse
import json
import socket
import struct
import sys
import time
from pathlib import Path
from typing import NoReturn


API_VERSIONS_KEY = 18
API_VERSIONS_VERSION = 0
CORRELATION_ID = 0x464A4F52
MAX_RESPONSE_BYTES = 8 * 1024 * 1024


class CheckError(Exception):
    """An invalid manifest, request, or broker response."""


def fail(message: str) -> NoReturn:
    raise CheckError(message)


def parse_bootstrap(value: str) -> tuple[str, int]:
    if value.startswith("["):
        close = value.find("]")
        if close < 0 or close + 1 >= len(value) or value[close + 1] != ":":
            fail(f"invalid bootstrap address: {value!r}")
        host = value[1:close]
        port_text = value[close + 2 :]
    else:
        host, separator, port_text = value.rpartition(":")
        if not separator:
            fail(f"bootstrap address must be host:port: {value!r}")

    if not host:
        fail(f"bootstrap host is empty: {value!r}")
    try:
        port = int(port_text)
    except ValueError:
        fail(f"bootstrap port is not an integer: {port_text!r}")
    if not 1 <= port <= 65535:
        fail(f"bootstrap port is outside 1-65535: {port}")
    return host, port


def load_expected(path: Path) -> tuple[dict[int, tuple[int, int]], dict[int, str]]:
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        fail(f"cannot read capability manifest {path}: {error}")

    if not isinstance(manifest, dict):
        fail("capability manifest must be a JSON object")
    capabilities = manifest.get("capabilities")
    if not isinstance(capabilities, list):
        fail("capability manifest field 'capabilities' must be an array")

    expected: dict[int, tuple[int, int]] = {}
    names: dict[int, str] = {}
    for index, capability in enumerate(capabilities):
        if not isinstance(capability, dict):
            fail(f"capabilities[{index}] must be an object")
        advertised = capability.get("advertised_kafka_api")
        if advertised is None:
            continue
        if not isinstance(advertised, dict):
            fail(f"capabilities[{index}].advertised_kafka_api must be an object")

        key = advertised.get("key")
        minimum = advertised.get("min_version")
        maximum = advertised.get("max_version")
        name = advertised.get("name")
        if not all(type(value) is int for value in (key, minimum, maximum)):
            fail(f"capabilities[{index}] has a non-integer API key or version")
        if not isinstance(name, str) or not name:
            fail(f"capabilities[{index}] has an invalid API name")
        if not -32768 <= key <= 32767:
            fail(f"API key {key} is outside the Kafka int16 range")
        if not -32768 <= minimum <= maximum <= 32767:
            fail(f"API key {key} has invalid version range {minimum}-{maximum}")
        if key in expected:
            fail(f"capability manifest contains duplicate advertised API key {key}")
        expected[key] = (minimum, maximum)
        names[key] = name

    if not expected:
        fail("capability manifest contains no advertised Kafka APIs")
    return expected, names


def receive_exact(connection: socket.socket, size: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        chunk = connection.recv(size - len(chunks))
        if not chunk:
            fail(
                f"broker closed the connection after {len(chunks)} of {size} response bytes"
            )
        chunks.extend(chunk)
    return bytes(chunks)


class Decoder:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.offset = 0

    def unpack(self, format_string: str, description: str) -> tuple[int, ...]:
        size = struct.calcsize(format_string)
        if self.offset + size > len(self.payload):
            fail(f"ApiVersions response ended while reading {description}")
        values = struct.unpack_from(format_string, self.payload, self.offset)
        self.offset += size
        return values


def request_api_versions(
    host: str, port: int, timeout: float, wait_seconds: float
) -> dict[int, tuple[int, int]]:
    client_id = b"fjord-docs"
    request = struct.pack(
        ">hhi", API_VERSIONS_KEY, API_VERSIONS_VERSION, CORRELATION_ID
    )
    request += struct.pack(">h", len(client_id)) + client_id
    frame = struct.pack(">i", len(request)) + request

    deadline = time.monotonic() + wait_seconds
    connection: socket.socket | None = None
    while connection is None:
        try:
            connection = socket.create_connection((host, port), timeout=timeout)
        except OSError as error:
            if time.monotonic() >= deadline:
                fail(f"cannot connect to {host}:{port}: {error}")
            time.sleep(0.1)

    with connection:
        connection.settimeout(timeout)
        try:
            connection.sendall(frame)
            response_size = struct.unpack(">i", receive_exact(connection, 4))[0]
            if not 0 < response_size <= MAX_RESPONSE_BYTES:
                fail(f"broker returned invalid response size {response_size}")
            payload = receive_exact(connection, response_size)
        except socket.timeout:
            fail(f"timed out exchanging ApiVersions with {host}:{port}")
        except OSError as error:
            fail(f"ApiVersions exchange with {host}:{port} failed: {error}")

    decoder = Decoder(payload)
    correlation_id = decoder.unpack(">i", "correlation ID")[0]
    if correlation_id != CORRELATION_ID:
        fail(
            f"correlation ID mismatch: expected {CORRELATION_ID}, got {correlation_id}"
        )
    error_code = decoder.unpack(">h", "error code")[0]
    if error_code != 0:
        fail(f"broker returned ApiVersions error code {error_code}")
    count = decoder.unpack(">i", "API array length")[0]
    if count < 0:
        fail(f"broker returned invalid API array length {count}")
    if count > (len(payload) - decoder.offset) // 6:
        fail(f"API array length {count} exceeds the response payload")

    actual: dict[int, tuple[int, int]] = {}
    for _ in range(count):
        key, minimum, maximum = decoder.unpack(">hhh", "API version entry")
        if minimum > maximum:
            fail(f"broker returned invalid range for API key {key}: {minimum}-{maximum}")
        if key in actual:
            fail(f"broker returned duplicate API key {key}")
        actual[key] = (minimum, maximum)
    if decoder.offset != len(payload):
        fail(f"ApiVersions v0 response has {len(payload) - decoder.offset} trailing bytes")
    return actual


def format_api(key: int, version_range: tuple[int, int], names: dict[int, str]) -> str:
    name = names.get(key, "unknown")
    return f"key {key} ({name}) v{version_range[0]}-v{version_range[1]}"


def compare_versions(
    expected: dict[int, tuple[int, int]],
    actual: dict[int, tuple[int, int]],
    names: dict[int, str],
) -> None:
    differences: list[str] = []
    for key in sorted(expected.keys() - actual.keys()):
        differences.append(f"missing: {format_api(key, expected[key], names)}")
    for key in sorted(actual.keys() - expected.keys()):
        differences.append(f"unexpected: {format_api(key, actual[key], names)}")
    for key in sorted(expected.keys() & actual.keys()):
        if expected[key] != actual[key]:
            differences.append(
                f"range mismatch: key {key} ({names[key]}) expected "
                f"v{expected[key][0]}-v{expected[key][1]}, got "
                f"v{actual[key][0]}-v{actual[key][1]}"
            )
    if differences:
        fail("advertised API set differs from capability manifest:\n  " + "\n  ".join(differences))


def main() -> int:
    repository = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="127.0.0.1:9092")
    parser.add_argument(
        "--manifest",
        type=Path,
        default=repository / "docs/public/data/capabilities.json",
    )
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument(
        "--wait-seconds",
        type=float,
        default=0.0,
        help="retry connection failures for this many seconds",
    )
    arguments = parser.parse_args()
    if arguments.timeout <= 0 or arguments.wait_seconds < 0:
        parser.error("--timeout must be positive and --wait-seconds must be non-negative")

    try:
        host, port = parse_bootstrap(arguments.bootstrap)
        expected, names = load_expected(arguments.manifest)
        actual = request_api_versions(
            host, port, arguments.timeout, arguments.wait_seconds
        )
        compare_versions(expected, actual, names)
    except CheckError as error:
        print(f"ApiVersions check failed: {error}", file=sys.stderr)
        return 1

    print(
        f"ApiVersions check passed: {host}:{port} advertised "
        f"{len(actual)} expected APIs"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
