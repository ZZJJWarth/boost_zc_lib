#!/usr/bin/env python3

from __future__ import annotations

import ctypes
import os


SERVICE_NAME = os.getenv("IOX2_SERVICE_NAME", "CppPubSubIceoryx2Image")
FRAME_ID_CAPACITY = 64


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    return int(value)


DEFAULT_WIDTH = _env_int("MSG_WIDTH", 1920)
DEFAULT_HEIGHT = _env_int("MSG_HEIGHT", 1080)
DEFAULT_CHANNELS = _env_int("MSG_CHANNELS", 3)
DEFAULT_TOTAL_MSGS = _env_int("TOTAL_MSGS", 200)
DEFAULT_SAMPLE_INTERVAL_MS = _env_int("SAMPLE_INTERVAL_MS", 34)


class ImageHeader(ctypes.Structure):
    _fields_ = [
        ("stamp_ns", ctypes.c_uint64),
        ("width", ctypes.c_uint32),
        ("height", ctypes.c_uint32),
        ("channels", ctypes.c_uint32),
        ("frame_id", ctypes.c_char * FRAME_ID_CAPACITY),
    ]

    def __str__(self) -> str:
        return (
            "ImageHeader { "
            f"stamp_ns: {self.stamp_ns}, width: {self.width}, height: {self.height}, "
            f"channels: {self.channels}, frame_id: {decode_fixed_string(self.frame_id)} "
            "}"
        )

    @staticmethod
    def type_name() -> str:
        return "ImageHeader"


def decode_fixed_string(value: bytes | ctypes.Array[ctypes.c_char]) -> str:
    raw = bytes(value)
    return raw.split(b"\0", 1)[0].decode("utf-8", errors="replace")


def encode_fixed_string(value: str, capacity: int = FRAME_ID_CAPACITY) -> bytes:
    encoded = value.encode("utf-8")[: max(capacity - 1, 0)]
    return encoded.ljust(capacity, b"\0")


def payload_size(width: int, height: int, channels: int) -> int:
    return width * height * channels
