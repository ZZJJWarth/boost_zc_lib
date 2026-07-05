#!/usr/bin/env python3

from __future__ import annotations

import ctypes
import os
import time

import iceoryx2 as iox2

from py_iceoryx_direct_common import (
    DEFAULT_CHANNELS,
    DEFAULT_HEIGHT,
    DEFAULT_SAMPLE_INTERVAL_MS,
    DEFAULT_TOTAL_MSGS,
    DEFAULT_WIDTH,
    ImageHeader,
    SERVICE_NAME,
    encode_fixed_string,
    payload_size,
)


def main() -> None:
    width = int(os.getenv("MSG_WIDTH", str(DEFAULT_WIDTH)))
    height = int(os.getenv("MSG_HEIGHT", str(DEFAULT_HEIGHT)))
    channels = int(os.getenv("MSG_CHANNELS", str(DEFAULT_CHANNELS)))
    total_msgs = int(os.getenv("TOTAL_MSGS", str(DEFAULT_TOTAL_MSGS)))
    sample_interval_ms = int(os.getenv("SAMPLE_INTERVAL_MS", str(DEFAULT_SAMPLE_INTERVAL_MS)))

    image_bytes_len = payload_size(width, height, channels)
    image_bytes = bytes([0xFF]) * image_bytes_len

    iox2.set_log_level_from_env_or(iox2.LogLevel.Info)
    node = iox2.NodeBuilder.new().create(iox2.ServiceType.Ipc)

    service = (
        node.service_builder(iox2.ServiceName.new(SERVICE_NAME))
        .publish_subscribe(iox2.Slice[ctypes.c_uint8])
        .user_header(ImageHeader)
        .max_publishers(1)
        .max_subscribers(1)
        .enable_safe_overflow(False)
        .history_size(0)
        .subscriber_max_buffer_size(1)
        .subscriber_max_borrowed_samples(1)
        .open_or_create()
    )

    publisher = (
        service.publisher_builder()
        .max_loaned_samples(1)
        .initial_max_slice_len(image_bytes_len)
        .create()
    )

    cycle_time = iox2.Duration.from_millis(sample_interval_ms)
    counter = 0

    print(
        "Publisher ready: "
        f"{width}x{height} channels={channels}, total_msgs={total_msgs}, "
        f"payload_bytes={image_bytes_len}, interval_ms={sample_interval_ms}"
    )

    try:
        while counter < total_msgs:
            node.wait(cycle_time)
            counter += 1

            sample = publisher.loan_slice_uninit(image_bytes_len)
            tmp = time.time_ns()
            ctypes.memmove(sample.payload_ptr, image_bytes, image_bytes_len)
            header = sample.user_header().contents
            header.stamp_ns = tmp

            header.width = width
            header.height = height
            header.channels = channels
            header.frame_id = encode_fixed_string(f"frame_{counter}")

            sample = sample.assume_init()
            sample.send()

            if counter == 1 or counter == total_msgs or counter % 10 == 0:
                print(f"Send sample {counter} with {image_bytes_len} bytes ...")

    except iox2.NodeWaitFailure:
        print("exit")


if __name__ == "__main__":
    main()
