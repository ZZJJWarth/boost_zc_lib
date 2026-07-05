#!/usr/bin/env python3

from __future__ import annotations

import ctypes
import math
import os
import time

import iceoryx2 as iox2
from sensor_msgs.msg import Image

from py_iceoryx_direct_common import (
    DEFAULT_CHANNELS,
    DEFAULT_HEIGHT,
    DEFAULT_SAMPLE_INTERVAL_MS,
    DEFAULT_TOTAL_MSGS,
    DEFAULT_WIDTH,
    ImageHeader,
    SERVICE_NAME,
    decode_fixed_string,
)


def sample_to_image(sample: iox2.Sample, header: ImageHeader) -> Image:
    payload_len = sample.header.number_of_elements
    payload = ctypes.string_at(sample.payload_ptr, payload_len)
    image = Image()
    image.header.stamp.sec = int(header.stamp_ns // 1_000_000_000)
    image.header.stamp.nanosec = int(header.stamp_ns % 1_000_000_000)
    image.header.frame_id = decode_fixed_string(header.frame_id)
    image.width = int(header.width)
    image.height = int(header.height)
    image.encoding = "rgb8"
    image.is_bigendian = 0
    image.step = int(header.width) * int(header.channels)
    image.data = payload
    return image


def main() -> None:
    width = int(os.getenv("MSG_WIDTH", str(DEFAULT_WIDTH)))
    height = int(os.getenv("MSG_HEIGHT", str(DEFAULT_HEIGHT)))
    channels = int(os.getenv("MSG_CHANNELS", str(DEFAULT_CHANNELS)))
    expected_messages = int(os.getenv("TOTAL_MSGS", str(DEFAULT_TOTAL_MSGS)))
    sample_interval_ms = int(os.getenv("SAMPLE_INTERVAL_MS", str(DEFAULT_SAMPLE_INTERVAL_MS)))
    subscriber_idle_wait_ms = int(os.getenv("SUBSCRIBER_IDLE_WAIT_MS", "1"))

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

    subscriber = service.subscriber_builder().create()
    idle_wait = iox2.Duration.from_millis(subscriber_idle_wait_ms)

    received_count = 0
    sum_latency_ms = 0.0
    min_latency_ms = math.inf
    max_latency_ms = 0.0

    print(
        "Subscriber ready: "
        f"{width}x{height} channels={channels}, expected_messages={expected_messages}, "
        f"sample_interval_ms={sample_interval_ms}, subscriber_idle_wait_ms={subscriber_idle_wait_ms}"
    )

    try:
        while received_count < expected_messages:
            drained_any = False

            while True:
                sample = subscriber.receive()
                if sample is None:
                    break

                drained_any = True

                header = sample.user_header().contents
                image = sample_to_image(sample, header)
                latency_ms = (time.time_ns() - header.stamp_ns) / 1_000_000.0

                received_count += 1
                sum_latency_ms += latency_ms
                min_latency_ms = min(min_latency_ms, latency_ms)
                max_latency_ms = max(max_latency_ms, latency_ms)

                payload_len = len(image.data)
                frame_id = image.header.frame_id
                if received_count == 1 or received_count == expected_messages or received_count % 10 == 0:
                    print(
                        f"received: {payload_len} bytes, frame_id={frame_id}, "
                        f"count={received_count}, latency={latency_ms:.3f} ms"
                    )

                sample.delete()

                if received_count >= expected_messages:
                    break

            # Only wait when there is no data currently available. This avoids
            # adding an extra fixed delay before processing queued samples.
            if received_count < expected_messages and not drained_any:
                node.wait(idle_wait)

        avg_latency_ms = sum_latency_ms / received_count if received_count else 0.0
        print(
            "Latency summary "
            f"({received_count} msgs): avg={avg_latency_ms:.3f} ms, "
            f"min={min_latency_ms:.3f} ms, max={max_latency_ms:.3f} ms"
        )

    except iox2.NodeWaitFailure:
        print("exit")


if __name__ == "__main__":
    main()
