#!/usr/bin/env python3

from __future__ import annotations

import math
import os

import rclpy
from rclpy.node import Node
from sensor_msgs.msg import Image


class MinimalPyNormalSubscriber(Node):
    def __init__(self) -> None:
        super().__init__("minimal_py_normal_subscriber")

        self._sub = self.create_subscription(Image, "topic", self._topic_callback, 10)
        self._expected_messages = int(os.getenv("TOTAL_MSGS", "200"))
        self._received_count = 0
        self._sum_latency_ms = 0.0
        self._min_latency_ms = math.inf
        self._max_latency_ms = 0.0

    def _topic_callback(self, msg: Image) -> None:
        callback_start_ns = self.get_clock().now().nanoseconds
        publish_ns = msg.header.stamp.sec * 1_000_000_000 + msg.header.stamp.nanosec
        latency_ms = (callback_start_ns - publish_ns) / 1_000_000.0

        self._received_count += 1
        self._sum_latency_ms += latency_ms
        self._min_latency_ms = min(self._min_latency_ms, latency_ms)
        self._max_latency_ms = max(self._max_latency_ms, latency_ms)

        self.get_logger().info(f"I heard image: {msg.height}x{msg.width}")
        self.get_logger().info(
            f"Latency: {latency_ms:.3f} ms, frame_id={msg.header.frame_id}, count={self._received_count}"
        )

        if self._received_count >= self._expected_messages:
            avg_latency_ms = self._sum_latency_ms / self._received_count
            self.get_logger().info(
                "Latency summary "
                f"({self._received_count} msgs): avg={avg_latency_ms:.3f} ms, "
                f"min={self._min_latency_ms:.3f} ms, max={self._max_latency_ms:.3f} ms"
            )
            rclpy.shutdown()


def main() -> None:
    rclpy.init()

    node = MinimalPyNormalSubscriber()
    try:
        rclpy.spin(node)
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()
