#!/usr/bin/env python3

from __future__ import annotations

import os

import rclpy
from rclpy.node import Node
from sensor_msgs.msg import Image

from cpp_pubsub_zc import ZcPyPublisher, init_shm, shutdown_shm


class MinimalPyPublisher(Node):
    def __init__(self) -> None:
        super().__init__("minimal_py_publisher")

        self._pub = ZcPyPublisher(self, "topic", Image, 10)
        self._count = 0
        self._max_count = int(os.getenv("TOTAL_MSGS", "200"))
        self._width = int(os.getenv("MSG_WIDTH", "1920"))
        self._height = int(os.getenv("MSG_HEIGHT", "1080"))
        self._channels = int(os.getenv("MSG_CHANNELS", "3"))
        sample_interval_ms = int(os.getenv("SAMPLE_INTERVAL_MS", "34"))

        self._timer = self.create_timer(sample_interval_ms / 1000.0, self._timer_callback)
        self._shutdown_requested = False
        self.get_logger().info(
            "Publisher image config: "
            f"{self._width}x{self._height} channels={self._channels}, total_msgs={self._max_count}, "
            f"interval_ms={sample_interval_ms}"
        )
        self.msg = Image()
        self.msg.width = self._width
        self.msg.height = self._height
        self.msg.encoding = "rgb8"
        self.msg.is_bigendian = 0
        self.msg.step = self._width * self._channels
        self.msg.data = bytes([0xFF]) * (self._height * self.msg.step)

    def _timer_callback(self) -> None:
        if self._shutdown_requested:
            return

        if self._count == 0 and self.count_subscribers("topic") == 0:
            return

        self.msg.header.stamp = self.get_clock().now().to_msg()
        self.msg.header.frame_id = f"frame_{self._count}"

        published = self._pub.publish(self.msg)
        if not published:
            self.get_logger().info("No subscribers, skip publish")
            return

        self._count += 1
        self.get_logger().info(f"Publishing image count={self._count}")

        if self._count >= self._max_count:
            self._shutdown_requested = True
            self._timer.cancel()
            rclpy.shutdown()


def main() -> None:
    init_shm()
    rclpy.init()

    node = MinimalPyPublisher()
    try:
        rclpy.spin(node)
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()
        shutdown_shm()


if __name__ == "__main__":
    main()
