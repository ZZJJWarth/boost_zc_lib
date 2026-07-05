from __future__ import annotations

from typing import Any, Callable, Dict, List, Type

from rclpy.node import Node
from sensor_msgs.msg import Image, PointCloud2, PointField
from std_msgs.msg import String

from . import _zc_py_bridge as bridge


def init_shm(name: str = "MyShm", size: int = 64000000) -> None:
    bridge.init_shm(name, size)


def shutdown_shm(name: str = "MyShm") -> None:
    bridge.shutdown_shm(name)


def _normalized_topic(topic_name: str) -> str:
    return topic_name[1:] if topic_name.startswith("/") else topic_name


def _image_to_object_name(msg: Image) -> str:
    return bridge.create_image(
        msg.header.stamp.sec,
        msg.header.stamp.nanosec,
        msg.header.frame_id,
        msg.width,
        msg.height,
        msg.encoding,
        int(msg.is_bigendian),
        msg.step,
        memoryview(msg.data),
    )


def _pc2_fields_to_dicts(fields: List[PointField]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for field in fields:
        out.append(
            {
                "name": field.name,
                "offset": int(field.offset),
                "datatype": int(field.datatype),
                "count": int(field.count),
            }
        )
    return out


def _pointcloud2_to_object_name(msg: PointCloud2) -> str:
    return bridge.create_pointcloud2(
        msg.header.stamp.sec,
        msg.header.stamp.nanosec,
        msg.header.frame_id,
        msg.height,
        msg.width,
        _pc2_fields_to_dicts(list(msg.fields)),
        int(msg.is_bigendian),
        msg.point_step,
        msg.row_step,
        int(msg.is_dense),
        memoryview(msg.data),
    )


def _dict_to_image(payload: Dict[str, Any]) -> Image:
    msg = Image()
    msg.header.stamp.sec = int(payload["stamp_sec"])
    msg.header.stamp.nanosec = int(payload["stamp_nanosec"])
    msg.header.frame_id = str(payload["frame_id"])
    msg.width = int(payload["width"])
    msg.height = int(payload["height"])
    msg.encoding = str(payload["encoding"])
    msg.is_bigendian = int(payload["is_bigendian"])
    msg.step = int(payload["step"])
    msg.data = payload["data"]
    return msg


def _dict_to_pointcloud2(payload: Dict[str, Any]) -> PointCloud2:
    msg = PointCloud2()
    msg.header.stamp.sec = int(payload["stamp_sec"])
    msg.header.stamp.nanosec = int(payload["stamp_nanosec"])
    msg.header.frame_id = str(payload["frame_id"])
    msg.height = int(payload["height"])
    msg.width = int(payload["width"])

    msg.fields = []
    for field in payload["fields"]:
        out_field = PointField()
        out_field.name = str(field["name"])
        out_field.offset = int(field["offset"])
        out_field.datatype = int(field["datatype"])
        out_field.count = int(field["count"])
        msg.fields.append(out_field)

    msg.is_bigendian = int(payload["is_bigendian"])
    msg.point_step = int(payload["point_step"])
    msg.row_step = int(payload["row_step"])
    msg.data = payload["data"]
    msg.is_dense = int(payload["is_dense"])
    return msg


class ZcPyPublisher:
    """Python publisher wrapper for shared-memory zero-copy transport."""

    def __init__(
        self,
        node: Node,
        topic_name: str,
        msg_type: Type[Any],
        qos_profile: Any = 10,
    ) -> None:
        if msg_type not in (Image, PointCloud2):
            raise TypeError("msg_type must be sensor_msgs.msg.Image or sensor_msgs.msg.PointCloud2")

        self._node = node
        self._topic_name = topic_name
        self._shm_topic_name = _normalized_topic(topic_name)
        self._msg_type = msg_type
        self._pub = node.create_publisher(String, topic_name, qos_profile)

    def publish(self, msg: Any) -> bool:
        if self._msg_type is Image:
            object_name = _image_to_object_name(msg)
        else:
            object_name = _pointcloud2_to_object_name(msg)

        published = bridge.prepare_publish(self._shm_topic_name, object_name)
        if not published:
            return False

        out = String()
        out.data = object_name
        self._pub.publish(out)
        return True


class ZcPySubscriber:
    """Python subscriber wrapper for shared-memory transport.

    Python callback side is fixed to one-copy unpack: shm payload -> Python ROS message.
    """

    def __init__(
        self,
        node: Node,
        topic_name: str,
        msg_type: Type[Any],
        callback: Callable[[Any], None],
        qos_profile: Any = 10,
    ) -> None:
        if msg_type not in (Image, PointCloud2):
            raise TypeError("msg_type must be sensor_msgs.msg.Image or sensor_msgs.msg.PointCloud2")

        self._node = node
        self._topic_name = topic_name
        self._shm_topic_name = _normalized_topic(topic_name)
        self._msg_type = msg_type
        self._callback = callback
        self._subscriber_id = bridge.register_subscriber(self._shm_topic_name)

        self._sub = node.create_subscription(String, topic_name, self._on_name_msg, qos_profile)

    def _on_name_msg(self, msg: String) -> None:
        if self._msg_type is Image:
            payload = bridge.take_image(msg.data, self._subscriber_id)
            self._callback(_dict_to_image(payload))
            return

        payload = bridge.take_pointcloud2(msg.data, self._subscriber_id)
        self._callback(_dict_to_pointcloud2(payload))

    def close(self) -> None:
        if getattr(self, "_subscriber_id", 0) != 0:
            bridge.unregister_subscriber(self._shm_topic_name, self._subscriber_id)
            self._subscriber_id = 0

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass
