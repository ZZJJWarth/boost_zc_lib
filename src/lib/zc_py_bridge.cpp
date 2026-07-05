#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "zc_pubsub.hpp"

namespace py = pybind11;

namespace {

void ensure_shm_ready()
{
  zc_interop::ensure_shm_ready_or_throw();
}

ShmImage * find_image_or_throw(const std::string & object_name)
{
  ensure_shm_ready();
  ShmImage * image_ptr = shm->find<ShmImage>(object_name.c_str()).first;
  if (image_ptr == nullptr) {
    throw std::runtime_error("image object not found: " + object_name);
  }
  return image_ptr;
}

ShmPointCloud2 * find_pc2_or_throw(const std::string & object_name)
{
  ensure_shm_ready();
  ShmPointCloud2 * pc2_ptr = shm->find<ShmPointCloud2>(object_name.c_str()).first;
  if (pc2_ptr == nullptr) {
    throw std::runtime_error("pointcloud2 object not found: " + object_name);
  }
  return pc2_ptr;
}

py::dict image_to_py_dict(const ShmImage & src)
{
  py::dict out;
  out["stamp_sec"] = src.header.stamp.sec;
  out["stamp_nanosec"] = src.header.stamp.nanosec;
  out["frame_id"] = std::string(src.header.frame_id.c_str());
  out["width"] = src.width;
  out["height"] = src.height;
  out["encoding"] = std::string(src.encoding.c_str());
  out["is_bigendian"] = src.is_bigendian;
  out["step"] = src.step;
  out["data"] = py::bytes(
    reinterpret_cast<const char *>(src.data.data()),
    static_cast<py::ssize_t>(src.data.size()));
  return out;
}

py::list fields_to_py_list(const ShmPointFieldVector & fields)
{
  py::list out;
  for (const auto & field : fields) {
    py::dict f;
    f["name"] = std::string(field.name.c_str());
    f["offset"] = field.offset;
    f["datatype"] = field.datatype;
    f["count"] = field.count;
    out.append(std::move(f));
  }
  return out;
}

py::dict pc2_to_py_dict(const ShmPointCloud2 & src)
{
  py::dict out;
  out["stamp_sec"] = src.header.stamp.sec;
  out["stamp_nanosec"] = src.header.stamp.nanosec;
  out["frame_id"] = std::string(src.header.frame_id.c_str());
  out["height"] = src.height;
  out["width"] = src.width;
  out["fields"] = fields_to_py_list(src.fields);
  out["is_bigendian"] = src.is_bigendian;
  out["point_step"] = src.point_step;
  out["row_step"] = src.row_step;
  out["is_dense"] = src.is_dense;
  out["data"] = py::bytes(
    reinterpret_cast<const char *>(src.data.data()),
    static_cast<py::ssize_t>(src.data.size()));
  return out;
}

}  // namespace

PYBIND11_MODULE(_zc_py_bridge, m)
{
  m.doc() = "Python bridge for cpp_pubsub shared-memory zero-copy transport";

  m.def(
    "init_shm",
    [](const std::string & name, size_t size) {
      shm_init(name.c_str(), size);
    },
    py::arg("name") = "MyShm",
    py::arg("size") = 64000000);

  m.def(
    "shutdown_shm",
    [](const std::string & name) {
      shm_shutdown(name.c_str());
    },
    py::arg("name") = "MyShm");

  m.def(
    "register_subscriber",
    [](const std::string & topic_name) -> size_t {
      ensure_shm_ready();
      return manager_->add_subscriber(topic_name.c_str(), shm);
    },
    py::arg("topic_name"));

  m.def(
    "unregister_subscriber",
    [](const std::string & topic_name, size_t subscriber_id) {
      ensure_shm_ready();
      if (subscriber_id == 0) {
        throw std::runtime_error("subscriber_id must not be zero");
      }
      manager_->delete_subscriber(topic_name.c_str(), subscriber_id, shm);
    },
    py::arg("topic_name"),
    py::arg("subscriber_id"));

  m.def(
    "create_image",
    [](int32_t stamp_sec,
       uint32_t stamp_nanosec,
       const std::string & frame_id,
       uint32_t width,
       uint32_t height,
       const std::string & encoding,
       uint8_t is_bigendian,
       uint32_t step,
       py::buffer data) -> std::string {
      ensure_shm_ready();
      auto * image = manager_->ShmImageGetNew(shm);
      image->header.stamp.sec = stamp_sec;
      image->header.stamp.nanosec = stamp_nanosec;
      image->header.frame_id = frame_id.c_str();
      image->width = width;
      image->height = height;
      image->encoding = encoding.c_str();
      image->is_bigendian = is_bigendian;
      image->step = step;

      py::buffer_info info = data.request();
      const auto * src = static_cast<const uint8_t *>(info.ptr);
      const size_t total_size = static_cast<size_t>(info.size) * static_cast<size_t>(info.itemsize);
      image->data.resize(total_size);
      if (total_size > 0) {
        std::memcpy(image->data.data(), src, total_size);
      }
      return zc_interop::make_object_name(image->myId);
    },
    py::arg("stamp_sec"),
    py::arg("stamp_nanosec"),
    py::arg("frame_id"),
    py::arg("width"),
    py::arg("height"),
    py::arg("encoding"),
    py::arg("is_bigendian"),
    py::arg("step"),
    py::arg("data"));

  m.def(
    "create_pointcloud2",
    [](int32_t stamp_sec,
       uint32_t stamp_nanosec,
       const std::string & frame_id,
       uint32_t height,
       uint32_t width,
       const py::list & fields,
       uint8_t is_bigendian,
       uint32_t point_step,
       uint32_t row_step,
       uint8_t is_dense,
       py::buffer data) -> std::string {
      ensure_shm_ready();
      auto * pc2 = manager_->ShmPointCloud2GetNew(shm);
      pc2->header.stamp.sec = stamp_sec;
      pc2->header.stamp.nanosec = stamp_nanosec;
      pc2->header.frame_id = frame_id.c_str();
      pc2->height = height;
      pc2->width = width;
      pc2->is_bigendian = is_bigendian;
      pc2->point_step = point_step;
      pc2->row_step = row_step;
      pc2->is_dense = is_dense;

      pc2->fields.clear();
      pc2->fields.reserve(static_cast<size_t>(py::len(fields)));
      for (const auto & item : fields) {
        py::dict f = py::cast<py::dict>(item);
        ShmPointField out_field(ShmCharAllocator(shm->get_segment_manager()));
        out_field.name = py::cast<std::string>(f["name"]).c_str();
        out_field.offset = py::cast<uint32_t>(f["offset"]);
        out_field.datatype = py::cast<uint8_t>(f["datatype"]);
        out_field.count = py::cast<uint32_t>(f["count"]);
        pc2->fields.push_back(std::move(out_field));
      }

      py::buffer_info info = data.request();
      const auto * src = static_cast<const uint8_t *>(info.ptr);
      const size_t total_size = static_cast<size_t>(info.size) * static_cast<size_t>(info.itemsize);
      pc2->data.resize(total_size);
      if (total_size > 0) {
        std::memcpy(pc2->data.data(), src, total_size);
      }
      return zc_interop::make_object_name(pc2->myId);
    },
    py::arg("stamp_sec"),
    py::arg("stamp_nanosec"),
    py::arg("frame_id"),
    py::arg("height"),
    py::arg("width"),
    py::arg("fields"),
    py::arg("is_bigendian"),
    py::arg("point_step"),
    py::arg("row_step"),
    py::arg("is_dense"),
    py::arg("data"));

  m.def(
    "prepare_publish",
    [](const std::string & topic_name, const std::string & object_name) -> bool {
      return zc_interop::prepare_publish(topic_name, object_name);
    },
    py::arg("topic_name"),
    py::arg("object_name"));

  m.def(
    "take_image",
    [](const std::string & object_name, size_t subscriber_id) -> py::dict {
      if (subscriber_id == 0) {
        throw std::runtime_error("subscriber_id must not be zero");
      }
      auto * image_ptr = find_image_or_throw(object_name);
      py::dict out = image_to_py_dict(*image_ptr);
      zc_interop::erase_from_processing_queue(subscriber_id, image_ptr->myId);
      manager_->releaseMessage(image_ptr, shm);
      return out;
    },
    py::arg("object_name"),
    py::arg("subscriber_id"));

  m.def(
    "take_pointcloud2",
    [](const std::string & object_name, size_t subscriber_id) -> py::dict {
      if (subscriber_id == 0) {
        throw std::runtime_error("subscriber_id must not be zero");
      }
      auto * pc2_ptr = find_pc2_or_throw(object_name);
      py::dict out = pc2_to_py_dict(*pc2_ptr);
      zc_interop::erase_from_processing_queue(subscriber_id, pc2_ptr->myId);
      manager_->releaseMessage(pc2_ptr, shm);
      return out;
    },
    py::arg("object_name"),
    py::arg("subscriber_id"));

}
