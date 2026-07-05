#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <thread>

#include "iceoryx_posh/mepoo/chunk_header.hpp"
#include "iceoryx_posh/popo/untyped_subscriber.hpp"
#include "iceoryx_posh/runtime/posh_runtime.hpp"
#include "sensor_msgs/msg/image.hpp"

namespace
{
constexpr uint32_t kEncodingCapacity = 32U;
constexpr uint32_t kFrameIdCapacity = 64U;

std::atomic_bool g_keep_running{true};

void signal_handler(int)
{
  g_keep_running.store(false, std::memory_order_relaxed);
}

struct SerializedImageHeader
{
  uint64_t seq;
  int64_t serialize_start_ns;
  uint32_t width;
  uint32_t height;
  uint32_t channels;
  uint32_t step;
  uint32_t is_bigendian;
  uint32_t encoding_length;
  uint32_t frame_id_length;
  uint64_t payload_size;
  char encoding[kEncodingCapacity];
  char frame_id[kFrameIdCapacity];
};

uint32_t parse_env_u32(const char * name, uint32_t default_value)
{
  const char * raw = std::getenv(name);
  if (raw == nullptr || raw[0] == '\0') {
    return default_value;
  }

  char * end = nullptr;
  const unsigned long parsed = std::strtoul(raw, &end, 10);
  if (end == raw || *end != '\0' ||
    parsed > static_cast<unsigned long>(std::numeric_limits<uint32_t>::max()))
  {
    return default_value;
  }
  return static_cast<uint32_t>(parsed);
}

bool deserialize_fixed_to_image(
  const void * raw_payload,
  uint64_t user_payload_size,
  sensor_msgs::msg::Image & output,
  uint64_t & seq,
  int64_t & serialize_start_ns)
{
  if (raw_payload == nullptr || user_payload_size < sizeof(SerializedImageHeader)) {
    return false;
  }

  const auto * header = static_cast<const SerializedImageHeader *>(raw_payload);
  const uint64_t available_payload_bytes = user_payload_size - sizeof(SerializedImageHeader);
  if (header->payload_size > available_payload_bytes) {
    return false;
  }
  if (header->encoding_length > kEncodingCapacity || header->frame_id_length > kFrameIdCapacity) {
    return false;
  }

  const auto * payload = reinterpret_cast<const uint8_t *>(header + 1);

  output.width = header->width;
  output.height = header->height;
  output.step = header->step;
  output.is_bigendian = static_cast<uint8_t>(header->is_bigendian);
  output.encoding.assign(header->encoding, header->encoding + header->encoding_length);
  output.header.frame_id.assign(header->frame_id, header->frame_id + header->frame_id_length);

  output.data.resize(static_cast<size_t>(header->payload_size));
  if (header->payload_size > 0U) {
    std::memcpy(output.data.data(), payload, static_cast<size_t>(header->payload_size));
  }

  serialize_start_ns = header->serialize_start_ns;
  output.header.stamp.sec = static_cast<int32_t>(serialize_start_ns / 1000000000LL);
  output.header.stamp.nanosec = static_cast<uint32_t>(serialize_start_ns % 1000000000LL);
  seq = header->seq;
  return true;
}
}  // namespace

int main()
{
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);

  iox::runtime::PoshRuntime::initRuntime("iox_image_subscriber");

  const uint32_t expected_messages = parse_env_u32("TOTAL_MSGS", 200U);
  if (expected_messages == 0U) {
    std::cerr << "[iceoryx sub] TOTAL_MSGS must be > 0" << std::endl;
    return 1;
  }

  const iox::capro::ServiceDescription service{"ImageBench", "Image", "Raw4096x2560"};
  iox::popo::UntypedSubscriber subscriber(service);

  uint64_t received_count = 0U;
  double latency_sum_ms = 0.0;
  double latency_min_ms = std::numeric_limits<double>::max();
  double latency_max_ms = 0.0;
  sensor_msgs::msg::Image image;

  std::cout << "[iceoryx sub] waiting for images (ser + transport + deser)..." << std::endl;

  while (g_keep_running.load(std::memory_order_relaxed)) {
    auto take_result = subscriber.take();
    if (take_result.has_error()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      continue;
    }

    const void * raw_payload = take_result.value();
    const auto * chunk_header = iox::mepoo::ChunkHeader::fromUserPayload(raw_payload);
    if (chunk_header == nullptr ||
      chunk_header->userPayloadSize() < sizeof(SerializedImageHeader))
    {
      subscriber.release(raw_payload);
      continue;
    }

    uint64_t seq = 0U;
    int64_t serialize_start_ns = 0LL;
    const bool ok = deserialize_fixed_to_image(
      raw_payload,
      chunk_header->userPayloadSize(),
      image,
      seq,
      serialize_start_ns);
    if (!ok) {
      subscriber.release(raw_payload);
      continue;
    }

    const auto deser_done = std::chrono::system_clock::now().time_since_epoch();
    const int64_t deser_done_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(deser_done).count();
    const double latency_ms =
      static_cast<double>(deser_done_ns - serialize_start_ns) / 1000000.0;

    ++received_count;
    latency_sum_ms += latency_ms;
    if (latency_ms < latency_min_ms) {
      latency_min_ms = latency_ms;
    }
    if (latency_ms > latency_max_ms) {
      latency_max_ms = latency_ms;
    }

    if (received_count % 30U == 0U) {
      const double avg_ms = latency_sum_ms / static_cast<double>(received_count);
      std::cout << std::fixed << std::setprecision(3)
                << "[iceoryx sub] seq=" << seq
                << " latency_ms=" << latency_ms
                << " avg=" << avg_ms
                << " min=" << latency_min_ms
                << " max=" << latency_max_ms
                << " size=" << image.width << "x" << image.height
                << std::endl;
    }

    subscriber.release(raw_payload);

    if (received_count >= expected_messages) {
      break;
    }
  }

  if (received_count > 0U) {
    const double avg_ms = latency_sum_ms / static_cast<double>(received_count);
    std::cout << std::fixed << std::setprecision(3)
              << "Total latency summary (serialize + transport + deserialize, "
              << received_count << " msgs): avg=" << avg_ms
              << " ms, min=" << latency_min_ms
              << " ms, max=" << latency_max_ms
              << " ms"
              << std::endl;
  } else {
    std::cout << "[iceoryx sub] no samples received" << std::endl;
  }

  return 0;
}
