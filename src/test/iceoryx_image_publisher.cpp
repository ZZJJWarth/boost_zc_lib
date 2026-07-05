#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <string>
#include <thread>

#include "iceoryx_posh/popo/untyped_publisher.hpp"
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

void copy_to_fixed_array(const std::string & src, char * dst, uint32_t capacity, uint32_t & copied_len)
{
  copied_len = static_cast<uint32_t>(src.size());
  if (copied_len > capacity) {
    copied_len = capacity;
  }

  std::memset(dst, 0, capacity);
  if (copied_len > 0U) {
    std::memcpy(dst, src.data(), copied_len);
  }
}

bool serialize_image_to_fixed(
  const sensor_msgs::msg::Image & image,
  uint64_t seq,
  int64_t serialize_start_ns,
  uint64_t payload_capacity,
  void * output)
{
  if (image.data.size() > payload_capacity) {
    return false;
  }

  auto * header = static_cast<SerializedImageHeader *>(output);
  auto * payload = reinterpret_cast<uint8_t *>(header + 1);

  header->seq = seq;
  header->serialize_start_ns = serialize_start_ns;
  header->width = image.width;
  header->height = image.height;
  header->step = image.step;
  header->payload_size = image.data.size();
  header->is_bigendian = static_cast<uint32_t>(image.is_bigendian);
  header->channels = (image.width > 0U) ? (image.step / image.width) : 0U;

  copy_to_fixed_array(image.encoding, header->encoding, kEncodingCapacity, header->encoding_length);
  copy_to_fixed_array(image.header.frame_id, header->frame_id, kFrameIdCapacity, header->frame_id_length);

  std::memset(payload, 0, static_cast<size_t>(payload_capacity));
  if (!image.data.empty()) {
    std::memcpy(payload, image.data.data(), image.data.size());
  }

  return true;
}
}  // namespace

int main()
{
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);

  iox::runtime::PoshRuntime::initRuntime("iox_image_publisher");

  const uint32_t width = parse_env_u32("MSG_WIDTH", 4096U);
  const uint32_t height = parse_env_u32("MSG_HEIGHT", 2560U);
  const uint32_t channels = parse_env_u32("MSG_CHANNELS", 3U);
  const uint32_t total_msgs = parse_env_u32("TOTAL_MSGS", 200U);
  const uint32_t sample_interval_ms = parse_env_u32("SAMPLE_INTERVAL_MS", 34U);

  if (width == 0U || height == 0U || channels == 0U || total_msgs == 0U) {
    std::cerr << "[iceoryx pub] Invalid parameters" << std::endl;
    return 1;
  }

  if (width > (std::numeric_limits<uint32_t>::max() / channels)) {
    std::cerr << "[iceoryx pub] width*channels overflow" << std::endl;
    return 1;
  }

  const uint32_t step = width * channels;
  const uint64_t payload_bytes = static_cast<uint64_t>(height) * step;
  const uint64_t sample_size = sizeof(SerializedImageHeader) + payload_bytes;

  if (sample_size > static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())) {
    std::cerr << "[iceoryx pub] Sample size too large for iceoryx chunk" << std::endl;
    return 1;
  }

  const iox::capro::ServiceDescription service{"ImageBench", "Image", "Raw4096x2560"};
  iox::popo::UntypedPublisher publisher(service);

  sensor_msgs::msg::Image image;
  image.width = width;
  image.height = height;
  image.encoding = "rgb8";
  image.is_bigendian = 0U;
  image.step = step;
  image.data.resize(static_cast<size_t>(payload_bytes));
  image.header.frame_id = "cam_front";

  uint64_t seq = 0U;
  auto next_tick = std::chrono::steady_clock::now();

  std::cout << "[iceoryx pub] Start publishing " << width << "x" << height <<
    " every " << sample_interval_ms << "ms, total=" << total_msgs << std::endl;

  while (g_keep_running.load(std::memory_order_relaxed) && seq < total_msgs) {
    next_tick += std::chrono::milliseconds(sample_interval_ms);

    const auto serialize_start = std::chrono::system_clock::now().time_since_epoch();
    const auto serialize_start_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(serialize_start).count();

    image.header.stamp.sec = static_cast<int32_t>(serialize_start_ns / 1000000000LL);
    image.header.stamp.nanosec = static_cast<uint32_t>(serialize_start_ns % 1000000000LL);
    std::memset(
      image.data.data(),
      static_cast<int>(seq & 0xFFU),
      image.data.size());

    auto loan_result = publisher.loan(static_cast<uint32_t>(sample_size));
    if (loan_result.has_error()) {
      std::cerr << "[iceoryx pub] loan failed, error="
                << static_cast<int>(loan_result.get_error()) << std::endl;
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      continue;
    }

    void * raw_payload = loan_result.value();
    const uint64_t publish_seq = seq + 1U;
    if (!serialize_image_to_fixed(
        image,
        publish_seq,
        serialize_start_ns,
        payload_bytes,
        raw_payload))
    {
      std::cerr << "[iceoryx pub] serialize to fixed buffer failed" << std::endl;
      publisher.release(raw_payload);
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      continue;
    }

    seq = publish_seq;

    publisher.publish(raw_payload);

    if (seq % 30U == 0U) {
      std::cout << "[iceoryx pub] published seq=" << seq
                << " payload_bytes=" << payload_bytes << std::endl;
    }

    std::this_thread::sleep_until(next_tick);
  }

  std::cout << "[iceoryx pub] stopped, total published=" << seq << std::endl;
  return 0;
}
