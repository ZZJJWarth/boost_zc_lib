#include <cstddef>
#include <limits>
#include <memory>

#include "rclcpp/rclcpp.hpp"
#include "sensor_msgs/msg/image.hpp"

#include "zc_pubsub.hpp"

using std::placeholders::_1;

class MinimalSubscriber : public ZcNode
{
  public:
    MinimalSubscriber()
    : ZcNode("minimal_subscriber"),
      received_count_(0),
      sum_latency_ms_(0.0),
      min_latency_ms_(std::numeric_limits<double>::max()),
      max_latency_ms_(0.0)
    {
      subscription_ = this->create_subscription<sensor_msgs::msg::Image>(
        "topic", 10, std::bind(&MinimalSubscriber::topic_callback, this, _1));
    }

  private:
    static constexpr size_t kExpectedMessages = 200;

    void topic_callback(const sensor_msgs::msg::Image & img)
    {
      const int64_t callback_start_ns = this->get_clock()->now().nanoseconds();
      const int64_t publish_ns = static_cast<int64_t>(img.header.stamp.sec) * 1000000000LL +
        static_cast<int64_t>(img.header.stamp.nanosec);
      const double latency_ms = static_cast<double>(callback_start_ns - publish_ns) / 1000000.0;

      ++received_count_;
      sum_latency_ms_ += latency_ms;
      if (latency_ms < min_latency_ms_) {
        min_latency_ms_ = latency_ms;
      }
      if (latency_ms > max_latency_ms_) {
        max_latency_ms_ = latency_ms;
      }

      RCLCPP_INFO(this->get_logger(), "I heard image: %ux%u",
        static_cast<unsigned int>(img.height), static_cast<unsigned int>(img.width));
      RCLCPP_INFO(this->get_logger(), "Latency: %.3f ms, frame_id=%s, count=%zu",
        latency_ms, img.header.frame_id.c_str(), received_count_);

      if (received_count_ >= kExpectedMessages) {
        const double avg_latency_ms = sum_latency_ms_ / static_cast<double>(received_count_);
        RCLCPP_INFO(this->get_logger(),
          "Latency summary (%zu msgs): avg=%.3f ms, min=%.3f ms, max=%.3f ms",
          received_count_, avg_latency_ms, min_latency_ms_, max_latency_ms_);
        rclcpp::shutdown();
      }
    }

    rclcpp::Subscription<std_msgs::msg::String>::SharedPtr subscription_;
    mutable size_t received_count_;
    mutable double sum_latency_ms_;
    mutable double min_latency_ms_;
    mutable double max_latency_ms_;
};

int main(int argc, char * argv[])
{
  shm_init();
  
  rclcpp::init(argc, argv);
  rclcpp::spin(std::make_shared<MinimalSubscriber>());
  rclcpp::shutdown();

  shm_shutdown();
  return 0;
}
