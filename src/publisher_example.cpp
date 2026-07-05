#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <string>

#include "rclcpp/node.hpp"
#include "rclcpp/create_publisher.hpp"
#include "rclcpp/executors.hpp"
#include "rclcpp/utilities.hpp"
#include "std_msgs/msg/string.hpp"
#include "lib/zc_pubsub.hpp"

using namespace std::chrono_literals;

/* This example creates a subclass of Node and uses std::bind() to register a
* member function as a callback from the timer. */

class MinimalPublisher : public rclcpp::Node
{
  public:
    MinimalPublisher()
    : Node("minimal_publisher"),
      count_(0),
      msg_width_(1920U),
      msg_height_(1080U),
      msg_channels_(3U)
    {
      // 使用子类 ZcPublisher
      publisher_ = rclcpp::create_publisher<
        std_msgs::msg::String,
        std::allocator<void>,
        ZcPublisher>(
          *this,
          "topic",
          rclcpp::QoS(10));
        
      timer_ = this->create_wall_timer(
      34ms, std::bind(&MinimalPublisher::timer_callback, this));

      RCLCPP_INFO(
        this->get_logger(),
        "Publisher image config: %ux%u channels=%u",
        msg_width_, msg_height_, msg_channels_);
    }

  private:
    static constexpr size_t kTotalMessages = 200;
    static constexpr auto kShutdownGracePeriod = 2s;
    
    void timer_callback()
    {

      auto data = manager_->ShmImageGetNew(shm);
      data->width = msg_width_;
      data->height = msg_height_;
      data->encoding = "rgb8";
      data->is_bigendian = 0;
      data->step = data->width * msg_channels_;
      data->data.resize(static_cast<size_t>(data->height) * data->step);
      
      std::memset(data->data.data(), 0xFF, data->data.size());


      RCLCPP_INFO(this->get_logger(), "Publishing image %zu", data->myId);
      publisher_->publish(data);
      ++count_;
      if (count_ >= kTotalMessages) {
        timer_->cancel();
        rclcpp::shutdown();
      }
    }
    rclcpp::TimerBase::SharedPtr timer_;
    std::shared_ptr<ZcPublisher> publisher_;
    size_t count_;
    uint32_t msg_width_;
    uint32_t msg_height_;
    uint32_t msg_channels_;
};

int main(int argc, char * argv[])
{
  shm_init();

  rclcpp::init(argc, argv);
  rclcpp::spin(std::make_shared<MinimalPublisher>());
  rclcpp::shutdown();

  shm_shutdown();
  return 0;
}