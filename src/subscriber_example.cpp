#include <cstddef>
#include <limits>
#include <memory>

#include "rclcpp/rclcpp.hpp"
#include "sensor_msgs/msg/image.hpp"

#include "lib/zc_pubsub.hpp"

using std::placeholders::_1;

class MinimalSubscriber : public ZcNode
{
  public:
    MinimalSubscriber()
    : ZcNode("minimal_subscriber")
    {
      subscription_ = this->create_subscription<sensor_msgs::msg::Image>(
        "topic", 10, std::bind(&MinimalSubscriber::topic_callback, this, _1));
    }

  private:

    void topic_callback(const sensor_msgs::msg::Image & img) const
    {

      RCLCPP_INFO(this->get_logger(), "I heard image: %ux%u",
        static_cast<unsigned int>(img.height), static_cast<unsigned int>(img.width));

    }

    rclcpp::Subscription<std_msgs::msg::String>::SharedPtr subscription_;
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