#include <cstddef>
#include <limits>
#include <memory>

#include "rclcpp/rclcpp.hpp"
#include "sensor_msgs/msg/image.hpp"

// 引入 std::placeholders::_1，配合 std::bind 使用。
// _1 表示回调函数真正执行时传入的第一个参数，也就是收到的 Image 消息。
using std::placeholders::_1;

/*
 * 这个示例通过继承 rclcpp::Node 创建一个 ROS 2 订阅节点。
 *
 * 节点会订阅 topic 话题上的 sensor_msgs::msg::Image 消息。
 * 每收到一帧图像，就根据消息头里的发布时间戳计算端到端延迟，
 * 并统计平均延迟、最小延迟和最大延迟。
 *
 * 这个文件通常和 normal_publisher_member_function.cpp 配套使用：
 *   发布者负责发布 200 帧图片；
 *   订阅者负责接收这 200 帧图片并统计延迟。
 */

class MinimalSubscriber : public rclcpp::Node
{
  public:
    // 构造函数：初始化节点名称和延迟统计变量，然后创建订阅者。
    MinimalSubscriber()
    : Node("normal_minimal_subscriber"),
      received_count_(0),
      sum_latency_ms_(0.0),
      min_latency_ms_(std::numeric_limits<double>::max()),
      max_latency_ms_(0.0)
    {
      // 创建一个 Image 类型的订阅者。
      //
      // 参数说明：
      //   "topic" ：订阅的话题名，需要和发布者发布的话题名一致；
      //   10      ：QoS 队列深度，表示最多缓存 10 条尚未处理的消息；
      //   std::bind(...) ：把类成员函数绑定成可调用对象，作为收到消息时的回调。
      //
      // std::bind(&MinimalSubscriber::topic_callback, this, _1) 的含义：
      //   1. 收到消息时调用当前对象 this 的 topic_callback 成员函数；
      //   2. _1 会被替换成 ROS 2 传进来的第一实参，也就是收到的 Image 消息。
      subscription_ = this->create_subscription<sensor_msgs::msg::Image>(
        "topic", 10, std::bind(&MinimalSubscriber::topic_callback, this, _1));
    }

  private:
    // 期望接收的消息总数。收到这么多帧后，订阅者会打印统计结果并关闭程序。
    static constexpr size_t kExpectedMessages = 200;

    // 订阅回调函数：每当 topic 上有一条 Image 消息到达，就会被 rclcpp 调用。
    //
    // 参数 img 是收到的图片消息引用。
    // 这里使用 const 引用，避免复制整张图片的数据，提高效率。
    //
    // 回调会更新统计变量，因此使用普通成员函数以兼容 Humble 的回调绑定。
    void topic_callback(const sensor_msgs::msg::Image & img)
    {
      // 记录回调刚开始执行时的当前时间，单位为纳秒。
      // 这里的时间来自当前节点的 ROS 时钟。
      const int64_t callback_start_ns = this->get_clock()->now().nanoseconds();

      // 从消息头 header.stamp 中还原发布者填写的发布时间戳。
      //
      // header.stamp.sec     ：整秒部分；
      // header.stamp.nanosec ：秒内纳秒部分。
      //
      // 发布者在发送消息前把当前时间写进 header.stamp，
      // 因此订阅端可以用“接收回调开始时间 - 发布时间”估算传输和调度延迟。
      const int64_t publish_ns = static_cast<int64_t>(img.header.stamp.sec) * 1000000000LL +
        static_cast<int64_t>(img.header.stamp.nanosec);

      // 延迟单位换算：
      //   callback_start_ns - publish_ns 得到纳秒；
      //   除以 1,000,000.0 转成毫秒。
      const double latency_ms = static_cast<double>(callback_start_ns - publish_ns) / 1000000.0;

      // 更新已接收消息数量。
      ++received_count_;

      // 累加延迟，用于最后计算平均值。
      sum_latency_ms_ += latency_ms;

      // 更新最小延迟。
      if (latency_ms < min_latency_ms_) {
        min_latency_ms_ = latency_ms;
      }

      // 更新最大延迟。
      if (latency_ms > max_latency_ms_) {
        max_latency_ms_ = latency_ms;
      }

      // 打印收到的图片尺寸。
      // 注意日志格式是 "%ux%u"，后面传入的是 height、width，
      // 所以输出顺序实际是“高 x 宽”，不是常见的“宽 x 高”。
      RCLCPP_INFO(this->get_logger(), "I heard image: %ux%u",
        static_cast<unsigned int>(img.height), static_cast<unsigned int>(img.width));

      // 打印本帧延迟、frame_id 和当前接收计数。
      // frame_id 由发布者设置，例如 frame_0、frame_1 等。
      RCLCPP_INFO(this->get_logger(), "Latency: %.3f ms, frame_id=%s, count=%zu",
        latency_ms, img.header.frame_id.c_str(), received_count_);

      // 收到足够数量的消息后，输出延迟统计并关闭 ROS 2。
      if (received_count_ >= kExpectedMessages) {
        // 平均延迟 = 累计延迟 / 接收帧数。
        const double avg_latency_ms = sum_latency_ms_ / static_cast<double>(received_count_);

        // 打印最终统计结果：平均延迟、最小延迟、最大延迟。
        RCLCPP_INFO(this->get_logger(),
          "Latency summary (%zu msgs): avg=%.3f ms, min=%.3f ms, max=%.3f ms",
          received_count_, avg_latency_ms, min_latency_ms_, max_latency_ms_);

        // 请求关闭 ROS 2 上下文。spin() 会因此退出，main() 随后继续执行。
        rclcpp::shutdown();
      }
    }

    // Image 消息订阅者。
    // 必须保存为成员变量，否则构造函数结束后订阅对象会被销毁，节点就收不到消息。
    rclcpp::Subscription<sensor_msgs::msg::Image>::SharedPtr subscription_;

    // 已收到的消息数量。
    //
    // mutable 表示即使在 const 成员函数 topic_callback() 中也允许修改它。
    mutable size_t received_count_;

    // 延迟总和，用于最终计算平均延迟。
    mutable double sum_latency_ms_;

    // 当前统计到的最小延迟。
    // 初始值设为 double 的最大值，这样第一条消息的延迟一定会小于它。
    mutable double min_latency_ms_;

    // 当前统计到的最大延迟。
    // 初始值设为 0，后续只要收到正延迟就会更新。
    mutable double max_latency_ms_;
};

int main(int argc, char * argv[])
{
  // 初始化 rclcpp。必须在创建 ROS 节点、订阅者等对象之前调用。
  // argc/argv 传入后，ROS 2 可以解析命令行中的 ROS 参数和 remap 规则。
  rclcpp::init(argc, argv);

  // 创建 MinimalSubscriber 节点并进入事件循环。
  // spin() 会阻塞当前线程，持续等待并处理订阅回调。
  // 当 topic_callback() 中调用 rclcpp::shutdown() 后，spin() 会退出。
  rclcpp::spin(std::make_shared<MinimalSubscriber>());

  // 再调用一次 shutdown 是常见清理写法。
  // 如果前面已经 shutdown，重复调用通常是安全的。
  rclcpp::shutdown();

  // 返回 0 表示程序正常结束。
  return 0;
}
