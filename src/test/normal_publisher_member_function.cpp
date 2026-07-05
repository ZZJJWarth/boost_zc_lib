#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <string>

#include "rclcpp/rclcpp.hpp"
#include "rclcpp/utilities.hpp"
#include "sensor_msgs/msg/image.hpp"

// 引入 chrono 字面量后，可以直接写 34ms、2s 这类时间常量。
using namespace std::chrono_literals;

/*
 * 这个示例通过继承 rclcpp::Node 创建一个 ROS 2 节点，并使用 std::bind()
 * 把类的成员函数注册为定时器回调函数。
 *
 * 节点启动后会定时构造 sensor_msgs::msg::Image 消息并发布到 topic 话题。
 * 图片尺寸和通道数可以通过环境变量配置：
 *   MSG_WIDTH    ：图片宽度，默认 1920
 *   MSG_HEIGHT   ：图片高度，默认 1080
 *   MSG_CHANNELS ：每个像素的通道数，默认 3，对应 rgb8
 */

class MinimalPublisher : public rclcpp::Node
{
  public:
    // 构造函数：初始化节点名称、计数器、停止标记，以及图片参数。
    MinimalPublisher()
    : Node("minimal_publisher"),
      count_(0),
      shutdown_requested_(false),
      msg_width_(parse_env_u32("MSG_WIDTH", 1920U)),
      msg_height_(parse_env_u32("MSG_HEIGHT", 1080U)),
      msg_channels_(parse_env_u32("MSG_CHANNELS", 3U))
    {
      // 创建一个 Image 类型的发布者。
      //
      // 第一个参数 "topic" 是话题名。
      // 第二个参数 rclcpp::QoS(10) 表示 QoS 队列深度为 10：
      // 当发布速度快于发送/接收速度时，中间件最多缓存 10 条待处理消息。
      publisher_ = this->create_publisher<sensor_msgs::msg::Image>("topic", rclcpp::QoS(10));

      // 创建墙上时钟定时器。墙上时钟即真实经过的时间，不依赖 ROS 仿真时间。
      //
      // 34ms 大约对应 29.4 FPS，接近 30 FPS。
      // std::bind(&MinimalPublisher::timer_callback, this) 的作用是：
      //   1. 指定要调用 MinimalPublisher::timer_callback 这个成员函数；
      //   2. 绑定 this 指针，让回调执行时知道要操作当前这个对象。
      timer_ = this->create_wall_timer(
        34ms, std::bind(&MinimalPublisher::timer_callback, this));

      // 打印最终使用的图片配置，便于确认环境变量是否生效。
      RCLCPP_INFO(
        this->get_logger(),
        "Publisher image config: %ux%u channels=%u",
        msg_width_, msg_height_, msg_channels_);
    }

  private:
    // 总共发布的图片帧数。达到这个数量后节点会主动关闭。
    static constexpr size_t kTotalMessages = 200;

    // 停止前等待订阅端确认消息的最长时间。
    // wait_for_all_acked() 在 Reliable QoS 下更有意义；如果底层不需要确认，
    // 这个等待通常会很快返回。
    static constexpr auto kShutdownGracePeriod = 2s;

    // 从环境变量读取一个 uint32_t 正整数配置。
    //
    // name          ：环境变量名，例如 MSG_WIDTH。
    // default_value ：读取失败、格式非法或数值越界时使用的默认值。
    static uint32_t parse_env_u32(const char * name, uint32_t default_value)
    {
      // std::getenv 返回 C 字符串指针；如果环境变量不存在，返回 nullptr。
      const char * raw = std::getenv(name);

      // 环境变量不存在，或者存在但内容为空字符串，都视为未配置。
      if (raw == nullptr || raw[0] == '\0') {
        return default_value;
      }

      // strtoul 会把字符串按 10 进制解析成 unsigned long。
      // end 用来判断字符串是否被完整解析：
      //   end == raw 说明开头就无法解析出数字；
      //   *end != '\0' 说明数字后面还有非法字符，例如 "123abc"。
      char * end = nullptr;
      const unsigned long parsed = std::strtoul(raw, &end, 10);

      // 过滤非法输入：
      //   1. 没有解析出任何数字；
      //   2. 字符串没有完整解析；
      //   3. 数值为 0，因为宽、高、通道数不能为 0；
      //   4. 超出 uint32_t 可表示范围。
      if (end == raw || *end != '\0' || parsed == 0UL ||
        parsed > static_cast<unsigned long>(std::numeric_limits<uint32_t>::max()))
      {
        return default_value;
      }
      return static_cast<uint32_t>(parsed);
    }

    // 定时器回调：每 34ms 被调用一次，负责构造并发布一帧 Image 消息。
    void timer_callback()
    {
      // 如果已经进入停止流程，直接返回，避免重复发布或重复 shutdown。
      if (shutdown_requested_) {
        return;
      }

      // 在发布第一帧之前等待至少一个订阅者出现。
      //
      // 这样做可以减少测试中“发布者刚启动就发完若干帧，但订阅者还没连上”
      // 导致的首帧丢失。这里只在 count_ == 0 时等待；开始发布后即使订阅者断开，
      // 发布者也会继续按计数完成整个流程。
      if (count_ == 0 && publisher_->get_subscription_count() == 0) {
        return;
      }

      // 创建一条新的 ROS Image 消息。
      // sensor_msgs::msg::Image 常用字段包括：
      //   header    ：时间戳和坐标系/帧标识；
      //   height    ：图片高度，单位像素；
      //   width     ：图片宽度，单位像素；
      //   encoding  ：像素编码格式；
      //   step      ：单行图片数据占用的字节数；
      //   data      ：连续存储的原始像素字节。
      sensor_msgs::msg::Image msg;
      msg.width = msg_width_;
      msg.height = msg_height_;

      // rgb8 表示每个像素 3 个 8-bit 通道，按 R、G、B 顺序排列。
      // 注意：代码允许 MSG_CHANNELS 被改成其他值，但 encoding 仍固定为 rgb8。
      // 如果通道数不是 3，消息的 step/data 大小会按 msg_channels_ 计算，
      // 但 encoding 语义和数据布局就不再严格匹配。
      msg.encoding = "rgb8";

      // is_bigendian 表示图像数据是否为大端字节序。
      // 对 rgb8 这种单字节通道来说字节序通常没有实际影响，这里固定填 0。
      msg.is_bigendian = 0;

      // step 是每一行占用的字节数。
      // 对 8-bit 多通道图像来说，一行字节数 = 宽度 * 通道数。
      msg.step = msg.width * msg_channels_;

      // data 是整张图的连续字节数组，总大小 = 高度 * 每行字节数。
      // resize 后 vector 会分配对应容量，后面直接逐字节填充。
      msg.data.resize(static_cast<size_t>(msg.height) * msg.step);

      // 取到底层连续内存的首地址，后面的循环用指针逐字节写入数据。
      auto * arr = msg.data.data();

      // 填充图片内容。
      //
      // (i + count_) & 0xFF 会生成 0~255 循环变化的字节值：
      //   i        ：当前字节在 data 中的位置；
      //   count_   ：当前发布的是第几帧，从 0 开始；
      //   & 0xFF   ：只保留低 8 位，保证能放入 uint8_t。
      //
      // 这样每一帧的数据都略有不同，便于订阅端验证收到的是新的图像内容。
      for (size_t i = 0; i < msg.data.size(); ++i) {
        *arr = static_cast<uint8_t>((i + count_) & 0xFF);
        ++arr;
      }

      // 记录发布时刻，单位是纳秒。
      // get_clock()->now() 使用当前节点关联的 ROS 时钟；默认情况下通常是系统时间，
      // 如果启用了 use_sim_time，则可能来自 /clock 仿真时间。
      const int64_t publish_time_ns = this->get_clock()->now().nanoseconds();

      // ROS 消息头里的 stamp 由 sec + nanosec 两部分组成。
      // 这里把纳秒时间戳拆成“整秒”和“秒内纳秒”。
      msg.header.stamp.sec = static_cast<int32_t>(publish_time_ns / 1000000000LL);
      msg.header.stamp.nanosec = static_cast<uint32_t>(publish_time_ns % 1000000000LL);

      // frame_id 通常用来表示该数据所属的坐标系/传感器帧。
      // 这里为了测试可读性，把帧号写成 frame_0、frame_1 ...
      msg.header.frame_id = "frame_" + std::to_string(count_);

      // 打印当前即将发布的帧序号。日志里使用 count_ + 1，让显示从 1 开始。
      RCLCPP_INFO(this->get_logger(), "Publishing image count=%zu", count_ + 1);

      // 真正把消息交给 ROS 2 发布系统。
      // publish(msg) 会把消息送入 rclcpp/rmw 层，由底层 DDS 负责网络传输。
      publisher_->publish(msg);

      // 发布成功后递增计数。
      ++count_;

      // 达到预设发布总数后，停止定时器，等待确认，然后关闭 rclcpp。
      if (count_ >= kTotalMessages) {
        shutdown_requested_ = true;

        // 取消定时器，避免后续再触发 timer_callback。
        timer_->cancel();

        // 尝试等待已经发布的消息被订阅端确认。
        // 如果超时，说明在宽限时间内仍有样本没有完成确认。
        if (!publisher_->wait_for_all_acked(kShutdownGracePeriod)) {
          RCLCPP_WARN(this->get_logger(), "Timed out waiting for final samples to be acknowledged");
        }

        // 请求关闭 ROS 2 上下文。spin() 会因此退出，main() 随后继续执行。
        rclcpp::shutdown();
      }
    }

    // 定时器对象必须保存为成员变量。
    // 如果只在构造函数里创建局部变量，构造函数结束后定时器会被销毁，回调不会执行。
    rclcpp::TimerBase::SharedPtr timer_;

    // Image 消息发布者，负责向 topic 话题发布 sensor_msgs::msg::Image。
    rclcpp::Publisher<sensor_msgs::msg::Image>::SharedPtr publisher_;

    // 已经发布的帧数，从 0 开始计数。
    size_t count_;

    // 停止流程标记，避免 shutdown 期间重复进入发布逻辑。
    bool shutdown_requested_;

    // 图片宽度，默认 1920，可通过 MSG_WIDTH 环境变量覆盖。
    uint32_t msg_width_;

    // 图片高度，默认 1080，可通过 MSG_HEIGHT 环境变量覆盖。
    uint32_t msg_height_;

    // 每个像素的字节/通道数量，默认 3，可通过 MSG_CHANNELS 环境变量覆盖。
    uint32_t msg_channels_;
};

int main(int argc, char * argv[])
{
  // 初始化 rclcpp。必须在创建节点、发布者、订阅者等 ROS 对象前调用。
  // argc/argv 传入后，ROS 2 可以解析命令行中的 ROS 参数和 remap 规则。
  rclcpp::init(argc, argv);

  // 创建 MinimalPublisher 节点并进入事件循环。
  // spin() 会阻塞当前线程，持续处理定时器回调、订阅回调、服务回调等。
  // 当 timer_callback() 中调用 rclcpp::shutdown() 后，spin() 会退出。
  rclcpp::spin(std::make_shared<MinimalPublisher>());

  // 再调用一次 shutdown 是常见写法，用于确保上下文被清理。
  // 如果前面已经 shutdown，重复调用通常是安全的。
  rclcpp::shutdown();

  // main 返回 0 表示程序正常结束。
  return 0;
}
