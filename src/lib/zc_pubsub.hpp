#pragma once

#include <string>
#include <memory>
#include <cstdio>
#include <unordered_map>
#include <utility>
#include <cstring>
#include <functional>
#include <stdexcept>
#include <vector>
#include <type_traits>

#include "rclcpp/publisher.hpp"
#include "rclcpp/qos.hpp"
#include "rclcpp/publisher_options.hpp"
#include "rclcpp/rclcpp.hpp"
#include "std_msgs/msg/header.hpp"
#include "std_msgs/msg/string.hpp"
#include "sensor_msgs/msg/image.hpp"
#include "sensor_msgs/msg/point_cloud2.hpp"
#include "sensor_msgs/msg/point_field.hpp"

#include "zc_shm.hpp"

template<typename RosMsgT>
struct ZcShmTraits;

template<>
struct ZcShmTraits<sensor_msgs::msg::Image>
{
  using ShmType = ShmImage;
};

template<>
struct ZcShmTraits<sensor_msgs::msg::PointCloud2>
{
  using ShmType = ShmPointCloud2;
};

template<typename RosMsgT, typename = void>
struct IsZcSupportedMessage : std::false_type {};

template<typename RosMsgT>
struct IsZcSupportedMessage<RosMsgT, std::void_t<typename ZcShmTraits<RosMsgT>::ShmType>> : std::true_type {};

namespace zc_interop
{

void ensure_shm_ready_or_throw();

std::string make_object_name(size_t id);

bool parse_object_name(const std::string & object_name, size_t & id);

void erase_from_processing_queue(size_t subscriber_id, size_t msg_id);

bool prepare_publish(const std::string & topic_name, const std::string & object_name);

}  // namespace zc_interop

class ZcNode : public rclcpp::Node
{
public:
  using rclcpp::Node::Node;

  /**
   * @brief 创建普通 ROS 订阅（非共享内存支持消息）。
   * @tparam MessageT 订阅消息类型。
   * @tparam CallbackT 回调类型。
   * @param topic_name 话题名。
   * @param qos QoS 配置。
   * @param callback 用户回调。
   * @param options 订阅选项。
   * @return 订阅对象智能指针。
   */
  template<typename MessageT, typename CallbackT,
           typename std::enable_if<!IsZcSupportedMessage<MessageT>::value, int>::type = 0>
  typename rclcpp::Subscription<MessageT>::SharedPtr create_subscription(
    const std::string & topic_name,
    const rclcpp::QoS & qos,
    CallbackT && callback,
    const rclcpp::SubscriptionOptionsWithAllocator<std::allocator<void>> & options =
      rclcpp::SubscriptionOptionsWithAllocator<std::allocator<void>>())
  {
    return rclcpp::Node::create_subscription<MessageT>(
      topic_name, qos, std::forward<CallbackT>(callback), options);
  }

  /**
   * @brief 创建共享内存订阅（拷贝模式），回调参数为解包后的业务消息（如ShmImage->Image）。
   * @tparam MessageT 共享内存支持的消息类型。
   * @tparam CallbackT 回调类型，签名为 const MessageT&。
   * @param topic_name 话题名。
   * @param qos QoS 配置。
   * @param callback 用户回调。
   * @param options 订阅选项。
   * @return 底层字符串订阅对象智能指针。
   */
  template<typename MessageT, typename CallbackT,
           typename std::enable_if<
             IsZcSupportedMessage<MessageT>::value &&
             std::is_invocable<CallbackT &, const MessageT &>::value,
             int>::type = 0>
  typename rclcpp::Subscription<std_msgs::msg::String>::SharedPtr create_subscription(
    const std::string & topic_name,
    const rclcpp::QoS & qos,
    CallbackT && callback,
    const rclcpp::SubscriptionOptionsWithAllocator<std::allocator<void>> & options =
      rclcpp::SubscriptionOptionsWithAllocator<std::allocator<void>>())
  {
    size_t subscriber_id = ensure_subscriber_id(topic_name);

    std::function<void(const std_msgs::msg::String &)> wrapper =
      [this, cb = std::forward<CallbackT>(callback), subscriber_id](const std_msgs::msg::String & msg) mutable {
        MessageT out;
        if (!receive_message<MessageT>(msg, subscriber_id, out)) {
          std::puts("Failed to find shared memory object");
          return;
        }
        cb(out);
      };

    return rclcpp::Node::create_subscription<std_msgs::msg::String>(
      topic_name, qos, std::move(wrapper), options);
  }

  /**
   * @brief 创建共享内存订阅（零拷贝句柄模式），回调接收原始字符串消息和清理函数。
   * @tparam MessageT 共享内存支持的消息类型。
   * @tparam CallbackT 回调类型，签名为 (const std_msgs::msg::String&, std::function<void()>)。
   * @param topic_name 话题名。
   * @param qos QoS 配置。
   * @param callback 用户回调。
   * @param zero_copy 是否启用零拷贝模式。
   * @param options 订阅选项。
   * @return 底层字符串订阅对象智能指针。
   */
  template<typename MessageT, typename CallbackT,
           typename std::enable_if<
             IsZcSupportedMessage<MessageT>::value &&
             std::is_invocable<CallbackT &, const std_msgs::msg::String &, std::function<void()>>::value,
             int>::type = 0>
  typename rclcpp::Subscription<std_msgs::msg::String>::SharedPtr create_subscription(
    const std::string & topic_name,
    const rclcpp::QoS & qos,
    CallbackT && callback,
    bool zero_copy,
    const rclcpp::SubscriptionOptionsWithAllocator<std::allocator<void>> & options =
      rclcpp::SubscriptionOptionsWithAllocator<std::allocator<void>>())
  {
    size_t subscriber_id = ensure_subscriber_id(topic_name);

    using ShmT = typename ZcShmTraits<MessageT>::ShmType;

    std::function<void(const std_msgs::msg::String &)> wrapper =
      [this, cb = std::forward<CallbackT>(callback), subscriber_id, zero_copy](const std_msgs::msg::String & msg) mutable {
        if (!zero_copy) {
          std::puts("Zero-copy flag is false, use the default shared-memory subscription overload instead.");
          return;
        }

        ShmT * data = shm->find<ShmT>(msg.data.c_str()).first;
        if (data == nullptr) {
          std::puts("Failed to find shared memory object");
          return;
        }

        auto cleared = std::make_shared<bool>(false);
        std::function<void()> clear = [subscriber_id, msg, cleared]() {
          if (*cleared) {
            return;
          }

          if (!manager_ || !shm || subscriber_id == 0) {
            return;
          }

          ShmT * clear_data = shm->find<ShmT>(msg.data.c_str()).first;
          if (clear_data == nullptr) {
            return;
          }

          std::string proc_name = "ShmBlockingProcessing_" + std::to_string(subscriber_id);
          auto proc_found = shm->find<ShmBlockingProcessing>(proc_name.c_str());
          ShmBlockingProcessing * proc_ptr = proc_found.first;
          if (proc_ptr != nullptr) {
            scoped_lock<interprocess_mutex> proc_lock(proc_ptr->mutex);
            proc_ptr->myMessage.erase(clear_data->myId);
          }

          manager_->releaseMessage(clear_data, shm);
          *cleared = true;
        };

        cb(msg, clear);
      };

    return rclcpp::Node::create_subscription<std_msgs::msg::String>(
      topic_name, qos, std::move(wrapper), options);
  }


  /**
   * @brief 删除当前节点创建的所有共享内存订阅者。
   */
  void delete_subscribers()
  {
    if (!manager_ || !shm) {
      return;
    }
    for (const auto & kv : subscriber_ids_) {
      const auto & topic = kv.first;
      const auto & info = kv.second;
      if (info.count > 0 && info.id != 0) {
        manager_->delete_subscriber(topic.c_str(), info.id, shm);
      }
    }
    subscriber_ids_.clear();
  }

  /**
   * @brief 析构节点时自动清理所有订阅者资源。
   */
  ~ZcNode() override
  {
    delete_subscribers();
  }

private:
  /**
   * @brief 确保指定话题拥有订阅者 ID，并维护重复订阅计数。
   * @param topic_name 话题名。
   * @return size_t 有效订阅者 ID；若共享内存未初始化则返回 0。
   */
  size_t ensure_subscriber_id(const std::string & topic_name)
  {
    if (manager_ && shm) {
      auto & info = subscriber_ids_[topic_name];
      if (info.count == 0) {
        info.id = manager_->add_subscriber(topic_name.c_str(), shm);
      }
      ++info.count;
      if (info.count > 1) {
        manager_->incr_subscriber_dup(info.id, shm);
      }
      return info.id;
    }

    std::puts("Shared memory manager not initialized.");
    return 0;
  }

  /**
   * @brief 将共享内存头部拷贝到 ROS Header。
   * @param src 共享内存头。
   * @param dst 目标 ROS 头部。
   */
  static void copy_header(const ShmHeader & src, std_msgs::msg::Header & dst)
  {
    dst.stamp.sec = src.stamp.sec;
    dst.stamp.nanosec = src.stamp.nanosec;
    dst.frame_id = src.frame_id.c_str();
  }

  /**
   * @brief 将共享内存图像数据拷贝到 ROS Image。
   * @param src 共享内存图像。
   * @param dst 目标 ROS 图像。
   */
  static void copy_image(const ShmImage & src, sensor_msgs::msg::Image & dst)
  {
    copy_header(src.header, dst.header);
    dst.width = src.width;
    dst.height = src.height;
    dst.encoding = src.encoding.c_str();
    dst.is_bigendian = src.is_bigendian;
    dst.step = src.step;
    dst.data.resize(src.data.size());
    std::memcpy(dst.data.data(), src.data.data(), src.data.size());
  }

  /**
   * @brief 将共享内存点云数据拷贝到 ROS PointCloud2。
   * @param src 共享内存点云。
   * @param dst 目标 ROS 点云。
   */
  static void copy_pointcloud2(const ShmPointCloud2 & src, sensor_msgs::msg::PointCloud2 & dst)
  {
    copy_header(src.header, dst.header);
    dst.height = src.height;
    dst.width = src.width;

    dst.fields.clear();
    dst.fields.reserve(src.fields.size());
    for (const auto & field : src.fields) {
      sensor_msgs::msg::PointField out_field;
      out_field.name = field.name.c_str();
      out_field.offset = field.offset;
      out_field.datatype = field.datatype;
      out_field.count = field.count;
      dst.fields.push_back(std::move(out_field));
    }

    dst.is_bigendian = src.is_bigendian != 0;
    dst.point_step = src.point_step;
    dst.row_step = src.row_step;
    dst.data.resize(src.data.size());
    std::memcpy(dst.data.data(), src.data.data(), src.data.size());
    dst.is_dense = src.is_dense != 0;
  }

  template<typename MessageT>
  /**
   * @brief 模板声明：将共享内存消息转换为目标 ROS 消息。
   * @tparam MessageT ROS 消息类型。
   * @param src 共享内存消息对象。
   * @param dst 目标 ROS 消息对象。
   */
  static void copy_from_shm(const typename ZcShmTraits<MessageT>::ShmType & src, MessageT & dst);

  template<typename MessageT>
  /**
   * @brief 接收并解包一条共享内存消息，同时完成消息队列与引用计数清理。
   * @tparam MessageT 目标 ROS 消息类型。
   * @param msg 传输的共享内存对象名字符串消息。
   * @param subscriber_id 当前订阅者 ID。
   * @param out 输出消息对象。
   * @return true 成功读取并转换消息。
   * @return false 读取失败或共享内存未初始化。
   */
  bool receive_message(
    const std_msgs::msg::String & msg,
    size_t subscriber_id,
    MessageT & out)
  {
    if (!manager_ || !shm || subscriber_id == 0) {
      return false;
    }

    using ShmT = typename ZcShmTraits<MessageT>::ShmType;
    ShmT * data = shm->find<ShmT>(msg.data.c_str()).first;
    if (data == nullptr) {
      return false;
    }

    copy_from_shm<MessageT>(*data, out);

    std::string proc_name = "ShmBlockingProcessing_" + std::to_string(subscriber_id);
    auto proc_found = shm->find<ShmBlockingProcessing>(proc_name.c_str());
    ShmBlockingProcessing * proc_ptr = proc_found.first;
    if (proc_ptr != nullptr) {
      scoped_lock<interprocess_mutex> proc_lock(proc_ptr->mutex);
      proc_ptr->myMessage.erase(data->myId);
    }

    manager_->releaseMessage(data, shm);
    return true;
  }

  struct TopicSubInfo {
    size_t id{0};
    size_t count{0};
  };
  std::unordered_map<std::string, TopicSubInfo> subscriber_ids_;
};


class ZcPublisher : public rclcpp::Publisher<std_msgs::msg::String>
{
public:
  using Base = rclcpp::Publisher<std_msgs::msg::String>;
  using Base::publish;

  /**
   * @brief 构造零拷贝发布器。
   * @param node_base 节点基础接口。
   * @param topic_name 话题名。
   * @param qos QoS 配置。
   * @param options 发布选项。
   */
  ZcPublisher(
    rclcpp::node_interfaces::NodeBaseInterface * node_base,
    const std::string & topic_name,
    const rclcpp::QoS & qos,
    const rclcpp::PublisherOptionsWithAllocator<std::allocator<void>> & options =
      rclcpp::PublisherOptionsWithAllocator<std::allocator<void>>())
  : Base(
      node_base,
      topic_name,
      qos,
      options)
  {}

  template<typename ShmMsgT>
  /**
   * @brief 发布共享内存消息对象名，并按订阅者数量设置引用计数。
   * @tparam ShmMsgT 共享内存消息类型。
   * @param data 待发布的共享内存消息指针。
   */
  void publish(ShmMsgT * data)
  {
    if (data == nullptr) {
      std::puts("Shared memory message is null, skip publish.");
      return;
    }

    if (!manager_ || !shm) {
      std::puts("Shared memory manager not initialized.");
      return;
    }

    // 得到topic名称，+1除去开头的斜杠
    const char * full = Base::get_topic_name();
    const char * topic_name = full && *full == '/' ? full + 1 : full;
    if (!manager_->publish(topic_name, data->myId, data->ref_cnt, shm)) {
      // 没有订阅者时不会设置有效 ref_cnt，直接强制回收避免引用计数下溢。
      manager_->releaseMessage(data, shm, true);
      return;
    }

    std_msgs::msg::String msg;
    msg.data = "ShmObject_" + std::to_string(data->myId);
    Base::publish(msg);
  }
};

template<>
/**
 * @brief Image 类型的共享内存到 ROS 消息转换特化。
 * @param src 共享内存图像对象。
 * @param dst 目标 ROS 图像对象。
 */
inline void ZcNode::copy_from_shm<sensor_msgs::msg::Image>(
  const ZcShmTraits<sensor_msgs::msg::Image>::ShmType & src,
  sensor_msgs::msg::Image & dst)
{
  copy_image(src, dst);
}

template<>
/**
 * @brief PointCloud2 类型的共享内存到 ROS 消息转换特化。
 * @param src 共享内存点云对象。
 * @param dst 目标 ROS 点云对象。
 */
inline void ZcNode::copy_from_shm<sensor_msgs::msg::PointCloud2>(
  const ZcShmTraits<sensor_msgs::msg::PointCloud2>::ShmType & src,
  sensor_msgs::msg::PointCloud2 & dst)
{
  copy_pointcloud2(src, dst);
}
