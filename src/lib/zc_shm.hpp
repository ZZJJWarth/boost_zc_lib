#pragma once

#include <atomic>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/containers/map.hpp>
#include <boost/interprocess/containers/set.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/interprocess_fwd.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/container/scoped_allocator.hpp>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <sys/types.h>
#include <string>
#include <cerrno>
#include <memory>
#include <pthread.h>
#include <utility>

using namespace boost::interprocess;

struct ShmTopic;

// 在共享内存中存储pthread_mutex_t的包装结构体
struct ShmSubscriberLiveness {
    pthread_mutex_t mutex;

    /**
     * @brief 构造共享内存可见的健壮互斥锁。
     * @details 将互斥锁设置为进程间共享并启用 robust 属性，用于订阅者存活检测。
     */
    ShmSubscriberLiveness();

    /**
     * @brief 析构并释放互斥锁资源。
     */
    ~ShmSubscriberLiveness();
};

using namespace boost::interprocess;

struct ShmTopic;

using Shmuint8Allocator = allocator<uint8_t, managed_shared_memory::segment_manager>;
using Shmuint8Vector    = vector<uint8_t, Shmuint8Allocator>;
using ShmCharAllocator = allocator<char, managed_shared_memory::segment_manager>;
using ShmScopedCharAllocator = boost::container::scoped_allocator_adaptor<ShmCharAllocator>;
using ShmString = basic_string<char, std::char_traits<char>, ShmCharAllocator>;
using ShmIntAllocator = allocator<size_t, managed_shared_memory::segment_manager>;
using ShmSet = set<size_t, std::less<size_t>, ShmIntAllocator>;
using ShmMessageCountPair = std::pair<const size_t, size_t>;
using ShmMessageCountAllocator = allocator<ShmMessageCountPair, managed_shared_memory::segment_manager>;
using ShmMessageCountMap = map<size_t, size_t, std::less<size_t>, ShmMessageCountAllocator>;

struct ShmTime {
    int32_t sec;
    uint32_t nanosec;

    /**
     * @brief 默认构造时间戳，初始化为 0 秒 0 纳秒。
     */
    ShmTime() : sec(0), nanosec(0) {}
};

struct ShmHeader {
    ShmTime stamp;
    ShmString frame_id;

    /**
     * @brief 构造共享内存消息头。
     * @param alloc 共享内存字符分配器，用于 frame_id 字符串。
     */
    explicit ShmHeader(const ShmCharAllocator& alloc)
            : stamp(), frame_id(alloc) {}
};

struct ShmImage {//内存内名称：ShmObject_[myId]
    size_t myId;
    std::atomic_size_t ref_cnt;

    ShmHeader header;
    uint32_t width;
    uint32_t height;
    ShmString encoding;
    uint8_t is_bigendian;
    uint32_t step;
    Shmuint8Vector data;

        /**
         * @brief 构造共享内存图像对象并绑定共享内存分配器。
         * @param mgr 共享内存 segment manager。
         */
    ShmImage(managed_shared_memory::segment_manager* mgr)
            : myId(0),
              ref_cnt(0),
                header(ShmCharAllocator(mgr)),
                width(0),
                height(0),
                encoding(ShmCharAllocator(mgr)),
                is_bigendian(0),
                step(0),
                data(Shmuint8Allocator(mgr)) {}
};

struct ShmPointField {
    using allocator_type = ShmCharAllocator;

    ShmString name;
    uint32_t offset;
    uint8_t datatype;
    uint32_t count;

    /**
     * @brief 使用字符分配器构造点字段。
     * @param alloc 共享内存字符分配器。
     */
    explicit ShmPointField(const ShmCharAllocator& alloc)
        : name(alloc), offset(0), datatype(0), count(0) {}

    /**
     * @brief 使用 scoped allocator 构造点字段。
     * @param alloc 作用域分配器，外层分配器用于 name。
     */
    explicit ShmPointField(const ShmScopedCharAllocator& alloc)
        : name(alloc.outer_allocator()), offset(0), datatype(0), count(0) {}

    /**
     * @brief 拷贝构造（显式指定字符分配器）。
     * @param other 源字段对象。
     * @param alloc 目标对象使用的字符分配器。
     */
    ShmPointField(const ShmPointField& other, const ShmCharAllocator& alloc)
        : name(other.name.c_str(), alloc),
          offset(other.offset),
          datatype(other.datatype),
          count(other.count) {}

    /**
     * @brief 拷贝构造（scoped allocator 版本）。
     * @param other 源字段对象。
     * @param alloc 作用域分配器。
     */
    ShmPointField(const ShmPointField& other, const ShmScopedCharAllocator& alloc)
        : ShmPointField(other, alloc.outer_allocator()) {}

    /**
     * @brief 移动构造（显式指定字符分配器）。
     * @param other 源字段对象。
     * @param alloc 目标对象使用的字符分配器。
     */
    ShmPointField(ShmPointField&& other, const ShmCharAllocator& alloc)
        : name(other.name.c_str(), alloc),
          offset(other.offset),
          datatype(other.datatype),
          count(other.count) {}

    /**
     * @brief 移动构造（scoped allocator 版本）。
     * @param other 源字段对象。
     * @param alloc 作用域分配器。
     */
    ShmPointField(ShmPointField&& other, const ShmScopedCharAllocator& alloc)
        : ShmPointField(std::move(other), alloc.outer_allocator()) {}
};

using ShmPointFieldAllocator = allocator<ShmPointField, managed_shared_memory::segment_manager>;
using ShmScopedPointFieldAllocator = boost::container::scoped_allocator_adaptor<
    ShmPointFieldAllocator,
    ShmCharAllocator>;
using ShmPointFieldVector = vector<ShmPointField, ShmScopedPointFieldAllocator>;

struct ShmPointCloud2 { //内存内名称：ShmObject_[myId]
    size_t myId;
    std::atomic_size_t ref_cnt;

    ShmHeader header;
    uint32_t height;
    uint32_t width;
    ShmPointFieldVector fields;
    uint8_t is_bigendian;
    uint32_t point_step;
    uint32_t row_step;
    Shmuint8Vector data;
    uint8_t is_dense;

        /**
         * @brief 构造共享内存点云对象并绑定容器分配器。
         * @param mgr 共享内存 segment manager。
         */
    explicit ShmPointCloud2(managed_shared_memory::segment_manager* mgr)
            : myId(0),
              ref_cnt(0),
              header(ShmCharAllocator(mgr)),
              height(0),
              width(0),
              fields(ShmScopedPointFieldAllocator(ShmPointFieldAllocator(mgr), ShmCharAllocator(mgr))),
              is_bigendian(0),
              point_step(0),
              row_step(0),
              data(Shmuint8Allocator(mgr)),
              is_dense(0) {}
};
struct ShmBlockingProcessing{//内存内名称：ShmBlockingProcessing_[subscriber_id]
    interprocess_mutex mutex;
    ShmMessageCountMap myMessage;

    /**
     * @brief 构造订阅者阻塞队列容器。
     * @param alloc 用于存储消息 ID 集合的分配器。
     */
    explicit ShmBlockingProcessing(const ShmIntAllocator& alloc)
        : mutex(),
          myMessage(std::less<size_t>(), ShmMessageCountAllocator(alloc.get_segment_manager())) {}
};
struct ShmTopic {//内存内名称：ShmTopic_[topic]
    interprocess_mutex mutex;
    ShmSet subscribers;

    /**
     * @brief 构造 topic 订阅者集合。
     * @param alloc 用于订阅者 ID 集合的分配器。
     */
    explicit ShmTopic(const ShmIntAllocator& alloc)
        : mutex(), subscribers(std::less<size_t>(), alloc) {}

};

// 每个订阅者的重复订阅计数（同一节点同一topic多次订阅）
struct ShmSubscriberInfo { // 内存内名称：ShmSubscriberInfo_[id]
    std::atomic_size_t dup_cnt;
    pid_t owner_pid;

    /**
     * @brief 构造订阅者重复订阅信息，默认重复计数为 1。
     */
    ShmSubscriberInfo() : dup_cnt(1), owner_pid(0) {}
};

struct ShmManager {//内存内名称：ShmManager
    std::atomic_size_t subscriber_cnt;
    std::atomic_size_t object_cnt;
    std::atomic_size_t user_cnt;//用于记录共享内存的用户数，为0时清理

    /**
     * @brief 构造共享内存管理器并初始化计数器。
     */
    ShmManager()
        : subscriber_cnt(0), object_cnt(0), user_cnt(0)
    {}

    // ── 模板方法（header-only）──────────────────────────────────────────

    template<typename ShmMsgT>
    /**
     * @brief 释放一条共享内存消息。
     * @param data 待释放的共享内存消息指针。
     * @param segment 共享内存段对象。
     * @param force_mode 是否强制销毁（忽略引用计数）。
     */
    void releaseMessage(ShmMsgT* data, managed_shared_memory* segment, bool force_mode = false)
    {
        if (data == nullptr) {
            return;
        }
        if (force_mode) {
            std::string obj_name = "ShmObject_" + std::to_string(data->myId);
            segment->destroy<ShmMsgT>(obj_name.c_str());
            return;
        }
        size_t previous = data->ref_cnt.load(std::memory_order_acquire);
        while (previous != 0 &&
               !data->ref_cnt.compare_exchange_weak(
                 previous, previous - 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
        }
        if (previous == 1) {
            std::printf("Message %zu has released\n", data->myId);
            std::string obj_name = "ShmObject_" + std::to_string(data->myId);
            segment->destroy<ShmMsgT>(obj_name.c_str());
        }
    }

    template<typename ShmMsgT>
    /**
     * @brief 在共享内存中新建消息对象并分配唯一对象 ID。
     * @param segment 共享内存段对象。
     * @return ShmMsgT* 新创建的共享内存消息指针。
     */
    ShmMsgT* ShmMessageGetNew(managed_shared_memory* segment)
    {
        auto alloc = segment->get_segment_manager();

        // Generate a unique subscriber id.
        size_t id = object_cnt.fetch_add(1, std::memory_order_relaxed) + 1;

        // Create a new shared-memory message object.
        std::string obj_name = "ShmObject_" + std::to_string(id);
        ShmMsgT* data = segment->construct<ShmMsgT>(obj_name.c_str())(alloc);
        data->myId = id;
        return data;
    }

    // ── 非模板方法（声明，实现在 zc_shm.cpp）────────────────────────────

    /**
     * @brief 新建共享内存图像消息。
     * @param segment 共享内存段对象。
     * @return ShmImage* 新建图像消息指针。
     */
    ShmImage* ShmImageGetNew(managed_shared_memory* segment);

    /**
     * @brief 新建共享内存点云消息。
     * @param segment 共享内存段对象。
     * @return ShmPointCloud2* 新建点云消息指针。
     */
    ShmPointCloud2* ShmPointCloud2GetNew(managed_shared_memory* segment);

    /**
     * @brief 根据消息 ID 查找并释放共享内存消息。
     * @param msg_id 消息对象 ID。
     * @param segment 共享内存段对象。
     */
    void releaseMessageById(size_t msg_id, managed_shared_memory* segment);

    /**
     * @brief 根据消息 ID 查找并释放共享内存消息多次。
     * @param msg_id 消息对象 ID。
     * @param count 释放次数。
     * @param segment 共享内存段对象。
     */
    void releaseMessageById(size_t msg_id, size_t count, managed_shared_memory* segment);

    /**
     * @brief 从指定 topic 的所有订阅者 pending 队列中移除消息 ID。
     */
    void removeMessageFromTopic(const string &topic, size_t msg_id, managed_shared_memory* segment);

    /**
     * @brief 为指定 topic 新增订阅者并初始化订阅者相关资源。
     * @param topic topic 名称。
     * @param segment 共享内存段对象。
     * @return size_t 分配给订阅者的唯一 ID。
     */
    size_t add_subscriber(const string &topic, managed_shared_memory* segment);

    /**
     * @brief 增加订阅者在同一 topic 下的重复订阅计数。
     * @param id 订阅者 ID。
     * @param segment 共享内存段对象。
     */
    void incr_subscriber_dup(size_t id, managed_shared_memory* segment);

    /**
     * @brief 查询订阅者重复订阅计数。
     * @param id 订阅者 ID。
     * @param segment 共享内存段对象。
     * @return size_t 重复订阅计数，若不存在则返回 1。
     */
    size_t get_subscriber_dup(size_t id, managed_shared_memory* segment);

    /**
     * @brief 删除订阅者并清理其挂起消息、存活标记和重复订阅信息。
     * @param topic topic 名称。
     * @param id 订阅者 ID。
     * @param segment 共享内存段对象。
     */
    void delete_subscriber(const string& topic, size_t id, managed_shared_memory* segment);

    /**
     * @brief 向指定 topic 的订阅者分发消息 ID，并计算消息引用计数。
     * @param topic topic 名称。
     * @param myid 当前消息对象 ID。
     * @param refcnt 消息引用计数（输出），写入有效订阅权重和。
     * @param segment 共享内存段对象。
     * @return bool 是否存在有效订阅者并成功发布。
     */
    bool publish(const string &topic, size_t myid, std::atomic_size_t &refcnt, managed_shared_memory* segment);
};

// 由共享库提供定义（实现在 zc_shm.cpp）
extern managed_shared_memory * shm;
extern ShmManager * manager_;

/**
 * @brief 初始化共享内存段及管理器，并增加当前用户计数。
 * @param name 共享内存名称。
 * @param size 共享内存大小（字节）。
 */
void shm_init(string name = "MyShm", size_t size = 64000000);

/**
 * @brief 关闭共享内存使用并在最后一个用户退出时清理共享内存。
 * @param name 共享内存名称。
 */
void shm_shutdown(string name = "MyShm");
