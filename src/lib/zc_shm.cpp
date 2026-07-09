#include "zc_shm.hpp"

#include <cerrno>
#include <csignal>
#include <cstdio>
#include <stdexcept>
#include <string>
#include <utility>
#include <unistd.h>

// ── 全局变量定义 ─────────────────────────────────────────────────────────────

boost::interprocess::managed_shared_memory * shm = nullptr;
ShmManager * manager_ = nullptr;

namespace {

size_t local_shm_ref_count = 0;
std::string local_shm_name;

bool is_process_alive(pid_t pid)
{
    if (pid <= 0) {
        return false;
    }
    return kill(pid, 0) == 0 || errno == EPERM;
}

void release_pending_messages(
    ShmBlockingProcessing* proc_ptr,
    ShmManager* manager,
    boost::interprocess::managed_shared_memory* segment)
{
    if (proc_ptr == nullptr) {
        return;
    }

    boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> proc_lock(
        proc_ptr->mutex);
    for (const auto & msg_count : proc_ptr->myMessage) {
        manager->releaseMessageById(msg_count.first, msg_count.second, segment);
    }
    proc_ptr->myMessage.clear();
}

}  // namespace

// ── ShmSubscriberLiveness ────────────────────────────────────────────────────

ShmSubscriberLiveness::ShmSubscriberLiveness() {
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
    pthread_mutex_init(&mutex, &attr);
    pthread_mutexattr_destroy(&attr);
}

ShmSubscriberLiveness::~ShmSubscriberLiveness() {
    pthread_mutex_destroy(&mutex);
}

// ── shm_init / shm_shutdown ──────────────────────────────────────────────────

void shm_init(std::string name, size_t size) {
    if (shm != nullptr) {
        if (name != local_shm_name) {
            throw std::runtime_error("shared memory is already initialized with a different name");
        }
        manager_->user_cnt.fetch_add(1, std::memory_order_relaxed);
        ++local_shm_ref_count;
        return;
    }

    shm = new boost::interprocess::managed_shared_memory(
        boost::interprocess::open_or_create, name.c_str(), size);
    manager_ = shm->find_or_construct<ShmManager>("ShmManager")();
    manager_->user_cnt.fetch_add(1, std::memory_order_relaxed);
    local_shm_name = name;
    local_shm_ref_count = 1;
}

void shm_shutdown(std::string name) {
    if (manager_ && shm) {
        const bool last_user = manager_->user_cnt.fetch_sub(1, std::memory_order_acq_rel) == 1;
        if (local_shm_ref_count > 0) {
            --local_shm_ref_count;
        }
        if (local_shm_ref_count == 0) {
            delete shm;
            shm = nullptr;
            manager_ = nullptr;
            local_shm_name.clear();
        }
        if (last_user) {
            boost::interprocess::shared_memory_object::remove(name.c_str());
            puts("All the Shared memory has been cleaned up.");
        }
    }
}

// ── ShmManager 非模板方法 ────────────────────────────────────────────────────

ShmImage* ShmManager::ShmImageGetNew(boost::interprocess::managed_shared_memory* segment) {
    return ShmMessageGetNew<ShmImage>(segment);
}

ShmPointCloud2* ShmManager::ShmPointCloud2GetNew(
    boost::interprocess::managed_shared_memory* segment) {
    return ShmMessageGetNew<ShmPointCloud2>(segment);
}

void ShmManager::releaseMessageById(
    size_t msg_id,
    boost::interprocess::managed_shared_memory* segment) {
    releaseMessageById(msg_id, 1, segment);
}

void ShmManager::releaseMessageById(
    size_t msg_id,
    size_t count,
    boost::interprocess::managed_shared_memory* segment) {
    std::string obj_name = "ShmObject_" + std::to_string(msg_id);
    ShmImage* image_ptr = segment->find<ShmImage>(obj_name.c_str()).first;
    if (image_ptr != nullptr) {
        for (size_t i = 0; i < count; ++i) {
            releaseMessage(image_ptr, segment);
        }
        return;
    }
    ShmPointCloud2* pc2_ptr = segment->find<ShmPointCloud2>(obj_name.c_str()).first;
    if (pc2_ptr != nullptr) {
        for (size_t i = 0; i < count; ++i) {
            releaseMessage(pc2_ptr, segment);
        }
    }
}

void ShmManager::removeMessageFromTopic(
    const std::string &topic,
    size_t msg_id,
    boost::interprocess::managed_shared_memory* segment)
{
    std::string topic_name = "ShmTopic_" + topic;
    ShmTopic* topic_ptr = segment->find<ShmTopic>(topic_name.c_str()).first;
    if (topic_ptr == nullptr) {
        return;
    }

    boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> topic_lock(
        topic_ptr->mutex);
    for (const auto & sub_id : topic_ptr->subscribers) {
        std::string proc_name = "ShmBlockingProcessing_" + std::to_string(sub_id);
        ShmBlockingProcessing* proc_ptr = segment->find<ShmBlockingProcessing>(proc_name.c_str()).first;
        if (proc_ptr == nullptr) {
            continue;
        }
        boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> proc_lock(
            proc_ptr->mutex);
        proc_ptr->myMessage.erase(msg_id);
    }
}

size_t ShmManager::add_subscriber(
    const std::string &topic,
    boost::interprocess::managed_shared_memory* segment) {
    auto mgr = segment->get_segment_manager();
    ShmIntAllocator int_alloc(mgr);
    // Generate a unique subscriber id.
    size_t id = subscriber_cnt.fetch_add(1, std::memory_order_relaxed) + 1;

    std::printf("Try to add subscriber with id %zu\n", id);

    // Find or create the topic entry in shared memory.
    std::string topic_name = "ShmTopic_" + topic;
    ShmTopic* topic_ptr = segment->find_or_construct<ShmTopic>(topic_name.c_str())(int_alloc);
    {
        boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> lock(
            topic_ptr->mutex);
        topic_ptr->subscribers.insert(id);
    }

    // Create per-subscriber blocking processing storage.
    std::string proc_name = "ShmBlockingProcessing_" + std::to_string(id);
    ShmBlockingProcessing* proc_ptr = segment->find_or_construct<ShmBlockingProcessing>(proc_name.c_str())(int_alloc);
    {
        boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> lock(
            proc_ptr->mutex);
        // Reserved for future per-subscriber initialization guarded by the mutex.
    }

    // Create per-subscriber liveness tracking in shared memory.
    std::string liveness_name = "ShmSubscriberLiveness_" + std::to_string(id);
    ShmSubscriberLiveness* liveness_ptr =
        segment->find_or_construct<ShmSubscriberLiveness>(liveness_name.c_str())();

    // The liveness object is kept for shared-memory layout compatibility.
    (void)liveness_ptr;

    // Initialize duplicate subscription counter to 1
    std::string info_name = "ShmSubscriberInfo_" + std::to_string(id);
    ShmSubscriberInfo* info_ptr = segment->find_or_construct<ShmSubscriberInfo>(info_name.c_str())();
    info_ptr->dup_cnt.store(1, std::memory_order_relaxed);
    info_ptr->owner_pid = getpid();

    return id;
}

void ShmManager::incr_subscriber_dup(
    size_t id,
    boost::interprocess::managed_shared_memory* segment) {
    std::string info_name = "ShmSubscriberInfo_" + std::to_string(id);
    ShmSubscriberInfo* info_ptr = segment->find_or_construct<ShmSubscriberInfo>(info_name.c_str())();
    info_ptr->dup_cnt.fetch_add(1, std::memory_order_relaxed);
}

size_t ShmManager::get_subscriber_dup(
    size_t id,
    boost::interprocess::managed_shared_memory* segment) {
    std::string info_name = "ShmSubscriberInfo_" + std::to_string(id);
    auto found = segment->find<ShmSubscriberInfo>(info_name.c_str());
    if (found.first == nullptr) {
        return 1;
    }
    return found.first->dup_cnt.load(std::memory_order_relaxed);
}

void ShmManager::delete_subscriber(
    const std::string& topic,
    size_t id,
    boost::interprocess::managed_shared_memory* segment) {
    printf("try to delete subscriber with id %zu\n", id);
    // Remove from topic subscriber set (single-topic setup).
    std::string topic_name = "ShmTopic_" + topic;
    ShmTopic* topic_ptr = segment->find<ShmTopic>(topic_name.c_str()).first;
    if (topic_ptr != nullptr) {
        boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> topic_lock(
            topic_ptr->mutex);
        topic_ptr->subscribers.erase(id);
    }

    // Clean up pending messages for this subscriber and destroy its queue.
    std::string proc_name = "ShmBlockingProcessing_" + std::to_string(id);
    auto proc_found = segment->find<ShmBlockingProcessing>(proc_name.c_str());
    ShmBlockingProcessing* proc_ptr = proc_found.first;
    if (proc_ptr != nullptr) {
        release_pending_messages(proc_ptr, this, segment);
        segment->destroy<ShmBlockingProcessing>(proc_name.c_str());
    }

    // Remove liveness marker.
    std::string liveness_name = "ShmSubscriberLiveness_" + std::to_string(id);
    auto liveness_found = segment->find<ShmSubscriberLiveness>(liveness_name.c_str());
    if (liveness_found.first != nullptr) {
        segment->destroy<ShmSubscriberLiveness>(liveness_name.c_str());
    }

    // Remove duplicate subscription info
    std::string info_name = "ShmSubscriberInfo_" + std::to_string(id);
    auto info_found = segment->find<ShmSubscriberInfo>(info_name.c_str());
    if (info_found.first != nullptr) {
        segment->destroy<ShmSubscriberInfo>(info_name.c_str());
    }
}

bool ShmManager::publish(
    const std::string &topic,
    size_t myid,
    std::atomic_size_t &refcnt,
    boost::interprocess::managed_shared_memory* segment) {
    auto mgr = segment->get_segment_manager();
    ShmIntAllocator int_alloc(mgr);

    std::string topic_name = "ShmTopic_" + topic;
    ShmTopic* topic_ptr = segment->find_or_construct<ShmTopic>(topic_name.c_str())(int_alloc);

    boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> topic_lock(
        topic_ptr->mutex);
    if (topic_ptr->subscribers.empty()) {
        return false;
    }

    size_t actual_subscribers = 0;

    for (auto it = topic_ptr->subscribers.begin(); it != topic_ptr->subscribers.end(); ) {
        const auto sub_id = *it;
        std::string liveness_name = "ShmSubscriberLiveness_" + std::to_string(sub_id);

        std::string info_name = "ShmSubscriberInfo_" + std::to_string(sub_id);
        ShmSubscriberInfo* info_ptr = segment->find<ShmSubscriberInfo>(info_name.c_str()).first;
        bool subscriber_alive = info_ptr != nullptr && is_process_alive(info_ptr->owner_pid);

        if (!subscriber_alive) {
            std::string proc_name = "ShmBlockingProcessing_" + std::to_string(sub_id);
            auto proc_found = segment->find<ShmBlockingProcessing>(proc_name.c_str());
            ShmBlockingProcessing* proc_ptr = proc_found.first;
            if (proc_ptr != nullptr) {
                release_pending_messages(proc_ptr, this, segment);
                segment->destroy<ShmBlockingProcessing>(proc_name.c_str());
            }
            auto liveness_to_remove = segment->find<ShmSubscriberLiveness>(liveness_name.c_str());
            if (liveness_to_remove.first != nullptr) {
                segment->destroy<ShmSubscriberLiveness>(liveness_name.c_str());
            }
            if (info_ptr != nullptr) {
                segment->destroy<ShmSubscriberInfo>(info_name.c_str());
            }
            it = topic_ptr->subscribers.erase(it);
            continue;
        }

        // 根据重复订阅计数增加权重
        size_t dup = info_ptr->dup_cnt.load(std::memory_order_relaxed);

        std::string proc_name = "ShmBlockingProcessing_" + std::to_string(sub_id);
        ShmBlockingProcessing* proc_ptr = segment->find_or_construct<ShmBlockingProcessing>(proc_name.c_str())(int_alloc);
        boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> proc_lock(
            proc_ptr->mutex);
        auto found = proc_ptr->myMessage.find(myid);
        if (found == proc_ptr->myMessage.end()) {
            proc_ptr->myMessage.insert(std::make_pair(myid, dup));
        } else {
            found->second += dup;
        }
        actual_subscribers += dup;
        ++it;
    }

    if (actual_subscribers == 0) {
        return false;
    }
    refcnt.store(actual_subscribers, std::memory_order_relaxed);
    return true;
}
