#include "zc_shm.hpp"

#include <cstdio>
#include <string>

// ── 全局变量定义 ─────────────────────────────────────────────────────────────

managed_shared_memory * shm = nullptr;
ShmManager * manager_ = nullptr;

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

void shm_init(string name, size_t size) {
    shm = new managed_shared_memory(open_or_create, name.c_str(), size);
    manager_ = shm->find_or_construct<ShmManager>("ShmManager")();
    manager_->user_cnt.fetch_add(1, std::memory_order_relaxed);
}

void shm_shutdown(string name) {
    if (manager_ && shm) {
        if (manager_->user_cnt.fetch_sub(1, std::memory_order_relaxed) == 1) {
            // Last user, clean up shared memory
            delete shm;
            shm = nullptr;
            manager_ = nullptr;
            shared_memory_object::remove(name.c_str());
            puts("All the Shared memory has been cleaned up.");
        }
    }
}

// ── ShmManager 非模板方法 ────────────────────────────────────────────────────

ShmImage* ShmManager::ShmImageGetNew(managed_shared_memory* segment) {
    return ShmMessageGetNew<ShmImage>(segment);
}

ShmPointCloud2* ShmManager::ShmPointCloud2GetNew(managed_shared_memory* segment) {
    return ShmMessageGetNew<ShmPointCloud2>(segment);
}

void ShmManager::releaseMessageById(size_t msg_id, managed_shared_memory* segment) {
    std::string obj_name = "ShmObject_" + std::to_string(msg_id);
    ShmImage* image_ptr = segment->find<ShmImage>(obj_name.c_str()).first;
    if (image_ptr != nullptr) {
        releaseMessage(image_ptr, segment);
        return;
    }
    ShmPointCloud2* pc2_ptr = segment->find<ShmPointCloud2>(obj_name.c_str()).first;
    if (pc2_ptr != nullptr) {
        releaseMessage(pc2_ptr, segment);
    }
}

size_t ShmManager::add_subscriber(const string &topic, managed_shared_memory* segment) {
    auto mgr = segment->get_segment_manager();
    ShmIntAllocator int_alloc(mgr);
    // Generate a unique subscriber id.
    size_t id = subscriber_cnt.fetch_add(1, std::memory_order_relaxed) + 1;

    std::printf("Try to add subscriber with id %zu\n", id);

    // Find or create the topic entry in shared memory.
    std::string topic_name = "ShmTopic_" + topic;
    ShmTopic* topic_ptr = segment->find_or_construct<ShmTopic>(topic_name.c_str())(int_alloc);
    {
        scoped_lock<interprocess_mutex> lock(topic_ptr->mutex);
        topic_ptr->subscribers.insert(id);
    }

    // Create per-subscriber blocking processing storage.
    std::string proc_name = "ShmBlockingProcessing_" + std::to_string(id);
    ShmBlockingProcessing* proc_ptr = segment->find_or_construct<ShmBlockingProcessing>(proc_name.c_str())(int_alloc);
    {
        scoped_lock<interprocess_mutex> lock(proc_ptr->mutex);
        // Reserved for future per-subscriber initialization guarded by the mutex.
    }

    // Create per-subscriber liveness tracking in shared memory.
    std::string liveness_name = "ShmSubscriberLiveness_" + std::to_string(id);
    ShmSubscriberLiveness* liveness_ptr =
        segment->find_or_construct<ShmSubscriberLiveness>(liveness_name.c_str())();

    // Lock the mutex to signal this subscriber is alive
    pthread_mutex_lock(&liveness_ptr->mutex);

    // Initialize duplicate subscription counter to 1
    std::string info_name = "ShmSubscriberInfo_" + std::to_string(id);
    segment->find_or_construct<ShmSubscriberInfo>(info_name.c_str())();

    return id;
}

void ShmManager::incr_subscriber_dup(size_t id, managed_shared_memory* segment) {
    std::string info_name = "ShmSubscriberInfo_" + std::to_string(id);
    ShmSubscriberInfo* info_ptr = segment->find_or_construct<ShmSubscriberInfo>(info_name.c_str())();
    info_ptr->dup_cnt.fetch_add(1, std::memory_order_relaxed);
}

size_t ShmManager::get_subscriber_dup(size_t id, managed_shared_memory* segment) {
    std::string info_name = "ShmSubscriberInfo_" + std::to_string(id);
    auto found = segment->find<ShmSubscriberInfo>(info_name.c_str());
    if (found.first == nullptr) {
        return 1;
    }
    return found.first->dup_cnt.load(std::memory_order_relaxed);
}

void ShmManager::delete_subscriber(const string& topic, size_t id, managed_shared_memory* segment) {
    printf("try to delete subscriber with id %zu\n", id);
    // Remove from topic subscriber set (single-topic setup).
    std::string topic_name = "ShmTopic_" + topic;
    ShmTopic* topic_ptr = segment->find<ShmTopic>(topic_name.c_str()).first;
    if (topic_ptr != nullptr) {
        scoped_lock<interprocess_mutex> topic_lock(topic_ptr->mutex);
        topic_ptr->subscribers.erase(id);
    }

    // Clean up pending messages for this subscriber and destroy its queue.
    std::string proc_name = "ShmBlockingProcessing_" + std::to_string(id);
    auto proc_found = segment->find<ShmBlockingProcessing>(proc_name.c_str());
    ShmBlockingProcessing* proc_ptr = proc_found.first;
    if (proc_ptr != nullptr) {
        {
            scoped_lock<interprocess_mutex> proc_lock(proc_ptr->mutex);
            for (const auto &msg_id : proc_ptr->myMessage) {
                releaseMessageById(msg_id, segment);
            }
            proc_ptr->myMessage.clear();
        }
        segment->destroy<ShmBlockingProcessing>(proc_name.c_str());
    }

    // Unlock and remove liveness mutex.
    std::string liveness_name = "ShmSubscriberLiveness_" + std::to_string(id);
    auto liveness_found = segment->find<ShmSubscriberLiveness>(liveness_name.c_str());
    if (liveness_found.first != nullptr) {
        pthread_mutex_unlock(&liveness_found.first->mutex);
        segment->destroy<ShmSubscriberLiveness>(liveness_name.c_str());
    }

    // Remove duplicate subscription info
    std::string info_name = "ShmSubscriberInfo_" + std::to_string(id);
    auto info_found = segment->find<ShmSubscriberInfo>(info_name.c_str());
    if (info_found.first != nullptr) {
        segment->destroy<ShmSubscriberInfo>(info_name.c_str());
    }
}

bool ShmManager::publish(const string &topic, size_t myid, std::atomic_size_t &refcnt, managed_shared_memory* segment) {
    auto mgr = segment->get_segment_manager();
    ShmIntAllocator int_alloc(mgr);

    std::string topic_name = "ShmTopic_" + topic;
    ShmTopic* topic_ptr = segment->find_or_construct<ShmTopic>(topic_name.c_str())(int_alloc);

    scoped_lock<interprocess_mutex> topic_lock(topic_ptr->mutex);
    if (topic_ptr->subscribers.empty()) {
        return false;
    }

    size_t actual_subscribers = 0;

    for (auto it = topic_ptr->subscribers.begin(); it != topic_ptr->subscribers.end(); ) {
        const auto sub_id = *it;
        std::string liveness_name = "ShmSubscriberLiveness_" + std::to_string(sub_id);

        bool subscriber_alive = true;
        auto liveness_found = segment->find<ShmSubscriberLiveness>(liveness_name.c_str());

        if (liveness_found.first == nullptr) {
            subscriber_alive = false;
        } else {
            ShmSubscriberLiveness* liveness_ptr = liveness_found.first;
            // Try to acquire the lock without blocking using trylock
            int lock_ret = pthread_mutex_trylock(&liveness_ptr->mutex);

            if (lock_ret == EOWNERDEAD) {
                // Owner died while holding the lock
                subscriber_alive = false;
                pthread_mutex_consistent(&liveness_ptr->mutex);
                pthread_mutex_unlock(&liveness_ptr->mutex);
            } else if (lock_ret == 0) {
                // Successfully acquired the lock, subscriber is dead
                subscriber_alive = false;
                pthread_mutex_unlock(&liveness_ptr->mutex);
            } else if (lock_ret == EBUSY) {
                // Lock is busy, subscriber is alive
                subscriber_alive = true;
            } else {
                // Some other error occurred
                subscriber_alive = false;
            }
        }

        if (!subscriber_alive) {
            std::string proc_name = "ShmBlockingProcessing_" + std::to_string(sub_id);
            auto proc_found = segment->find<ShmBlockingProcessing>(proc_name.c_str());
            ShmBlockingProcessing* proc_ptr = proc_found.first;
            if (proc_ptr != nullptr) {
                {
                    scoped_lock<interprocess_mutex> proc_lock(proc_ptr->mutex);
                    for (const auto &msg_id : proc_ptr->myMessage) {
                        releaseMessageById(msg_id, segment);
                    }
                    proc_ptr->myMessage.clear();
                }
                segment->destroy<ShmBlockingProcessing>(proc_name.c_str());
            }
            auto liveness_to_remove = segment->find<ShmSubscriberLiveness>(liveness_name.c_str());
            if (liveness_to_remove.first != nullptr) {
                segment->destroy<ShmSubscriberLiveness>(liveness_name.c_str());
            }
            it = topic_ptr->subscribers.erase(it);
            continue;
        }

        // 根据重复订阅计数增加权重
        size_t dup = get_subscriber_dup(sub_id, segment);

        std::string proc_name = "ShmBlockingProcessing_" + std::to_string(sub_id);
        ShmBlockingProcessing* proc_ptr = segment->find_or_construct<ShmBlockingProcessing>(proc_name.c_str())(int_alloc);
        scoped_lock<interprocess_mutex> proc_lock(proc_ptr->mutex);
        proc_ptr->myMessage.insert(myid);
        actual_subscribers += dup;
        ++it;
    }

    if (actual_subscribers == 0) {
        return false;
    }
    refcnt.store(actual_subscribers, std::memory_order_relaxed);
    return true;
}
