#include "zc_pubsub.hpp"

#include <cstdio>
#include <stdexcept>
#include <string>

namespace zc_interop {

void ensure_shm_ready_or_throw()
{
  if (!manager_ || !shm) {
    throw std::runtime_error("shared memory is not initialized, call shm_init() first");
  }
}

std::string make_object_name(size_t id)
{
  return "ShmObject_" + std::to_string(id);
}

bool parse_object_name(const std::string & object_name, size_t & id)
{
  static const std::string kPrefix = "ShmObject_";
  if (object_name.rfind(kPrefix, 0) != 0) {
    return false;
  }

  try {
    id = static_cast<size_t>(std::stoull(object_name.substr(kPrefix.size())));
  } catch (const std::exception &) {
    return false;
  }
  return true;
}

void erase_from_processing_queue(size_t subscriber_id, size_t msg_id)
{
  if (!manager_ || !shm || subscriber_id == 0) {
    return;
  }

  std::string proc_name = "ShmBlockingProcessing_" + std::to_string(subscriber_id);
  ShmBlockingProcessing * proc_ptr = shm->find<ShmBlockingProcessing>(proc_name.c_str()).first;
  if (proc_ptr == nullptr) {
    return;
  }

  boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> proc_lock(
    proc_ptr->mutex);
  auto found = proc_ptr->myMessage.find(msg_id);
  if (found == proc_ptr->myMessage.end()) {
    return;
  }
  if (found->second <= 1) {
    proc_ptr->myMessage.erase(found);
    return;
  }
  --found->second;
}

bool prepare_publish(const std::string & topic_name, const std::string & object_name)
{
  ensure_shm_ready_or_throw();

  size_t id = 0;
  if (!parse_object_name(object_name, id)) {
    throw std::runtime_error("invalid object name: " + object_name);
  }

  if (auto * image_ptr = shm->find<ShmImage>(object_name.c_str()).first) {
    bool published = false;
    try {
      published = manager_->publish(topic_name.c_str(), id, image_ptr->ref_cnt, shm);
    } catch (...) {
      manager_->removeMessageFromTopic(topic_name.c_str(), id, shm);
      manager_->releaseMessage(image_ptr, shm, true);
      throw;
    }
    if (!published) {
      manager_->releaseMessage(image_ptr, shm, true);
      return false;
    }
    return true;
  }

  if (auto * pc2_ptr = shm->find<ShmPointCloud2>(object_name.c_str()).first) {
    bool published = false;
    try {
      published = manager_->publish(topic_name.c_str(), id, pc2_ptr->ref_cnt, shm);
    } catch (...) {
      manager_->removeMessageFromTopic(topic_name.c_str(), id, shm);
      manager_->releaseMessage(pc2_ptr, shm, true);
      throw;
    }
    if (!published) {
      manager_->releaseMessage(pc2_ptr, shm, true);
      return false;
    }
    return true;
  }

  throw std::runtime_error("shared-memory object not found: " + object_name);
}

void rollback_publish(const std::string & topic_name, const std::string & object_name)
{
  ensure_shm_ready_or_throw();

  size_t id = 0;
  if (!parse_object_name(object_name, id)) {
    return;
  }

  manager_->removeMessageFromTopic(topic_name.c_str(), id, shm);

  if (auto * image_ptr = shm->find<ShmImage>(object_name.c_str()).first) {
    manager_->releaseMessage(image_ptr, shm, true);
    return;
  }

  if (auto * pc2_ptr = shm->find<ShmPointCloud2>(object_name.c_str()).first) {
    manager_->releaseMessage(pc2_ptr, shm, true);
  }
}

}  // namespace zc_interop
