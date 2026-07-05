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

  scoped_lock<interprocess_mutex> proc_lock(proc_ptr->mutex);
  proc_ptr->myMessage.erase(msg_id);
}

bool prepare_publish(const std::string & topic_name, const std::string & object_name)
{
  ensure_shm_ready_or_throw();

  size_t id = 0;
  if (!parse_object_name(object_name, id)) {
    throw std::runtime_error("invalid object name: " + object_name);
  }

  if (auto * image_ptr = shm->find<ShmImage>(object_name.c_str()).first) {
    if (!manager_->publish(topic_name.c_str(), id, image_ptr->ref_cnt, shm)) {
      manager_->releaseMessage(image_ptr, shm, true);
      return false;
    }
    return true;
  }

  if (auto * pc2_ptr = shm->find<ShmPointCloud2>(object_name.c_str()).first) {
    if (!manager_->publish(topic_name.c_str(), id, pc2_ptr->ref_cnt, shm)) {
      manager_->releaseMessage(pc2_ptr, shm, true);
      return false;
    }
    return true;
  }

  throw std::runtime_error("shared-memory object not found: " + object_name);
}

}  // namespace zc_interop
