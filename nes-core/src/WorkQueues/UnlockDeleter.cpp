#include "WorkQueues/UnlockDeleter.hpp"

namespace NES {
UnlockDeleter::UnlockDeleter() {}

UnlockDeleter::UnlockDeleter(std::mutex mutex) : lock(mutex) {}

UnlockDeleter::UnlockDeleter(std::mutex mutex, std::try_to_lock_t adoptLock) : lock{mutex, adoptLock} {}

};
