#include "WorkQueues/UnlockDeleter.hpp"

namespace NES {
UnlockDeleter::UnlockDeleter() {}

UnlockDeleter::UnlockDeleter(std::mutex& mutex) : lock(mutex) {}

UnlockDeleter::UnlockDeleter(std::mutex& mutex, std::try_to_lock_t tryToLock) : lock{mutex, tryToLock} {
    if (!lock) {
        //todo: write custom exception for this case
        throw std::exception();
    }
}

};
