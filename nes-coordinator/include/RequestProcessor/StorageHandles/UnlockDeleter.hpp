/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#ifndef NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_STORAGEHANDLES_UNLOCKDELETER_HPP_
#define NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_STORAGEHANDLES_UNLOCKDELETER_HPP_

#include <mutex>

namespace NES::RequestProcessor::Experimental {

/**
 * @brief This class holds a lock on the supplied mutex on only releases it when it is destructed. It can be supplied as a
 * custom deleter to a unique pointer to guarantee thread safe access to the resource held by the pointer without
 * freeing the resource when the pointer is destructed.
 * idea taken from:
 * https://stackoverflow.com/questions/23610561/return-locked-resource-from-class-with-automatic-unlocking#comment36245473_23610561
 */
class UnlockDeleter {
  public:
    /**
     * @brief In case of serial access to the resource no mutex is needed and the destructor of the class will have no effect
     */
    explicit UnlockDeleter();

    /**
     * @brief Blocks until a lock on the mutex is acquired, then keeps the lock until the object is destructed
     * @param mutex
     */
    explicit UnlockDeleter(std::mutex& mutex);

    /**
     * @brief Tries to acquire a lock on the mutex. On success, the lock lives until the object is destroyed.
     * Throws an exception if no lock could be acquired.
     * @param mutex
     * @param tryToLock
     */
    explicit UnlockDeleter(std::mutex& mutex, std::try_to_lock_t tryToLock);

    /**
     * @brief Keeps the supplied lock in a locked state until the unlock deleter is destructed.
     * Throws an exception if the supplied lock is not in locked state.
     * @param lock The lock to be kept for the whole lifetime of the unlock deleter object
     */
    explicit UnlockDeleter(std::unique_lock<std::mutex> lock);

    /**
     * @brief The action called when the unique pointer is destroyed. We use a no op be cause we do NOT want to free the resource.
     * @tparam T: The type of the resource
     */
    template<typename T>
    void operator()(T*) const noexcept {
        // no-op
    }

  private:
    std::unique_lock<std::mutex> lock;
};
}// namespace NES::RequestProcessor::Experimental
#endif  // NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_STORAGEHANDLES_UNLOCKDELETER_HPP_
