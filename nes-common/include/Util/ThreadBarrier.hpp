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

#ifndef NES_COMMON_INCLUDE_UTIL_THREADBARRIER_HPP_
#define NES_COMMON_INCLUDE_UTIL_THREADBARRIER_HPP_

#include <Runtime/NesThread.hpp>
#include <condition_variable>
#include <mutex>
namespace NES {

/**
 * @brief Utility class that introduce a barrier for N threads.
 * The barrier resets when N threads call wait().
 */
class ThreadBarrier {
  public:
    /**
     * @brief Create a Barrier for size threads
     * @param size
     */
    explicit ThreadBarrier(uint32_t size) : size(size), count(0) {
        NES_ASSERT2_FMT(size <= NES::Runtime::NesThread::MaxNumThreads, "Invalid thread count " << size);
    }

    ThreadBarrier() = delete;

    ThreadBarrier(const ThreadBarrier&) = delete;

    ThreadBarrier& operator=(const ThreadBarrier&) = delete;

    ~ThreadBarrier() {
        std::unique_lock<std::mutex> lock(mutex);
        // NES_ASSERT2_FMT(count >= size, "destroying not completed thread barrier count=" << count << " size=" << size);
        // NES_ASSERT2_FMT(size <= NES::Runtime::NesThread::MaxNumThreads, "Invalid thread count " << size);
    }

    /**
     * @brief This method will block the calling thread until N threads have invoke wait().
     */
    void wait() {
        std::unique_lock<std::mutex> lock(mutex);
        NES_ASSERT2_FMT(size <= NES::Runtime::NesThread::MaxNumThreads, "Invalid thread count " << size);
        if (++count >= size) {
            cvar.notify_all();
        } else {
            // while loop to avoid spurious wakeup
            while (count < size) {
                cvar.wait(lock);
            }
        }
    }

  private:
    const uint32_t size;
    uint32_t count;
    std::mutex mutex;
    std::condition_variable cvar;
};
using ThreadBarrierPtr = std::shared_ptr<ThreadBarrier>;
}// namespace NES
#endif// NES_COMMON_INCLUDE_UTIL_THREADBARRIER_HPP_
