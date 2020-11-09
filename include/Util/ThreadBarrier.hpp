/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_THREADBARRIER_HPP
#define NES_THREADBARRIER_HPP

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
    explicit ThreadBarrier(uint32_t size) : size(size), count(0), mutex(), cvar() {}

    ThreadBarrier(const ThreadBarrier&) = delete;

    ThreadBarrier& operator=(const ThreadBarrier&) = delete;

    /**
     * @brief This method will block the calling thread until N threads have invoke wait().
     */
    void wait() {
        std::unique_lock<std::mutex> lock(mutex);
        if (++count >= size) {
            cvar.notify_all();
        } else {
            while (count < size) {
                cvar.wait(lock);
            }
        }
    }

  private:
    const uint32_t size;
    std::mutex mutex;
    uint32_t count;
    std::condition_variable cvar;
};
}// namespace NES
#endif//NES_THREADBARRIER_HPP
