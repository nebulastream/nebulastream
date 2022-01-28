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

#ifndef NES_INCLUDE_REST_USR_INTERRUPT_HANDLER_HPP_
#define NES_INCLUDE_REST_USR_INTERRUPT_HANDLER_HPP_

#include <condition_variable>
#include <csignal>
#include <iostream>
#include <mutex>

static std::condition_variable condition;
static std::mutex mutex;

namespace NES {

class InterruptHandler {
  public:
    static void hookUserInterruptHandler() { signal(SIGTERM, handleUserInterrupt); }

    static void handleUserInterrupt(int signal) {
        std::cout << "handleUserInterrupt" << '\n';
        if (signal == SIGTERM) {
            std::cout << "SIGINT trapped ..." << '\n';
            condition.notify_one();
        }
    }

    static void waitForUserInterrupt() {
        std::cout << "waitForUserInterrupt()" << '\n';
        std::unique_lock<std::mutex> lock{mutex};
        std::cout << "waitForUserInterrupt() got lock" << '\n';
        condition.wait(lock);
        std::cout << "user has signaled to interrupt program..." << '\n';
        lock.unlock();
    }
};
}// namespace NES

#endif  // NES_INCLUDE_REST_USR_INTERRUPT_HANDLER_HPP_
