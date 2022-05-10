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
#include <REST/RestServerInterruptHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <condition_variable>
#include <csignal>
#include <iostream>
#include <mutex>

namespace NES {

static std::condition_variable condition;
static std::mutex mutex;

void RestServerInterruptHandler::hookUserInterruptHandler() { signal(SIGTERM, handleUserInterrupt); }

void RestServerInterruptHandler::handleUserInterrupt(int signal) {
    NES_DEBUG("handleUserInterrupt");
    if (signal == SIGTERM) {
        NES_DEBUG("SIGINT trapped ...");
        condition.notify_all();
    }
}

void RestServerInterruptHandler::waitForUserInterrupt() {
    NES_DEBUG("Wait for interrupt of RestServer");
    std::unique_lock<std::mutex> lock{mutex};
    NES_TRACE("Wait for interrupt: got lock");
    condition.wait(lock);
    NES_DEBUG("User has signaled to interrupt program");
    lock.unlock();
}

}// namespace NES