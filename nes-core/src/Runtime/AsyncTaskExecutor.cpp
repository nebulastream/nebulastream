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

#include <Runtime/AsyncTaskExecutor.hpp>
#include <Util/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <exception>

namespace NES::Runtime {

AsyncTaskExecutor::AsyncTaskExecutor() : running(true) {
    runningThread = std::make_shared<std::thread>([this]() {
        try {
            setThreadName("AsyncThread");
            runningRoutine();
            completionPromise.set_value(true);
        } catch (std::exception const& ex) {
            completionPromise.set_exception(std::make_exception_ptr(ex));
        }
    });
    runningThread->detach();
}

AsyncTaskExecutor::~AsyncTaskExecutor() { destroy(); }

void AsyncTaskExecutor::runningRoutine() {
    while (true) {
        std::unique_lock lock(workMutex);
        while (asyncTaskQueue.empty()) {
            cv.wait(lock);
        }
        if (running) {
            auto& taskWrapper = asyncTaskQueue.front();
            taskWrapper();
            asyncTaskQueue.pop_front();// task is invalid after this call
        } else {
            break;
        }
    }
}

bool AsyncTaskExecutor::destroy() {
    bool expected = true;
    if (running.compare_exchange_strong(expected, false)) {
        try {
            {
                std::unique_lock lock(workMutex);
                asyncTaskQueue.emplace_back(
                    []() {
                    },
                    nullptr);
                cv.notify_all();
            }
            bool result = completionPromise.get_future().get();
            NES_ASSERT(result, "Cannot shutdown AsyncTaskExecutor");
            {
                std::unique_lock lock(workMutex);
                asyncTaskQueue.clear();
            }
            return result;
        } catch (std::exception const& ex) {
            NES_ASSERT(false, "Cannot shutdown AsyncTaskExecutor");
        }
    }
    return false;
}

}// namespace NES::Runtime