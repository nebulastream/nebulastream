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
#include <Runtime/HardwareManager.hpp>
#include <Util/Logger//Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <exception>
#include <numeric>
#include <string>

namespace NES::Runtime {

AsyncTaskExecutor::AsyncTaskExecutor(const HardwareManagerPtr& hardwareManager, uint32_t numOfThreads)
    : running(true), hardwareManager(hardwareManager) {
    NES_ASSERT(this->hardwareManager, "Invalid hardware manager");
    for (uint32_t i = 0; i < numOfThreads; ++i) {
        auto promise = std::make_shared<std::promise<bool>>();
        completionPromises.emplace_back(promise);
        runningThreads.emplace_back(std::make_shared<std::thread>([this, i, promise]() {
            try {
                setThreadName("AsyncThr-%d", i);
                runningRoutine();
                promise->set_value(true);
            } catch (std::exception const& ex) {
                promise->set_exception(std::make_exception_ptr(ex));
            }
        }));
        runningThreads.back()->detach();
    }
}

AsyncTaskExecutor::~AsyncTaskExecutor() { destroy(); }

void* AsyncTaskExecutor::allocateAsyncTask(size_t taskSize) { return hardwareManager->getGlobalAllocator()->allocate(taskSize); }

void AsyncTaskExecutor::deallocateAsyncTask(void* task, size_t size) {
    hardwareManager->getGlobalAllocator()->deallocate(task, size);
}

void AsyncTaskExecutor::runningRoutine() {
    while (true) {
        std::unique_lock lock(workMutex);
        while (asyncTaskQueue.empty()) {
            cv.wait(lock);
        }
        if (running) {
            AsyncTaskWrapper taskWrapper = asyncTaskQueue.front();
            asyncTaskQueue.pop_front();
            lock.unlock();
            taskWrapper();
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
                for (uint32_t i = 0; i < runningThreads.size(); ++i) {
                    asyncTaskQueue.emplace_back(
                        []() {
                        },
                        []() {
                        });
                }
                cv.notify_all();
            }
            auto result = std::accumulate(completionPromises.begin(), completionPromises.end(), true, [](bool acc, auto promise) {
                auto r = promise->get_future().get();
                return acc && r;
            });
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