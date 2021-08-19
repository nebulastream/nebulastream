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

#include <Runtime/NesThread.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/Task.hpp>
#include <Runtime/ThreadPool.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger.hpp>
#include <Util/ThreadBarrier.hpp>
#include <Util/ThreadNaming.hpp>
#include <cstring>
#include <functional>
#include <utility>
namespace NES::Runtime {

ThreadPool::ThreadPool(uint64_t nodeId, QueryManagerPtr queryManager, uint32_t numThreads, std::vector<uint64_t> workerToCoreMapping)
: nodeId(nodeId), numThreads(numThreads), queryManager(std::move(queryManager)), workerToCoreMapping(workerToCoreMapping) {}

ThreadPool::~ThreadPool() {
    NES_DEBUG("Threadpool: Destroying Thread Pool");
    stop();
    NES_DEBUG("QueryManager: Destroy threads Queue");
    threads.clear();
}

void ThreadPool::runningRoutine(WorkerContext&& workerContext) {
    while (running) {
        try {
            switch (queryManager->processNextTask(running, workerContext)) {
                case ExecutionResult::Finished:
                case ExecutionResult::Ok: {
                    break;
                }
                case ExecutionResult::AllFinished: {
                    running = false;
                    break;
                }
                case ExecutionResult::Error: {
                    // TODO add here error handling (see issues 524 and 463)
                    NES_ERROR("Threadpool: finished task with error");
                    running = false;
                    break;
                }
                default: {
                    NES_THROW_RUNTIME_ERROR("unsupported");
                }
            }
        } catch (std::exception const& error) {
            NES_ERROR("Got fatal error on thread " << workerContext.getId() << ": " << error.what());
            NES_THROW_RUNTIME_ERROR("Got fatal error on thread " << workerContext.getId() << ": " << error.what());
        }
    }
    // to drain the queue for pending reconfigurations
    try {
        queryManager->processNextTask(running, workerContext);
    } catch (std::exception const& error) {
        NES_ERROR("Got fatal error on thread " << workerContext.getId() << ": " << error.what());
        NES_THROW_RUNTIME_ERROR("Got fatal error on thread " << workerContext.getId() << ": " << error.what());
    }
    NES_DEBUG("Threadpool: end runningRoutine");
}

bool ThreadPool::start() {
    auto barrier = std::make_shared<ThreadBarrier>(numThreads + 1);
    std::unique_lock lock(reconfigLock);
    if (running) {
        NES_DEBUG("Threadpool:start already running, return false");
        return false;
    }
    running = true;

    /* spawn threads */
    NES_DEBUG("Threadpool: Spawning " << numThreads << " threads");
    for (uint64_t i = 0; i < numThreads; ++i) {
        threads.emplace_back([this, i, barrier]() {
            setThreadName("Wrk-%d-%d", nodeId, i);
            if(workerToCoreMapping.size() != 0)
            {
                cpu_set_t cpuset;
                CPU_ZERO(&cpuset);
                CPU_SET(workerToCoreMapping[i], &cpuset);
                int rc = pthread_setaffinity_np(this->threads[i].native_handle(), sizeof(cpu_set_t), &cpuset);
                if (rc != 0) {
                    NES_ERROR("Error calling pthread_setaffinity_np: " << rc );
                }
                else
                {
                    NES_WARNING("worker " << i << " pins to core=" << workerToCoreMapping[i]);
                }
            }
            else
            {
                NES_WARNING("Worker use default affinity");
            }

            barrier->wait();
            runningRoutine(WorkerContext(NesThread::getId()));
        });
    }
    barrier->wait();
    NES_DEBUG("Threadpool: start return from start");
    return true;
}

bool ThreadPool::stop() {
    std::unique_lock lock(reconfigLock);
    NES_DEBUG("ThreadPool: stop thread pool while " << (running.load() ? "running" : "not running") << " with " << numThreads
                                                    << " threads");
    auto expected = true;
    if (!running.compare_exchange_strong(expected, false)) {
        return false;
    }
    /* wake up all threads in the query manager,
     * so they notice the change in the run variable */
    NES_DEBUG("Threadpool: Going to unblock " << numThreads << " threads");
    queryManager->poisonWorkers();
    /* join all threads if possible */
    for (auto& thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    threads.clear();
    NES_DEBUG("Threadpool: stop finished");
    return true;
}

uint32_t ThreadPool::getNumberOfThreads() const {
    std::unique_lock lock(reconfigLock);
    return numThreads;
}

}// namespace NES::Runtime

#pragma clang diagnostic pop