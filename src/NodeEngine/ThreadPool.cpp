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

#include <NodeEngine/NesThread.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <NodeEngine/Task.hpp>
#include <NodeEngine/ThreadPool.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <Util/Logger.hpp>
#include <Util/ThreadBarrier.hpp>
#include <Util/ThreadNaming.hpp>
#include <functional>
#include <string.h>
namespace NES::NodeEngine {

ThreadPool::ThreadPool(uint64_t nodeId, QueryManagerPtr queryManager, uint16_t numThreads)
    : running(false), numThreads(numThreads), nodeId(nodeId), threads(), queryManager(queryManager) {}

ThreadPool::~ThreadPool() {
    NES_DEBUG("Threadpool: Destroying Thread Pool");
    stop();
    NES_DEBUG("QueryManager: Destroy threads Queue");
    threads.clear();
}

void ThreadPool::runningRoutine(WorkerContext&& workerContext) {
    try {
        while (running) {
            switch (queryManager->processNextTask(running, workerContext)) {
                case QueryManager::Ok: {
                    break;
                }
                case QueryManager::Finished: {
                    running = false;
                    break;
                }
                case QueryManager::Error: {
                    // TODO add here error handling (see issues 524 and 463)
                    NES_ERROR("Threadpool: finished task with error");
                    running = false;
                    break;
                }
                default: {
                    NES_THROW_RUNTIME_ERROR("unsupported");
                }
            }
        }
        queryManager->processNextTask(running, workerContext);
        NES_DEBUG("Threadpool: end runningRoutine");
    } catch (std::exception& error) {
        NES_ERROR("Got fatal error on thread " << workerContext.getId() << ": " << error.what());
        NES_ASSERT(false, "fatal error");
    }
}

bool ThreadPool::start() {
    auto barrier = std::make_shared<ThreadBarrier>(numThreads + 1);
    std::unique_lock<std::mutex> lock(reconfigLock);
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
            barrier->wait();
            runningRoutine(WorkerContext(NesThread::getId()));
        });
    }
    barrier->wait();
    NES_DEBUG("Threadpool:start return from start");
    return true;
}

bool ThreadPool::stop() {
    std::unique_lock<std::mutex> lock(reconfigLock);
    NES_DEBUG("ThreadPool: stop thread pool while " << (running.load() ? "running" : "not running") << " with " << numThreads
                                                    << " threads");
    /* wake up all threads in the query manager,
     * so they notice the change in the run variable */
    NES_DEBUG("Threadpool: Going to unblock " << numThreads << " threads");
    running = false;
    queryManager->unblockThreads();
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

void ThreadPool::restart() {
    stop();
    start();
}

void ThreadPool::setNumberOfThreadsWithoutRestart(uint16_t size) { numThreads = size; }
void ThreadPool::setNumberOfThreadsWithRestart(uint16_t size) {
    numThreads = size;
    restart();
}

uint16_t ThreadPool::getNumberOfThreads() { return numThreads; }

}// namespace NES::NodeEngine
