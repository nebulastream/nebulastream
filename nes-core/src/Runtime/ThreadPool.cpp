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

#include <Network/NetworkChannel.hpp>
#include <Runtime/NesThread.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/Task.hpp>
#include <Runtime/ThreadPool.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger.hpp>
#include <Util/ThreadBarrier.hpp>
#include <Util/ThreadNaming.hpp>
#include <cstring>
#include <filesystem>
#include <functional>
#include <string>
#include <thread>
#include <utility>

#ifdef NES_USE_ONE_QUEUE_PER_NUMA_NODE
#if defined(__linux__)
#include <Runtime/HardwareManager.hpp>
#include <numa.h>
#include <numaif.h>
#ifdef ENABLE_PAPI_PROFILER
#include <Runtime/Profiler/PAPIProfiler.hpp>
#endif
#endif
#endif

namespace NES::Runtime {

ThreadPool::ThreadPool(uint64_t nodeId,
                       QueryManagerPtr queryManager,
                       uint32_t numThreads,
                       std::vector<BufferManagerPtr> bufferManagers,
                       uint64_t numberOfBuffersPerWorker,
                       HardwareManagerPtr hardwareManager,
                       std::vector<uint64_t> workerPinningPositionList,
                       std::vector<uint64_t> queuePinListMapping)
    : nodeId(nodeId), numThreads(numThreads), queryManager(std::move(queryManager)), bufferManagers(bufferManagers),
      numberOfBuffersPerWorker(numberOfBuffersPerWorker), workerPinningPositionList(workerPinningPositionList),
      queuePinListMapping(queuePinListMapping), hardwareManager(hardwareManager) {}

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
          uint64_t queueIdx = 0;
          BufferManagerPtr localBufferManager;
#if defined(NES_USE_ONE_QUEUE_PER_NUMA_NODE) || defined(NES_USE_ONE_QUEUE_PER_QUERY)
          if (workerPinningPositionList.size() != 0) {
              NES_ASSERT(numThreads <= workerPinningPositionList.size(),
                         "Not enough worker positions for pinning are provided");
              uint64_t
                  maxPosition = *std::max_element(workerPinningPositionList.begin(), workerPinningPositionList.end());
              NES_ASSERT(maxPosition < std::thread::hardware_concurrency(), "pinning position is out of cpu range");
              //pin core
              cpu_set_t cpuset;
              CPU_ZERO(&cpuset);
              CPU_SET(workerPinningPositionList[i], &cpuset);
              int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
              if (rc != 0) {
                  NES_ERROR("Error calling pthread_setaffinity_np: " << rc);
              } else {
                  NES_WARNING("worker " << i << " pins to core=" << workerPinningPositionList[i]);
              }
          } else {
              NES_THROW_RUNTIME_ERROR(
                  "NES_USE_ONE_QUEUE_PER_NUMA_NODE or NES_USE_ONE_QUEUE_PER_QUERY require a mapping list");
          }
#endif

#ifdef NES_USE_ONE_QUEUE_PER_NUMA_NODE
          NES_ASSERT(false, "I don't think this works anymore");
//            auto nodeOfCpu = numa_node_of_cpu(threadToCoreMapping);
//            auto numaNodeIndex = nodeOfCpu;
//            NES_ASSERT(numaNodeIndex <= (int) bufferManagers.size(), "requested buffer manager idx is too large");
//
//            localBufferManager = bufferManagers[numaNodeIndex];
//            NES_ASSERT(numThreads <= workerPinningPositionList.size(), "Not enough worker positions for pinning are provided");
//            std::stringstream ss;
//            ss << "Worker thread " << i << " pins to core=" << workerPinningPositionList[i]
//               << " will use numa node =" << numaNodeIndex << std::endl;
//            std::cout << ss.str();
//            queueIdx = numaNodeIndex;
#elif defined(NES_USE_ONE_QUEUE_PER_QUERY)
          //give one buffer manager per queue
          if (queuePinListMapping.empty()) {
              queueIdx = 0;
          } else {
              NES_WARNING("worker " << i << " pins to queue=" << queuePinListMapping[i]);
              queueIdx = queuePinListMapping[i];
          }

          NES_WARNING("worker " << i << " uses buffer manager=" << i);
          localBufferManager = bufferManagers[i];
#else
          localBufferManager = bufferManagers[0];
#endif

          barrier->wait();
          NES_ASSERT(localBufferManager != NULL, "localBufferManager is null");
#ifdef ENABLE_PAPI_PROFILER
          auto path = std::filesystem::path("worker_" + std::to_string(NesThread::getId()) + ".csv");
          auto profiler = std::make_shared<Profiler::PapiCpuProfiler>(Profiler::PapiCpuProfiler::Presets::CachePresets,
                                                                      std::ofstream(path, std::ofstream::out),
                                                                      NesThread::getId(),
                                                                      NesThread::getId());
          queryManager->cpuProfilers[NesThread::getId() % queryManager->cpuProfilers.size()] = profiler;
#endif
          // TODO (2310) properly initialize the profiler with a file, thread, and core id
#if defined(NES_USE_ONE_QUEUE_PER_NUMA_NODE) || defined(NES_USE_ONE_QUEUE_PER_QUERY)
          runningRoutine(WorkerContext(NesThread::getId(), localBufferManager, numberOfBuffersPerWorker, queueIdx));
#else
          runningRoutine(WorkerContext(NesThread::getId(), localBufferManager, numberOfBuffersPerWorker));
#endif
        });
    }
    barrier->wait();
    NES_DEBUG("Threadpool: start return from start");
    return true;
}

bool ThreadPool::stop() {
    std::unique_lock lock(reconfigLock);
    NES_DEBUG(
        "ThreadPool: stop thread pool while " << (running.load() ? "running" : "not running") << " with " << numThreads
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
    for (auto& thread: threads) {
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