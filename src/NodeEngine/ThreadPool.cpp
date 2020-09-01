#include <NodeEngine/QueryManager.hpp>
#include <NodeEngine/Task.hpp>
#include <NodeEngine/ThreadPool.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <Util/Logger.hpp>
#include <Util/ThreadBarrier.hpp>
#include <functional>
#include <string.h>
#include <Util/ThreadNaming.hpp>
namespace NES {

ThreadPool::ThreadPool(uint64_t nodeId, QueryManagerPtr queryManager, size_t numThreads)
    : running(false),
      numThreads(numThreads),
      nodeId(nodeId),
      threads(),
      queryManager(queryManager) {
}

ThreadPool::~ThreadPool() {
    NES_DEBUG("Threadpool: Destroying Thread Pool");
    stop();
    NES_DEBUG("QueryManager: Destroy threads Queue");
    threads.clear();
}

void ThreadPool::runningRoutine(WorkerContext&& workerContext) {
    while (running) {
        Task task;
        if (!!(task = queryManager->getWork(running))) {
            auto success = task(workerContext);
            if (success) {
                queryManager->completedWork(task, workerContext);
                NES_TRACE("Threadpool: finished task " << task.toString() << " with success");
            } else {
                // TODO add here error handling (see issues 524 and 463)
                NES_TRACE("Threadpool: finished task " << task.toString() << " with error");
            }
        } else {
            NES_ERROR("Threadpool: task invalid");
            running = false;
        }
    }
    NES_DEBUG("Threadpool: end running now cleanup");
    queryManager->cleanup();
    NES_DEBUG("Threadpool: end running end cleanup");
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
            runningRoutine(WorkerContext(i));
        });
    }
    barrier->wait();
    NES_DEBUG("Threadpool:start return from start");
    return true;
}

bool ThreadPool::stop() {
    std::unique_lock<std::mutex> lock(reconfigLock);
    NES_DEBUG(
        "ThreadPool: stop thread pool while " << (running.load() ? "running" : "not running") << " with " << numThreads
                                              << " threads");
    running = false;
    /* wake up all threads in the query manager,
     * so they notice the change in the run variable */
    NES_DEBUG("Threadpool: Going to unblock " << numThreads << " threads");
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

void ThreadPool::setNumberOfThreadsWithoutRestart(size_t size) {
    numThreads = size;
}

void ThreadPool::setNumberOfThreadsWithRestart(size_t size) {
    numThreads = size;
    restart();
}

size_t ThreadPool::getNumberOfThreads() {
    return numThreads;
}

}// namespace NES
