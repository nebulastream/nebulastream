#include <NodeEngine/QueryManager.hpp>
#include <NodeEngine/Task.hpp>
#include <NodeEngine/ThreadPool.hpp>
#include <Util/Logger.hpp>
#include <Util/ThreadBarrier.hpp>
#include <functional>
#include <string.h>
namespace NES {

ThreadPool::ThreadPool(QueryManagerPtr queryManager)
    : running(false),
      numThreads(1),
      threads(),
      queryManager(queryManager) {
}

ThreadPool::~ThreadPool() {
    NES_DEBUG("Threadpool: Destroying Thread Pool");
    stop();
    NES_DEBUG("QueryManager: Destroy threads Queue");
    threads.clear();
}

void ThreadPool::runningRoutine() {
    while (running) {
        TaskPtr task = queryManager->getWork(running);
        //TODO: check if TaskPtr() will really return a task that is skipped in if statement
        if (task) {
            task->execute();
            queryManager->completedWork(task);
            NES_DEBUG("Threadpool: finished task " << task);
        } else {
            NES_DEBUG("Threadpool: task invalid " << task);
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
        threads.emplace_back([this, barrier]() {
            barrier->wait();
            runningRoutine();
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
