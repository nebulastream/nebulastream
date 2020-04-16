#include <NodeEngine/Dispatcher.hpp>
#include <NodeEngine/Task.hpp>
#include <NodeEngine/ThreadPool.hpp>
#include <Util/Logger.hpp>
#include <functional>
#include <string.h>
namespace NES {

ThreadPool& ThreadPool::instance() {
    static ThreadPool instance;
    return instance;
}

ThreadPool::ThreadPool()
    : running(false),
      numThreads(1),
      threads() {
}

ThreadPool::~ThreadPool() {
    NES_DEBUG("Threadpool: Destroying Thread Pool")
    stop();
    NES_DEBUG("Dispatcher: Destroy threads Queue")
    threads.clear();
}

void ThreadPool::runningRoutine() {
    Dispatcher& dispatcher = Dispatcher::instance();
    while (running) {
        TaskPtr task = dispatcher.getWork(running);
        //TODO: check if TaskPtr() will really return a task that is skipped in if statement
        if (task) {
            task->execute();
            dispatcher.completedWork(task);
            NES_DEBUG("Threadpool: finished task " << task)
        } else {
            NES_DEBUG("Threadpool: task invalid " << task)
            running = false;
        }
    }
    dispatcher.cleanup();
}

bool ThreadPool::start() {
    std::unique_lock<std::mutex> lock(reconfigLock);
    if (running)
        return false;

    running = true;
    /* spawn threads */
    NES_DEBUG("Threadpool: Spawning " << numThreads << " threads")
    for (uint64_t i = 0; i < numThreads; ++i) {
        threads.emplace_back([this]() {
          runningRoutine();
        });
    }
    return true;
}

bool ThreadPool::stop() {
    std::unique_lock<std::mutex> lock(reconfigLock);
    if (!running) {
        return false;
    }
    running = false;
    /* wake up all threads in the dispatcher,
     * so they notice the change in the run variable */
    Dispatcher::instance().unblockThreads();
    /* join all threads if possible */
    for (auto& thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    threads.clear();
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

}  // namespace NES
