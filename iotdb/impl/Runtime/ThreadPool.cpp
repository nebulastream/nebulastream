/*
 * ThreadPool.cpp
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#include <Runtime/Dispatcher.hpp>
#include <Runtime/Task.hpp>
#include <Runtime/ThreadPool.hpp>
#include <functional>
#include <Util/Logger.hpp>

namespace iotdb {

ThreadPool::ThreadPool() : run(), threads() {}

ThreadPool::~ThreadPool() {
	IOTDB_DEBUG("Threadpool: Destroying Thread Pool")
  stop();
}
void ThreadPool::worker_thread() {
  Dispatcher &dispatcher = Dispatcher::instance();
  while (run) {
    TaskPtr task = dispatcher.getWork(run);
    if (task) {
      task->execute();
      dispatcher.completedWork(task);
      IOTDB_DEBUG("Threadpool: finished task ")
    }
  }
}

void ThreadPool::start() {
  if (run)
    return;
  run = true;
  /* spawn threads */
//  auto num_threads = std::thread::hardware_concurrency();
  auto num_threads = 1;
  IOTDB_DEBUG("Threadpool: Spawning " << num_threads << " threads")
  for (uint64_t i = 0; i < num_threads; ++i) {
    threads.push_back(std::thread(std::bind(&ThreadPool::worker_thread, this)));
  }
  /* TODO: pin each thread to a fixed core */
}

void ThreadPool::stop() {
  if (!run)
    return;
  run = false;
  /* wake up all threads in the dispatcher,
   * so they notice the change in the run variable */
  Dispatcher::instance().unblockThreads();
  /* join all threads if possible */
  for (auto &thread : threads) {
    if (thread.joinable())
      thread.join();
  }
  IOTDB_DEBUG("Threadpool: All threads are joined.")
}
}
