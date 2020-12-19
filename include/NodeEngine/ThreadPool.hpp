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

#ifndef THREADPOOL_H_
#define THREADPOOL_H_

#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <iostream>
#include <thread>
#include <vector>

namespace NES::NodeEngine {

/**
 * @brief the tread pool handles the dynamic scheduling of tasks during runtime
 * @Limitations
 *    - threads are not pinned to cores
 *    - not using std::thread::hardware_concurrency() to run with max threads
 *    - no statistics are gathered
 */
class ThreadPool {
  public:
    /**
     * @brief default constructor
     */
    ThreadPool(uint64_t nodeId, QueryManagerPtr queryManager, uint16_t numThreads);

    /**
     * @brief default destructor
     */
    ~ThreadPool();

    /**
     * @brief start the Thread pool
     * 1.) check if thread pool is already running,
     *    - if yes, return false
     *    - if not set to running to true
     * 2.) spawn n threads and bind them to the running routine (routine that probes queue for runable tasks)
     * @return indicate if start succeed
     */
    bool start();

    /**
       * @brief stop the Thread pool
       * 1.) check if thread pool is already running,
       *    - if no, return false
       *    - if yes set to running to false
       * 2.) waking up all sleeping threads, during their next getWork,
       * they will recognize that the execution should terminate and exit running routine
       * 3.) join all threads, i.e., wait till all threads return
       * @return indicate if stop succeed
       */
    bool stop();

  private:
    /**
       * @brief running routine of threads, in this routine, threads repeatedly execute the following steps
       * 1.) Check if running is still true
       * 2.) If yes, request work from query manager (blocking until tasks get available)
       * 3.) If task is valid, execute the task and completeWork
       * 4.) Repeat
       */
    void runningRoutine(WorkerContext&& workerContext);

  public:
    /**
     * @brief set the number of threads in the thread pool
     * @note this effect will take place after the next restart
     * @param number of threads
     */
    void setNumberOfThreadsWithoutRestart(uint16_t size);

    /**
      * @brief set the number of threads in the thread pool
      * @param number of threads
      * @caution this will restart the engine
      */
    void setNumberOfThreadsWithRestart(uint16_t size);

    /**
     * @brief get the current number of threads in thread pool
     * @return number of current threads
     */
    uint16_t getNumberOfThreads();

    /**
     *@brief restart the node engine
     */
    void restart();

  private:
    //indicating if the thread pool is running, used for multi-thread execution
    const uint64_t nodeId;
    std::atomic<bool> running;
    std::atomic<uint16_t> numThreads;
    std::vector<std::thread> threads;
    std::mutex reconfigLock;
    QueryManagerPtr queryManager;
};

typedef std::shared_ptr<ThreadPool> ThreadPoolPtr;

}// namespace NES::NodeEngine

#endif /* THREADPOOL_H_ */
