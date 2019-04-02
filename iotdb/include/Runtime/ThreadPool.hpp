/*
 * ThreadPool.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef THREADPOOL_H_
#define THREADPOOL_H_

#include <iostream>
#include <thread>
#include <vector>
namespace iotdb {
class ThreadPool {
  public:
    void worker_thread();

  void start(size_t numberOfThreads);
  void start();

    void stop();

    static ThreadPool& instance();

    void setNumberOfThreads(size_t size) { numThreads = size; };
    size_t getNumberOfThreads() { return numThreads; };

  private:
    ThreadPool();
    ~ThreadPool();

    bool run;
    size_t numThreads;
    std::vector<std::thread> threads;
};
} // namespace iotdb

#endif /* THREADPOOL_H_ */
