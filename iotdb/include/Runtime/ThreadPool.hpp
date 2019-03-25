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

  void start(size_t pNumberOfThreads = 1);

  void stop();

  static ThreadPool &instance();

  void setNumberOfThreads(size_t size){numThreads = size;};

private:
  ThreadPool();
  ~ThreadPool();

  bool run;
  size_t numThreads;
  std::vector<std::thread> threads;
};
}

#endif /* THREADPOOL_H_ */
