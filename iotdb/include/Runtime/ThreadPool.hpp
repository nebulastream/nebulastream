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
    ThreadPool();
    void worker_thread();

    void start();

    void stop();
    ~ThreadPool();

  private:
    bool run;
    std::vector<std::thread> threads;
};
} // namespace iotdb

#endif /* THREADPOOL_H_ */
