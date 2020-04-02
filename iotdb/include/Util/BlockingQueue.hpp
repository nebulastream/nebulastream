#pragma once

#include <iostream>
#include <algorithm>
#include <queue>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <Util/Logger.hpp>
#include <optional>
namespace NES {

template<typename T>

class BlockingQueue {
 private:
  size_t capacity;
  std::queue<T> bufferQueue;
  std::mutex queueMutex;
  std::condition_variable notFull;
  std::condition_variable notEmpty;

 public:

  BlockingQueue()
      :
      capacity(0) {
  }
  ;

  inline BlockingQueue(size_t capacity)
      :
      capacity(capacity) {
    // empty
  }

  inline void setCapacity(size_t capacity) {
    std::unique_lock<std::mutex> lock(queueMutex);
    this->capacity = capacity;
  }

  inline size_t getCapacity() {
    return capacity;
  }

  inline size_t size() {
    std::unique_lock<std::mutex> lock(queueMutex);
    return bufferQueue.size();
  }

  inline bool empty() {
    std::unique_lock<std::mutex> lock(queueMutex);
    return bufferQueue.empty();
  }

  inline void reset() {
    std::unique_lock<std::mutex> lock(queueMutex);

    //TODO: I am not sure if this is the right way to go
    while (!bufferQueue.empty()) {
//      delete (char*) bufferQueue.front()->getBuffer();
      NES_DEBUG("reset pop=" << bufferQueue.front())
      bufferQueue.pop();
    }
    bufferQueue = std::queue<T>();
  }

  inline void push(const T& elem) {
    {
      std::unique_lock<std::mutex> lock(queueMutex);

      // wait while the queue is full
      while (bufferQueue.size() >= capacity) {
        notFull.wait(lock);
      }
      NES_DEBUG("BlockingQueue: pushing element " << elem)
      bufferQueue.push(elem);
    }
    notEmpty.notify_all();
  }

  inline const T pop() {
    {
      std::unique_lock<std::mutex> lock(queueMutex);

      // wait while the queue is empty
      while (bufferQueue.size() == 0) {
        notEmpty.wait(lock);
      }
      T retVal = bufferQueue.front();
      NES_DEBUG("BlockingQueue: popping element " << bufferQueue.front())
      bufferQueue.pop();

      notFull.notify_one();
      return retVal;
    }
  }

    inline const std::optional<T> popTimeout(size_t timeout_ms) {
    {
        auto timeout = std::chrono::milliseconds(timeout_ms);
        std::unique_lock<std::mutex> lock(queueMutex);

        // wait while the queue is empty
        auto ret = notEmpty.wait_for(lock, timeout, [=](){return bufferQueue.size() > 0;});
        if (!ret) {
            return std::nullopt;
        }
        T retVal = bufferQueue.front();
        NES_DEBUG("BlockingQueue: popping element timeout " << bufferQueue.front())
        bufferQueue.pop();

        notFull.notify_one();
        return retVal;
    }
}

//  inline const T& front() {
//    std::unique_lock<std::mutex> lock(queueMutex);
//
//    // wait while the queue is empty
//    while (bufferQueue.size() == 0) {
//      notEmpty.wait(lock);
//    }
//    return bufferQueue.front();
//  }

};
}
