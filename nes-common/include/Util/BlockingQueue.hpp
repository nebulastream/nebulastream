/*
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

#pragma once

#include <algorithm>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <Util/Logger/Logger.hpp>

namespace NES
{

template <typename T>

class BlockingQueue
{
private:
    uint64_t capacity;
    std::queue<T> bufferQueue;
    std::mutex queueMutex;
    std::condition_variable notFull;
    std::condition_variable notEmpty;

public:
    BlockingQueue() : capacity(0) { };

    inline BlockingQueue(uint64_t capacity) : capacity(capacity) { }

    inline void setCapacity(uint64_t capacityParam)
    {
        std::unique_lock<std::mutex> lock(queueMutex);
        this->capacity = capacityParam;
    }

    inline uint64_t getCapacity()
    {
        std::unique_lock<std::mutex> lock(queueMutex);
        return capacity;
    }

    inline uint64_t size()
    {
        std::unique_lock<std::mutex> lock(queueMutex);
        return bufferQueue.size();
    }

    inline bool empty()
    {
        std::unique_lock<std::mutex> lock(queueMutex);
        return bufferQueue.empty();
    }

    inline void reset()
    {
        std::unique_lock<std::mutex> lock(queueMutex);

        ///TODO: I am not sure if this is the right way to go
        while (!bufferQueue.empty())
        {
            NES_TRACE("reset pop={}", bufferQueue.front())
            bufferQueue.pop();
        }
        bufferQueue = std::queue<T>();
    }

    inline void push(const T& elem)
    {
        {
            std::unique_lock<std::mutex> lock(queueMutex);

            /// wait while the queue is full
            while (bufferQueue.size() >= capacity)
            {
                notFull.wait(lock);
            }
            NES_TRACE("BlockingQueue: pushing element {}", elem)
            bufferQueue.push(elem);
        }
        notEmpty.notify_all();
    }

    inline const T pop()
    {
        {
            std::unique_lock<std::mutex> lock(queueMutex);

            /// wait while the queue is empty
            while (bufferQueue.size() == 0)
            {
                notEmpty.wait(lock);
            }
            T retVal = bufferQueue.front();
            ///      NES_DEBUG("BlockingQueue: popping element {}", bufferQueue.front());
            bufferQueue.pop();

            notFull.notify_one();
            return retVal;
        }
    }

    inline const std::optional<T> popTimeout(uint64_t timeout_ms)
    {
        {
            auto timeout = std::chrono::milliseconds(timeout_ms);
            std::unique_lock<std::mutex> lock(queueMutex);

            /// wait while the queue is empty
            auto ret = notEmpty.wait_for(lock, timeout, [=, this]() { return bufferQueue.size() > 0; });
            if (!ret)
            {
                return std::nullopt;
            }
            T retVal = bufferQueue.front();
            ///        NES_DEBUG("BlockingQueue: popping element timeout {}", bufferQueue.front());
            bufferQueue.pop();

            notFull.notify_one();
            return retVal;
        }
    }
};
}
