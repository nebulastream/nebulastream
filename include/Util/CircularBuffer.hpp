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

#ifndef NES_CIRCULARBUFFER_H_
#define NES_CIRCULARBUFFER_H_

#include <mutex>
#include <memory>
namespace NES {

template<class T>
class CircularBuffer {
  public:
    explicit CircularBuffer(size_t size) :
        maxSize(size),
        buffer(std::unique_ptr<T[]>(new T[size])) {};

    void write(T item)
    {
        std::unique_lock<std::mutex> lock(mutex);
        this->buffer[this->head] = item;

        if (this->full) {
            this->tail = (this->tail + 1) % this->maxSize;
        }

        this->head = (this->head + 1) % this->maxSize;
        this->full = this->head == this->tail;
    }

    T read()
    {
        std::unique_lock<std::mutex> lock(mutex);
        if (this->isEmpty()) {
            return T();
        }

        auto value = this->buffer[this->tail];
        this->full = false;
        this->tail = (this->tail + 1) % this->maxSize;

        return value;
    }

    void reset()
    {
        std::unique_lock<std::mutex> lock(mutex);
        this->head = this->tail;
        this->full = false;
    }

    bool isEmpty() const
    {
        return !this->full && (this->head == this->tail);
    }

    bool isFull() const
    {
        return this->full;
    }

    size_t capacity() const
    {
        return this->maxSize;
    }

    size_t size() const
    {
        size_t size = this->maxSize;
        if(!this->full) {
            size = (this->head >= this->tail)
                   ? this->head - this->tail
                   : this->maxSize + this->head - this->tail;
        }
        return size;
    }

  private:
    std::mutex mutex;
    size_t head = 0;
    size_t tail = 0;
    const size_t maxSize;
    bool full = false;
    std::unique_ptr<T[]> buffer;
};

}// namespace NES
#endif /* INCLUDE_CIRCULARBUFFER_H_ */
