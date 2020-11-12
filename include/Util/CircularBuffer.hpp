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

#include <memory>
#include <mutex>
namespace NES {

/**
 * @brief A templated class for a circular buffer. The implementation
 * is header-only. Currently the structure supports write/read/reset
 * and checks for isFull/isEmpty/capacity/size.
 *
 * @tparam T - type of the value in the buffer slots.
 */
template<class T>
class CircularBuffer {
  public:
    /**
     * @brief The ctor of the circ buffer, takes a size parameter.
     * @param size of the internal buffer
     */
    explicit CircularBuffer(size_t size) : maxSize(size),
                                           buffer(std::unique_ptr<T[]>(new T[size])){};

    /**
     * @brief Writes an item to a slot in the buffer. Wraps around
     * using modulo. This can potentially change to mod2.
     *
     * @param item to write
     */
    void write(T item) {
        std::unique_lock<std::mutex> lock(mutex);
        this->buffer[this->head] = item;

        if (this->full) {
            this->tail = (this->tail + 1) % this->maxSize;
        }

        this->head = (this->head + 1) % this->maxSize;
        this->full = this->head == this->tail;
    }

    /**
     * @brief Reads an item from the next slot in the buffer.
     * If the buffer is empty, it returns a default value. The
     * tail is wrapped around using tail+1 mod maxSize.
     *
     * @return the next value from the buffer
     */
    T read() {
        std::unique_lock<std::mutex> lock(mutex);
        if (this->isEmpty()) {
            return T();
        }

        auto value = this->buffer[this->tail];
        this->full = false;
        this->tail = (this->tail + 1) % this->maxSize;

        return value;
    }

    /**
     * @brief Resets the buffer, by moving head to tail.
     */
    void reset() {
        std::unique_lock<std::mutex> lock(mutex);
        this->head = this->tail;
        this->full = false;
    }

    /**
     * @brief Checks if buffer is empty.
     * @return true if not full and buffer is same as tail
     */
    bool isEmpty() const {
        return !this->full && (this->head == this->tail);
    }

    /**
     * @brief Checks is buffer is full.
     * @return value of full
     */
    bool isFull() const {
        return this->full;
    }

    /**
     * @brief Return total capacity of buffer.
     * @return value of maxSize
     */
    size_t capacity() const {
        return this->maxSize;
    }

    /**
     * @brief Return size of buffer (no. of items).
     * @return head - tail and + maxSize if tail > head
     */
    size_t size() const {
        size_t size = this->maxSize;
        if (!this->full) {
            size = (this->head >= this->tail)
                ? this->head - this->tail
                : this->maxSize + this->head - this->tail;
        }
        return size;
    }

  private:
    /**
     * @brief indicates writes
     */
    size_t head = 0;

    /**
     * @brief indicates reads
     */
    size_t tail = 0;

    /**
     * @brief maximum size of buffer
     */
    const size_t maxSize;

    /**
     * @brief the buffer, of type T[]
     */
    std::unique_ptr<T[]> buffer;

    /**
     * @brief state of fullness
     */
    bool full = false;

    std::mutex mutex;
};

}// namespace NES
#endif /* INCLUDE_CIRCULARBUFFER_H_ */
