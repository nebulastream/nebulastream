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
 * and checks for isFull/isEmpty/capacity/size. The isFull/isEmpty
 * bools are used to not waste a slot in the buffer and guard
 * from reading from an empty buffer as well as writing to a
 * full buffer, signalling wrap-around.
 *
 * Refs:
 * - https://www.boost.org/doc/libs/1_74_0/doc/html/boost/circular_buffer.html
 * - https://embeddedartistry.com/blog/2017/05/17/creating-a-circular-buffer-in-c-and-c
 * - https://www.approxion.com/category/circular-adventures/page/1/
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
    explicit CircularBuffer(uint64_t size) : maxSize(size),
                                             buffer(std::make_unique<T[]>(size)),
                                             mutex(){};

    /**
     * @brief Writes an item to a slot in the buffer. Wraps around
     * using modulo. This can potentially change to mod2. See
     * ref #2 in class docu.
     *
     * We immediately write an item to the current head. If
     * the buffer is full, we move the tail to mod maxSize.
     * We move the head to mod maxSize regardless. Full is
     * assigned the result of current head equals current
     * tail.
     *
     * @param item to write
     */
    void write(T item) {
        std::unique_lock<std::mutex> lock(mutex);
        buffer[head] = item;

        if (full) {
            tail = (tail + 1) % maxSize;
        }

        head = (head + 1) % maxSize;
        full = head == tail;
    }

    /**
     * @brief Reads an item from the next slot in the buffer.
     * If the buffer is empty, it returns a default value.
     * The full flag is now false and a slot is available
     * for writing. The tail is wrapped around using
     * tail+1 mod maxSize.
     *
     * @return the next value from the buffer
     */
    T read() {
        std::unique_lock<std::mutex> lock(mutex);
        if (this->isEmpty()) {
            return T();
        }

        auto value = buffer[tail];
        full = false;
        tail = (tail + 1) % maxSize;

        return value;
    }

    /**
     * @brief Resets the buffer, by moving head to tail.
     */
    void reset() {
        std::unique_lock<std::mutex> lock(mutex);
        head = tail;
        full = false;
    }

    /**
     * @brief Checks if buffer is empty.
     * @return true if not full and buffer is same as tail
     */
    bool isEmpty() const {
        return !full && (head == tail);
    }

    /**
     * @brief Checks is buffer is full.
     * @return value of full
     */
    bool isFull() const {
        return full;
    }

    /**
     * @brief Return total capacity of buffer.
     * @return value of maxSize
     */
    uint64_t capacity() const {
        return maxSize;
    }

    /**
     * @brief Return size of buffer (no. of items).
     * @return head - tail and + maxSize if tail > head
     */
    uint64_t size() const {
        uint64_t size = maxSize;
        if (!full) {
            size = (head >= tail)
                ? head - tail
                : maxSize + head - tail;
        }
        return size;
    }

  private:
    // indicates writes
    uint64_t head = 0;

    // indicates reads
    uint64_t tail = 0;

    // maximum size of buffer
    const uint64_t maxSize;

    // the buffer, of type T[]
    std::unique_ptr<T[]> buffer;

    // state of fullness
    bool full = false;

    std::mutex mutex;
};

}// namespace NES
#endif /* INCLUDE_CIRCULARBUFFER_H_ */
