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

#ifndef NES_UNIKERNEL_UTIL_CIRCULARBUFFER_HPP
#define NES_UNIKERNEL_UTIL_CIRCULARBUFFER_HPP
#include <cstdint>
#include <span>
#include <vector>

class CircularBuffer {
  public:
    CircularBuffer(size_t capacity) : buffer(capacity), read(0), write(0), size_(0) {}

    std::span<char> reserveDataForWrite(size_t requested_size);

    void returnMemoryForWrite(std::span<char> segment, size_t used_bytes);

    struct Iterator {
        using iterator_category = std::forward_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = const char;
        using pointer = const char*;
        using reference = const char&;

        Iterator(const CircularBuffer& buffer, int64_t index, bool full) : buffer(buffer), index(index), full(full) {}

        reference operator*() const { return buffer.buffer[index]; }
        pointer operator->() { return buffer.buffer.data() + index; }

        // Prefix increment
        Iterator& operator++() {
            full = false;
            index = (index + 1) % buffer.buffer.size();
            return *this;
        }

        // Postfix increment
        Iterator operator++(int) {
            Iterator tmp = *this;
            ++(*this);
            return tmp;
        }

        friend bool operator==(const Iterator& a, const Iterator& b) { return a.index == b.index && !a.full; };
        friend bool operator!=(const Iterator& a, const Iterator& b) { return a.index != b.index || a.full; };

      private:
        const CircularBuffer& buffer;
        size_t index;
        bool full;
    };

    void popData(size_t bytes_used);
    [[nodiscard]] std::span<char> peekData(std::span<char>&& possible_dst, size_t max_request);

    [[nodiscard]] Iterator begin() const { return {*this, read, full()}; }

    [[nodiscard]] Iterator end() const { return {*this, (read + size_) % static_cast<int64_t>(buffer.size()), full()}; }

    size_t size() const;

    bool full() const;

    bool empty() const;

    size_t capacity() const;

  private:
    size_t contigous_write_range() const;

    size_t contigous_read_range() const;

    void return_write(size_t bytes_returned);

    void advance_write(size_t bytes_written);

    void advance_read(size_t bytes_read);

    std::vector<char> buffer;
    int64_t read;
    int64_t write;
    int64_t size_;
};
#endif//NES_UNIKERNEL_UTIL_CIRCULARBUFFER_HPP
