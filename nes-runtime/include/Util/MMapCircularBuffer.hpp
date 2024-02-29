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

#include <cstdint>
#include <span>

#ifndef NES_RUNTIME_INCLUDE_UTIL_MMAPCIRCULARBUFFER_HPP_
#define NES_RUNTIME_INCLUDE_UTIL_MMAPCIRCULARBUFFER_HPP_
class MMapCircularBuffer {
    void confirmWrite(std::span<char> reserved, size_t used_bytes);
    void confirmRead(std::span<const char> reserved, size_t used_bytes);

    class writer {
        const std::span<char> data_;
        MMapCircularBuffer& ref;
        size_t bytes_written = 0;
        writer(const std::span<char>& data, MMapCircularBuffer& ref) : data_(data), ref(ref) {}

      public:
        ~writer() {
            NES_ASSERT(bytes_written <= data_.size(), "Overflow Write");
            ref.confirmWrite(data_, bytes_written);
        }
        void consume(size_t bytes_consumed) {
            NES_ASSERT(bytes_consumed + bytes_written <= data_.size(), "Overflow Write");
            bytes_written += bytes_consumed;
        };
        char* data() const { return std::span<char>(*this).data(); }
        size_t size() const { return std::span<char>(*this).size(); }
        operator const std::span<char>() const { return {data_.begin() + bytes_written, data_.end()}; }
        friend MMapCircularBuffer;
    };

    class reader {
        const std::span<const char> data;
        MMapCircularBuffer& ref;
        size_t bytes_consumed = 0;
        reader(const std::span<const char>& data, MMapCircularBuffer& ref) : data(data), ref(ref) {}

      public:
        ~reader() {
            NES_ASSERT(bytes_consumed <= data.size(), "Overflow Read");
            ref.confirmRead(data, bytes_consumed);
        }
        void consume(std::span<const char> subspan) {
            NES_ASSERT(bytes_consumed + subspan.size() <= data.size(), "Overflow Read");
            bytes_consumed += subspan.size();
        }
        std::span<const char> consume(size_t consumed) {
            NES_ASSERT(bytes_consumed + consumed <= data.size(), "Overflow Read");
            auto span = std::span{data.begin() + bytes_consumed, data.begin() + bytes_consumed + consumed};
            bytes_consumed += consumed;
            return span;
        }
        size_t size() const { return std::span<const char>(*this).size(); }
        operator const std::span<const char>() const { return {data.begin() + bytes_consumed, data.end()}; }
        friend MMapCircularBuffer;
    };

  public:
    explicit MMapCircularBuffer(size_t capacity);

    ~MMapCircularBuffer();

    /**
     * \brief
     * \param requested_size May return less than the requested size
     * \return returns a view into the popped data.
     *         NOTE: this data may be overridden by subsequent writes
     */
    reader read();
    writer write();

    size_t size() const;
    size_t capacity() const;
    bool full() const;
    bool empty() const;

  private:
    size_t capacity_ = 0;
    size_t read_idx = 0;
    size_t write_idx = 0;
    char* data;
    int fd;
    bool acive_read = false;
    bool acive_write = false;
};
#endif//NES_RUNTIME_INCLUDE_UTIL_MMAPCIRCULARBUFFER_HPP_
