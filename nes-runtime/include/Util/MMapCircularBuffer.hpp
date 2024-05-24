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
#if __has_include(<span> )
#include <span>
#define SPAN_TYPE std::span
#else
#include <absl/types/span.h>
#define SPAN_TYPE absl::Span
#endif

#ifndef NES_RUNTIME_INCLUDE_UTIL_MMAPCIRCULARBUFFER_HPP_
#define NES_RUNTIME_INCLUDE_UTIL_MMAPCIRCULARBUFFER_HPP_
/**
 * A CircularBuffer which efficiently allows a writer to append data to the buffer which a reader can consume from the Buffer,
 * after the writer commits its changes, during destruction.
 * This variant exploits virtual mapped memory to create the illusion of a continuous ring of memory. The API exposes a writer,
 * which reserves memory for writing and commits written memory using RAII. The reader allows a consumer to peek into the data,
 * before consuming it.
 *
 * This buffer is not thread safe! More over it is not allowed to have a reader and a writer at the same time.
 */
class MMapCircularBuffer {
    void confirmWrite(SPAN_TYPE<char> reserved, size_t used_bytes);
    void confirmRead(SPAN_TYPE<const char> reserved, size_t used_bytes);

    class writer {
        const SPAN_TYPE<char> data_;
        MMapCircularBuffer& ref;
        size_t bytes_written = 0;
        writer(const SPAN_TYPE<char>& data, MMapCircularBuffer& ref) : data_(data), ref(ref) {}

      public:
        ~writer() {
            NES_ASSERT(bytes_written <= data_.size(), "Overflow Write");
            ref.confirmWrite(data_, bytes_written);
        }
        void consume(size_t bytes_consumed) {
            NES_ASSERT(bytes_consumed + bytes_written <= data_.size(), "Overflow Write");
            bytes_written += bytes_consumed;
        };
        [[nodiscard]] char* data() const { return data_.data() + bytes_written; }
        [[nodiscard]] size_t size() const { return data_.size() - bytes_written; }
        operator const SPAN_TYPE<char>() const { return data_.subspan(bytes_written); }
        friend MMapCircularBuffer;
    };

    class reader {
        const SPAN_TYPE<const char> data;
        MMapCircularBuffer& ref;
        size_t bytes_consumed = 0;
        reader(const SPAN_TYPE<const char>& data, MMapCircularBuffer& ref) : data(data), ref(ref) {}

      public:
        ~reader() {
            NES_ASSERT(bytes_consumed <= data.size(), "Overflow Read");
            ref.confirmRead(data, bytes_consumed);
        }
        void consume(SPAN_TYPE<const char> subspan) {
            NES_ASSERT(bytes_consumed + subspan.size() <= data.size(), "Overflow Read");
            bytes_consumed += subspan.size();
        }
        SPAN_TYPE<const char> consume(size_t consumed) {
            NES_ASSERT(bytes_consumed + consumed <= data.size(), "Overflow Read");
            const auto span = data.subspan(bytes_consumed, consumed);
            bytes_consumed += consumed;
            return span;
        }
        [[nodiscard]] size_t size() const { return data.size(); }
        operator const SPAN_TYPE<const char>() const { return data.subspan(bytes_consumed); }
        friend MMapCircularBuffer;
    };

  public:
    explicit MMapCircularBuffer(size_t capacity);

    ~MMapCircularBuffer();

    /**
     * Returns a view of the readable memory. Calling consume will consume the memory once the destructor of the reader is called.
     * @return reader
     */
    reader read();

    /**
     * Returns a view of the writeable memory. Calling consume will mark the memory as used once the destructor of the writer is
     * called.
     * @return writer
     */
    writer write();

    /**
     * Gets the current size of the buffer
     * @return size in bytes
     */
    [[nodiscard]] size_t size() const;

    /**
     * Gets the total capacity of the buffer. Capacity is always a multiple of the `getpagesize()`.
     * @return capacity in bytes
     */
    [[nodiscard]] size_t capacity() const;
    [[nodiscard]] bool full() const;
    [[nodiscard]] bool empty() const;

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
