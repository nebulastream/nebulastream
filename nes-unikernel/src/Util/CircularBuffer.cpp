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

#include <Util/CircularBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <sys/mman.h>

std::span<char> CircularBuffer::reserveDataForWrite(size_t requested_size) {
    auto segment_size = std::min(contigous_write_range(), requested_size);
    NES_TRACE("Write: Reserving {} bytes\n", segment_size);
    std::span<char> segment{buffer.data() + write, segment_size};
    advance_write(segment_size);
    return segment;
}
void CircularBuffer::returnMemoryForWrite(std::span<char> segment, size_t used_bytes) {
    NES_ASSERT(segment.size() >= used_bytes, "Overflow");
    auto bytes_returned = segment.size() - used_bytes;
    NES_TRACE("Return: Returning {} bytes\n", bytes_returned);
    return_write(bytes_returned);
}

[[nodiscard]] std::span<char> CircularBuffer::peekData(std::span<char>&& possible_span, size_t max_request_size) {

    // 1. If contigous memory > min
    // use min(contiguous, max_request) contigous memory
    // 2. Copy into span min (span_size, max_request)
    // copy contigous bytes into span
        //wrap around
    // copy request_size - contigous into span

    auto current_max_contigoues_range = contigous_read_range();

    if (current_max_contigoues_range >= possible_span.size()) {
        return std::span<char>{buffer.data() + read, std::min(current_max_contigoues_range, max_request_size)};
    } else {
        auto total_read_size = std::min(possible_span.size(), static_cast<size_t>(size_));
        // source has to be smaller
        std::memcpy(possible_span.data(), buffer.data() + read, current_max_contigoues_range);
        auto bytes_left_to_read = total_read_size - current_max_contigoues_range;
        std::memcpy(possible_span.data() + current_max_contigoues_range, buffer.data(), bytes_left_to_read);
        return possible_span;
    }
}


void CircularBuffer::popData(size_t bytes_used) {
    NES_ASSERT(bytes_used <= size(), "cannot pop data");
    advance_read(bytes_used);
}

size_t CircularBuffer::size() const { return size_; }
bool CircularBuffer::full() const { return size_ == static_cast<int64_t>(buffer.size()); }
bool CircularBuffer::empty() const { return size_ == 0; }
size_t CircularBuffer::capacity() const { return buffer.size(); }
size_t CircularBuffer::contigous_write_range() const {
    if (write >= read) {
        return std::min(static_cast<int64_t>(buffer.size()) - write, static_cast<int64_t>(buffer.size()) - size_);
    } else {
        return std::min(read - write, static_cast<int64_t>(buffer.size()) - size_);
    }
}
size_t CircularBuffer::contigous_read_range() const {
    if (read >= write) {
        return std::min(static_cast<int64_t>(buffer.size()) - read, size_);
    } else {
        return std::min(write - read, size_);
    }
}
void CircularBuffer::return_write(size_t bytes_returned) {
    size_ -= bytes_returned;
    write = (write - bytes_returned) % buffer.size();
    NES_ASSERT(size_ >= 0, "Overflow");
}
void CircularBuffer::advance_write(size_t bytes_written) {
    size_ += bytes_written;
    write = (write + bytes_written) % buffer.size();
    NES_ASSERT(size_ >= 0, "Overflow");
}
void CircularBuffer::advance_read(size_t bytes_read) {
    size_ -= bytes_read;
    read = (read + bytes_read) % buffer.size();
    NES_ASSERT(size_ >= 0, "Overflow");
}