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

std::span<char> CircularBuffer::reserveDataForWrite() {
    auto segment_size = contigous_write_range();
    NES_TRACE("Write: Reserving {} bytes\n", segment_size);
    std::span<char> segment{buffer.data() + write, segment_size};
    advance_write(segment_size);
    return segment;
}
void CircularBuffer::returnMemoryForWrite(std::span<char> segment, size_t bytes_written) {
    NES_ASSERT(segment.size() >= bytes_written, "Overflow");
    auto bytes_returned = segment.size() - bytes_written;
    NES_TRACE("Return: Returning {} bytes\n", bytes_returned);
    return_write(bytes_returned);
}

[[nodiscard]] std::span<char> CircularBuffer::popData(std::span<char>&& possible_span) {
    auto total_read_size = std::min(possible_span.size(), static_cast<size_t>(size_));
    auto current_max_contigoues_range = contigous_read_range();
    if (current_max_contigoues_range >= total_read_size) {
        NES_TRACE("Pop: Reading {} bytes\n", total_read_size);
        std::span<char> segment{buffer.data() + read, total_read_size};
        advance_read(total_read_size);
        return segment;
    } else {
        std::memcpy(possible_span.data(), buffer.data() + read, current_max_contigoues_range);
        advance_read(current_max_contigoues_range);
        auto bytes_left_to_read = total_read_size - current_max_contigoues_range;
        NES_ASSERT(contigous_read_range() >= bytes_left_to_read, "Wrap around did not work as expected");
        std::memcpy(possible_span.data() + current_max_contigoues_range, buffer.data() + read, bytes_left_to_read);
        advance_read(bytes_left_to_read);
        return possible_span;
    }
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