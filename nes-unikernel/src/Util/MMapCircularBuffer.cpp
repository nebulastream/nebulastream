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

#include <Util/Logger/Logger.hpp>
#include <Util/MMapCircularBuffer.hpp>
#include <sys/mman.h>

MMapCircularBuffer::MMapCircularBuffer(size_t capacity) : capacity_(capacity) {
    assert(capacity_ % getpagesize() == 0 && "Cirular Buffer requires a capacity_ that is divisible by the getpagesize()");
    fd = memfd_create("queue_buffer", 0);
    ftruncate(fd, capacity_);
    data = static_cast<char*>(mmap(NULL, 2 * capacity_, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    mmap(data, capacity_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0);
    mmap(data + capacity_, capacity_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0);
}
MMapCircularBuffer::~MMapCircularBuffer() {
    munmap(data + capacity_, capacity_);
    munmap(data, capacity_);
    close(fd);
}
std::span<char> MMapCircularBuffer::reserveDataForWrite(size_t requestedSize) {
    const auto actualSize = std::min(requestedSize, capacity_ - size());
    const auto memory = std::span(data + write, actualSize);
    write += actualSize;
    return memory;
}
void MMapCircularBuffer::returnMemoryForWrite(std::span<char> reserved, size_t usedBytes) {
    assert(reserved.size() <= size());
    assert(reserved.size() >= usedBytes);

    const auto left_over = reserved.size() - usedBytes;
    write -= left_over;
}
std::span<const char> MMapCircularBuffer::popData(size_t requestedSize) {
    const auto actualSize = std::min(requestedSize, size());
    const auto memory = std::span(data + read, actualSize);
    read += actualSize;

    if (read > capacity_) {
        write -= capacity_;
        read -= capacity_;
    }

    return memory;
}

std::span<const char> MMapCircularBuffer::peekData(size_t requestedSize) {
    const auto actualSize = std::min(requestedSize, size());
    const auto memory = std::span(data + read, actualSize);
    return memory;
}

size_t MMapCircularBuffer::size() const { return (write - read); }
size_t MMapCircularBuffer::capacity() const { return capacity_; }
bool MMapCircularBuffer::full() const { return size() == capacity_; };
bool MMapCircularBuffer::empty() const { return size() == 0; }