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

#include <Util/MMapCircularBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <sys/mman.h>

CircularBuffer::CircularBuffer(size_t capacity) : capacity(capacity) {
    assert(capacity % getpagesize() == 0 && "Cirular Buffer requires a capacity that is divisible by the getpagesize()");
    fd = memfd_create("queue_buffer", 0);
    ftruncate(fd, capacity);
    data = static_cast<char*>(mmap(NULL, 2 * capacity, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    mmap(data, capacity, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0);
    mmap(data + capacity, capacity, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0);
}
CircularBuffer::~CircularBuffer() {
    munmap(data + capacity, capacity);
    munmap(data, capacity);
    close(fd);
}
std::span<char> CircularBuffer::reserveDataForWrite(size_t requestedSize) {
    const auto actualSize = std::min(requestedSize, getCapacity() - size());
    const auto memory = std::span(data + write, actualSize);
    write += actualSize;
    return memory;
}
void CircularBuffer::returnMemoryForWrite(std::span<char> reserved, size_t usedBytes) {
    assert(reserved.size() <= size());
    assert(reserved.size() >= usedBytes);

    const auto left_over = reserved.size() - usedBytes;
    write -= left_over;
}
std::span<const char> CircularBuffer::popData(size_t requestedSize) {
    const auto actualSize = std::min(requestedSize, size());
    const auto memory = std::span(data + read, actualSize);
    read += actualSize;

    if (read > capacity) {
        write -= capacity;
        read -= capacity;
    }

    return memory;
}

std::span<const char> CircularBuffer::peekData(size_t requestedSize) {
    const auto actualSize = std::min(requestedSize, size());
    const auto memory = std::span(data + read, actualSize);
    return memory;
}

size_t CircularBuffer::size() const { return (write - read); }
size_t CircularBuffer::getCapacity() const { return capacity; }
bool CircularBuffer::full() const { return size() == getCapacity(); }
bool CircularBuffer::empty() const { return size() == 0; }