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
#include <unistd.h>

#ifdef __APPLE__
#include <fcntl.h>
static std::mutex anonymousFileLock;
#endif

/**
 * Quick check to verify that the mmapped region of memory has the desired effect.
 */
static bool smokeTest(char* data, size_t capacity_) {
    volatile char* test_ptr = data;
    test_ptr[0] = 'A';
    test_ptr[1] = 'B';
    test_ptr[2] = 'C';
    test_ptr[3] = 'D';
    test_ptr[4] = '\0';
    return strncmp(((const char*) test_ptr), ((const char*) test_ptr + capacity_), 4) == 0;
}

static const void* MMAP_FAIL = reinterpret_cast<void*>(-1);

MMapCircularBuffer::MMapCircularBuffer(size_t capacity) : capacity_(capacity) {
    NES_ASSERT(capacity_ % getpagesize() == 0, "Cirular Buffer requires a capacity that is divisible by the getpagesize()");
    size_t err;

    /**
     * We create a anomynous memory segement, by creating an inmemory File, resizing it to the capacity. The memory segment is
     * mapped into the processes address space twice. That way there exists now two distinct memory addresses that refer to the
     * same memory. A write to address 0 will be equivalent to a write to address capacity. This allows to create the illusion of
     * a ring in memory. Reads and writes that would previously need to account for a wrap around can now be contigous. This
     * illusion is maintained by wrapping the read and write index during reads.
     */

#ifdef __APPLE__
    // MacOS does not have the memfd_create syscall to create an anonymous memory region. Workaround is to create a file and
    // immediatly unlink (delete) it.
    {
        std::scoped_lock sl(anonymousFileLock);
        fd = open("queue_buffer", O_CREAT | O_RDWR, 0600);
        if (fd > 0) {
            NES_ASSERT(unlink("queue_buffer") == 0, "Could not unlink the anonymous file");
        }
    }
#else
    fd = memfd_create("queue_buffer", 0);
#endif
    if (fd == -1) {
        err = errno;
        goto exit_with_exception;
    }

    if (ftruncate(fd, capacity_) == -1) {
        err = errno;
        goto fd;
    }

    data = static_cast<char*>(mmap(NULL, 2 * capacity_, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    if (data == MMAP_FAIL) {
        err = errno;
        goto fd;
    }

    if (mmap(data, capacity_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0) == MMAP_FAIL) {
        err = errno;
        goto mmap_1;
    }

    if (mmap(data + capacity_, capacity_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0) == MMAP_FAIL) {
        err = errno;
        goto mmap_1;
    }

    if (!smokeTest(data, capacity_)) {
        err = ENOTSUP;
        goto mmap_2;
    }

    return;

// error handling
mmap_2:
    munmap(data + capacity_, capacity_);
mmap_1:
    munmap(data, capacity_);
fd:
    close(fd);
exit_with_exception:
    NES_THROW_RUNTIME_ERROR(fmt::format("Could not create MMapCircularBuffer: {}", std::strerror(err)));
}

MMapCircularBuffer::~MMapCircularBuffer() {
    munmap(data + capacity_, capacity_);
    munmap(data, capacity_);
    close(fd);
}

MMapCircularBuffer::reader MMapCircularBuffer::read() {
    NES_ASSERT(!acive_read, "Only a single active reader is allowed");
    const auto memory = SPAN_TYPE(data + read_idx, size());
    NES_TRACE("Reader for {} bytes. {} - {}", memory.size(), fmt::ptr(memory.data()), fmt::ptr(memory.data() + memory.size()));
    acive_read = true;
    return reader(memory, *this);
}

MMapCircularBuffer::writer MMapCircularBuffer::write() {
    NES_ASSERT(!acive_write, "Only a single active writer is allowed");
    const auto memory = SPAN_TYPE(data + write_idx, capacity_ - size());
    NES_TRACE("Writer for {} bytes. {} - {}", memory.size(), fmt::ptr(memory.data()), fmt::ptr(memory.data() + memory.size()));
    acive_write = true;
    return writer(memory, *this);
}

void MMapCircularBuffer::confirmWrite(SPAN_TYPE<char>, size_t used_bytes) {
    acive_write = false;
    write_idx += used_bytes;
    NES_TRACE("Write: Advancing {} bytes. WriteIdx: {}", used_bytes, write_idx);
}

void MMapCircularBuffer::confirmRead(SPAN_TYPE<const char>, size_t used_bytes) {
    acive_read = false;
    read_idx += used_bytes;
    NES_TRACE("Read: Advancing {} bytes. ReadIdx: {}", used_bytes, read_idx);

    if (read_idx > capacity_) {
        write_idx -= capacity_;
        read_idx -= capacity_;
        NES_TRACE("Wrap around. Write: {}, Read: {}", write_idx, read_idx);
    }
}

size_t MMapCircularBuffer::size() const { return (write_idx - read_idx); }
size_t MMapCircularBuffer::capacity() const { return capacity_; }
bool MMapCircularBuffer::full() const { return size() == capacity_; };
bool MMapCircularBuffer::empty() const { return size() == 0; }
