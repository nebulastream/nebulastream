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
#ifndef NES_LAZYJOINUTIL_HPP
#define NES_LAZYJOINUTIL_HPP

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <sys/mman.h>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime::Execution {

static constexpr auto BLOOM_FALSE_POSITIVE_RATE = 1e-2;
static constexpr auto CHUNK_SIZE = 128;
static constexpr auto PREALLOCATED_SIZE = 1 * 1024;
static constexpr auto NUM_PARTITIONS = 512;

namespace detail {

/**
 * @brief allocates memory that is aligned
 * @tparam T
 * @param size
 * @param alignment
 * @return pointer to allocated memory
 */
template<typename T = void>
T* allocAligned(size_t size, size_t alignment = 16) {
    void* tmp = nullptr;
    NES_ASSERT2_FMT(0 == posix_memalign(&tmp, alignment, sizeof(T) * size),
                    "Cannot allocate " << sizeof(T) * size << " bytes: " << strerror(errno));
    return reinterpret_cast<T*>(tmp);
}

/**
 * @brief allocates memory that are pinned to memory
 * @tparam T
 * @tparam huge_page_size
 * @param size
 * @return pointer to allocated memory
 */
template<typename T = void, size_t huge_page_size = 1 << 21>
T* allocHugePages(size_t size) {
    void* tmp = nullptr;
    NES_ASSERT2_FMT(0 == posix_memalign(&tmp, huge_page_size, sizeof(T) * size),
                    "Cannot allocate " << sizeof(T) * size << " bytes: " << strerror(errno));
    madvise(tmp, size * sizeof(T), MADV_HUGEPAGE);
    NES_ASSERT2_FMT(tmp != nullptr, "Cannot remap as huge pages");
    mlock(tmp, size * sizeof(T));
    return reinterpret_cast<T*>(tmp);
}
} // namespace detail

namespace Operators {
struct __attribute__((packed)) JoinPartitionIdTumpleStamp {
    size_t partitionId;
    size_t lastTupleTimeStamp;
};
}

namespace Util {
/**
* @brief hashes the key with murmur hash
 * @param key
 * @return calculated hash
 */
uint64_t murmurHash(uint64_t key);

/**
 * @brief create CSV lines from the tuples
 * @param tbuffer the tuple buffer
 * @param schema how to read the tuples from the buffer
 * @return a full string stream as string
 */
std::string printTupleBufferAsCSV(Runtime::TupleBuffer tbuffer, const SchemaPtr& schema);


Runtime::TupleBuffer getBufferFromPointer(uint8_t* recordPtr, SchemaPtr schema, BufferManagerPtr bufferManager);

Runtime::TupleBuffer getBufferFromNautilus(Nautilus::Record nautilusRecord, SchemaPtr schema, BufferManagerPtr bufferManager);



} // namespace Util
} // namespace NES::Runtime::Execution
#endif//NES_LAZYJOINUTIL_HPP