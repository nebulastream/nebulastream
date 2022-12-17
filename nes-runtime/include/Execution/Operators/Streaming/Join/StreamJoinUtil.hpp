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
#ifndef NES_STREAMJOINUTIL_HPP
#define NES_STREAMJOINUTIL_HPP

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
static constexpr auto NUM_PARTITIONS = 16;

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
    #ifdef __linux__
        madvise(tmp, size * sizeof(T), MADV_HUGEPAGE);
    #endif
        
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

/**
 * @brief Creates a TupleBuffer from recordPtr
 * @param recordPtr
 * @param schema
 * @param bufferManager
 * @return Filled tupleBuffer
 */
Runtime::TupleBuffer getBufferFromPointer(uint8_t* recordPtr, SchemaPtr schema, BufferManagerPtr bufferManager);

/**
 * @brief Creates a TupleBuffer from a Nautilus::Record
 * @param nautilusRecord
 * @param schema
 * @param bufferManager
 * @return Filled TupleBuffer
 */
Runtime::TupleBuffer getBufferFromRecord(Nautilus::Record nautilusRecord, SchemaPtr schema, BufferManagerPtr bufferManager);

/**
 * @brief Writes from the nautilusRecord to the record at index recordIndex
 * @param recordIndex
 * @param baseBufferPtr
 * @param nautilusRecord
 * @param schema
 * @param bufferManager
 */
void writeNautilusRecord(uint64_t recordIndex, int8_t* baseBufferPtr, Nautilus::Record nautilusRecord, SchemaPtr schema, BufferManagerPtr bufferManager);

/**
 * @brief this function iterates through all buffers and merges all buffers into a newly created vector so that the new buffers
 * contain as much tuples as possible. Additionally, there are only tuples in a buffer that belong to the same window
 * @param buffers
 * @param schema
 * @param timeStampFieldName
 * @param bufferManager
 * @return buffer of tuples
 */
std::vector<Runtime::TupleBuffer> mergeBuffersSameWindow(std::vector<Runtime::TupleBuffer>& buffers, SchemaPtr schema,
                                                         const std::string& timeStampFieldName, BufferManagerPtr bufferManager,
                                                         uint64_t windowSize);

/**
 * @brief Iterates through buffersToSort and sorts each buffer ascending to sortFieldName
 * @param buffersToSort
 * @param schema
 * @param sortFieldName
 * @param bufferManager
 * @return sorted buffers
 */
std::vector<Runtime::TupleBuffer> sortBuffersInTupleBuffer(std::vector<Runtime::TupleBuffer>& buffersToSort, SchemaPtr schema,
                                                           const std::string& sortFieldName, BufferManagerPtr bufferManager);

/**
 * @brief Creates the join schema from the left and right schema
 * @param leftSchema
 * @param rightSchema
 * @param keyFieldName
 * @return
 */
SchemaPtr createJoinSchema(SchemaPtr leftSchema, SchemaPtr rightSchema, const std::string& keyFieldName);


} // namespace Util
} // namespace NES::Runtime::Execution
#endif//NES_STREAMJOINUTIL_HPP