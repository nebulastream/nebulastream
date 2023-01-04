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

#include <Nautilus/Interface/Record.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <sys/mman.h>

namespace NES::Runtime::Execution {

static constexpr auto BLOOM_FALSE_POSITIVE_RATE = 1e-2;
static constexpr auto CHUNK_SIZE = 128;
static constexpr auto PREALLOCATED_SIZE = 1 * 1024;
static constexpr auto NUM_PARTITIONS = 16;

namespace Operators {
struct __attribute__((packed)) JoinPartitionIdTumpleStamp {
    size_t partitionId;
    size_t lastTupleTimeStamp;
};
}// namespace Operators

namespace Util {

// TODO #3362
/**
* @brief hashes the key with murmur hash
 * @param key
 * @return calculated hash
 */
uint64_t murmurHash(uint64_t key);

/**
 * @brief Creates the join schema from the left and right schema
 * @param leftSchema
 * @param rightSchema
 * @param keyFieldName
 * @return
 */
SchemaPtr createJoinSchema(SchemaPtr leftSchema, SchemaPtr rightSchema, const std::string& keyFieldName);

}// namespace Util
}// namespace NES::Runtime::Execution
#endif//NES_STREAMJOINUTIL_HPP