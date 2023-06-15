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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINUTIL_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINUTIL_HPP_

#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <sys/mman.h>
#include <vector>

namespace NES {
class Schema;
using SchemaPtr = std::shared_ptr<Schema>;
}// namespace NES

namespace NES::Runtime::Execution {

enum class StreamJoinStrategy : uint8_t {
    HASH_JOIN_LOCAL,
    HASH_JOIN_GLOBAL_LOCKING,
    HASH_JOIN_GLOBAL_LOCK_FREE,
    NESTED_LOOP_JOIN
};

static constexpr auto BLOOM_FALSE_POSITIVE_RATE = 1e-2;

static constexpr auto DEFAULT_HASH_NUM_PARTITIONS = 1;
static constexpr auto DEFAULT_HASH_PAGE_SIZE = 131072;
static constexpr auto DEFAULT_HASH_PREALLOC_PAGE_COUNT = 1;
static constexpr auto DEFAULT_JOIN_STRATEGY = StreamJoinStrategy::NESTED_LOOP_JOIN;
static constexpr auto DEFAULT_HASH_TOTAL_HASH_TABLE_SIZE = 17179869184*2;


namespace Operators {
struct __attribute__((packed)) JoinPartitionIdTWindowIdentifier {
    size_t partitionId;
    size_t windowIdentifier;
};
}// namespace Operators

namespace Util {

/**
 * @brief Creates the join schema from the left and right schema
 * @param leftSchema
 * @param rightSchema
 * @param keyFieldName
 * @return
 */
SchemaPtr createJoinSchema(const SchemaPtr& leftSchema, const SchemaPtr& rightSchema, const std::string& keyFieldName);

}// namespace Util
}// namespace NES::Runtime::Execution
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINUTIL_HPP_
