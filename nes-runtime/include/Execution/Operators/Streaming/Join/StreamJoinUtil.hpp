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

#include <Execution/Operators/Streaming/Join/StreamSlice.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Util/StdInt.hpp>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <sys/mman.h>
#include <vector>

namespace NES {
class Schema;
using SchemaPtr = std::shared_ptr<Schema>;
}// namespace NES

namespace NES::Runtime::Execution {

using StreamSlicePtr = std::shared_ptr<StreamSlice>;

static constexpr auto BLOOM_FALSE_POSITIVE_RATE = 1e-2;
static constexpr auto DEFAULT_HASH_NUM_PARTITIONS = 1;
static constexpr auto DEFAULT_HASH_PAGE_SIZE = 131072;
static constexpr auto DEFAULT_HASH_PREALLOC_PAGE_COUNT = 1;
static constexpr auto DEFAULT_HASH_TOTAL_HASH_TABLE_SIZE = 2 * 1024 * 1024;

/**
 * @brief Stores the information of a window. The start, end, and the identifier
 */
struct WindowInfo {
    WindowInfo(uint64_t windowStart, uint64_t windowEnd) : windowStart(windowStart), windowEnd(windowEnd), windowId(windowEnd) {}
    WindowInfo() : WindowInfo(0_u64, 0_u64) {};

    uint64_t windowStart;
    uint64_t windowEnd;
    uint64_t windowId;

    std::string toString() const {
        std::ostringstream oss;
        oss << windowStart << ","
            << windowEnd << ","
            << windowId;
        return oss.str();
    }
};

/**
 * @brief Stores triggerable slices for the windows to be triggered.
 */
struct TriggerableWindows {
    struct TriggerableSlices {
        std::vector<StreamSlicePtr> slicesToTrigger;
        WindowInfo windowInfo;

        /**
         * @brief Adds the slice to be triggered for this window
         * @param slice
         */
        void add(StreamSlicePtr slice) {
            slicesToTrigger.emplace_back(slice);
        }
    };
    std::map<uint64_t, TriggerableSlices> windowIdToTriggerableSlices;
};

namespace Operators {
struct __attribute__((packed)) JoinPartitionIdSliceIdWindow {
    uint64_t partitionId;
    uint64_t sliceIdentifierLeft;
    uint64_t sliceIdentifierRight;
    WindowInfo windowInfo;
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
