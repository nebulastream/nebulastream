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
#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINUTIL_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINUTIL_HPP_

#include <Execution/Operators/Streaming/Join/StreamSlice.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Common.hpp>
#include <Util/StdInt.hpp>
#include <Util/TestTupleBuffer.hpp>
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

static constexpr auto DEFAULT_HASH_NUM_PARTITIONS = 1;
static constexpr auto DEFAULT_HASH_PAGE_SIZE = 131072;
static constexpr auto DEFAULT_HASH_PREALLOC_PAGE_COUNT = 1;
static constexpr auto DEFAULT_HASH_TOTAL_HASH_TABLE_SIZE = 2 * 1024 * 1024;

/**
 * @brief Stores the information of a window. The start, end, and the identifier
 */
enum class WindowInfoState : uint8_t { BOTH_SIDES_FILLING, EMITTED_TO_PROBE, ONCE_SEEN_DURING_TERMINATION };
class WindowInfo {
  public:
    WindowInfo();
    WindowInfo(uint64_t windowStart, uint64_t windowEnd);

    bool operator<(const WindowInfo& other) const;

    std::string toString() const;

    uint64_t windowStart;
    uint64_t windowEnd;
    uint64_t windowId;
};

/**
 * @brief This struct stores a slice ptr and the state. We require this information, as we have to know the state of a slice for a given window
 */
struct SlicesAndState {
    std::vector<StreamSlicePtr> slices;
    WindowInfoState windowState;
};

namespace Operators {
struct __attribute__((packed)) JoinPartitionIdSliceIdWindow {
    uint64_t partitionId;
    uint64_t sliceIdentifierLeft;
    uint64_t sliceIdentifierRight;
    WindowInfo windowInfo;
};

/**
 * @brief This stores a sliceId and a corresponding windowId
 */
class WindowSliceIdKey {
  public:
    explicit WindowSliceIdKey(uint64_t sliceId, uint64_t windowId) : sliceId(sliceId), windowId(windowId) {}

    bool operator<(const WindowSliceIdKey& other) const {
        // For now, this should be fine as the sliceId is monotonically increasing
        if (sliceId != other.sliceId) {
            return sliceId < other.sliceId;
        } else {
            return windowId < other.windowId;
        }
    }

    uint64_t sliceId;
    uint64_t windowId;
};

/**
 * @brief This stores the left, right and output schema for a binary join
 */
struct JoinSchema {
  public:
    JoinSchema(const SchemaPtr& leftSchema, const SchemaPtr& rightSchema, const SchemaPtr& joinSchema)
        : leftSchema(leftSchema), rightSchema(rightSchema), joinSchema(joinSchema) {}

    const SchemaPtr leftSchema;
    const SchemaPtr rightSchema;
    const SchemaPtr joinSchema;
};

/**
 * @brief Stores the window start, window end and join key field name
 */
struct WindowMetaData {
  public:
    WindowMetaData(const std::string& windowStartFieldName,
                   const std::string& windowEndFieldName,
                   const std::string& windowKeyFieldName)
        : windowStartFieldName(windowStartFieldName), windowEndFieldName(windowEndFieldName),
          windowKeyFieldName(windowKeyFieldName) {}

    std::string windowStartFieldName;
    std::string windowEndFieldName;
    std::string windowKeyFieldName;
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
#endif // NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINUTIL_HPP_
