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

static constexpr auto BLOOM_FALSE_POSITIVE_RATE = 1e-2;
static constexpr auto PAGE_SIZE = 128;
static constexpr auto NUM_PREALLOC_PAGES = 1 * 1024;
static constexpr auto NUM_PARTITIONS = 1;
//static constexpr auto DEFAULT_MEM_SIZE_JOIN = 1024 * 1024 * 1024UL;
static constexpr auto DEFAULT_MEM_SIZE_JOIN = 1024 * 1024 * 1024UL * 8;

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
SchemaPtr createJoinSchema(SchemaPtr leftSchema, SchemaPtr rightSchema, const std::string& keyFieldName);

// TODO Once #3693 is done, we can use the same function in UtilityFunction
/**
 * @brief Creates multiple TupleBuffers from the csv file until the lastTimeStamp has been read
 * @param csvFile
 * @param schema
 * @param timeStampFieldName
 * @param lastTimeStamp
 * @param bufferManager
 * @return Vector of TupleBuffers
 */
[[maybe_unused]] std::vector<Runtime::TupleBuffer> createBuffersFromCSVFile(const std::string& csvFile,
                                                                            const SchemaPtr& schema,
                                                                            Runtime::BufferManagerPtr bufferManager,
                                                                            uint64_t originId = 0,
                                                                            const std::string& timestampFieldname = "ts");

// TODO Once #3693 is done, we can use the same function in UtilityFunction
/**
 * @brief casts a value in string format to the correct type and writes it to the TupleBuffer
 * @param value: string value that is cast to the PhysicalType and written to the TupleBuffer
 * @param schemaFieldIndex: field/attribute that is currently processed
 * @param tupleBuffer: the TupleBuffer to which the value is written containing the currently chosen memory layout
 * @param json: denotes whether input comes from JSON for correct parsing
 * @param schema: the schema the data are supposed to have
 * @param tupleCount: current tuple count, i.e. how many tuples have already been produced
 * @param bufferManager: the buffer manager
 */
void writeFieldValueToTupleBuffer(std::string inputString,
                                  uint64_t schemaFieldIndex,
                                  Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer,
                                  const SchemaPtr& schema,
                                  uint64_t tupleCount,
                                  const Runtime::BufferManagerPtr& bufferManager);

// TODO Once #3693 is done, we can use the same function in UtilityFunction
/**
 * @brief function to replace all string occurrences
 * @param data input string will be replaced in-place
 * @param toSearch search string
 * @param replaceStr replace string
 */
void findAndReplaceAll(std::string& data, const std::string& toSearch, const std::string& replaceStr);

// TODO Once #3693 is done, we can use the same function in UtilityFunction
/**
 * @brief Returns a vector that contains all the physical types from the schema
 * @param schema
 * @return std::vector<PhysicalTypePtr>
 */
std::vector<PhysicalTypePtr> getPhysicalTypes(SchemaPtr schema);

// TODO Once #3693 is done, we can use the same function in UtilityFunction
/**
 * @brief checks if the buffers contain the same tuples
 * @param buffer1
 * @param buffer2
 * @param schema
 * @return True if the buffers contain the same tuples
 */
bool checkIfBuffersAreEqual(Runtime::TupleBuffer buffer1, Runtime::TupleBuffer buffer2, const uint64_t schemaSizeInByte);

// TODO Once #3693 is done, we can use the same function in UtilityFunction
/**
 * @brief create CSV lines from the tuples
 * @param tbuffer the tuple buffer
 * @param schema how to read the tuples from the buffer
 * @return a full string stream as string
 */
std::string printTupleBufferAsCSV(Runtime::TupleBuffer tbuffer, const SchemaPtr& schema);

}// namespace Util
}// namespace NES::Runtime::Execution
#endif//NES_STREAMJOINUTIL_HPP