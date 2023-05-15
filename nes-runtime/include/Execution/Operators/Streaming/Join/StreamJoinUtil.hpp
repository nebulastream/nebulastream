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
#include <memory>
#include <string>
#include <sys/mman.h>
#include <vector>
#include <functional>
#include <filesystem>
#include <set>

namespace NES {
class Schema;
using SchemaPtr = std::shared_ptr<Schema>;
}// namespace NES

namespace NES::Runtime::Execution {

static constexpr auto BLOOM_FALSE_POSITIVE_RATE = 1e-2;
static constexpr auto CHUNK_SIZE = 128;
static constexpr auto PREALLOCATED_SIZE = 1 * 1024;
static constexpr auto NUM_PARTITIONS = 16;
static constexpr auto DEFAULT_MEM_SIZE_JOIN = 1 * 1024 * 1024UL;

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

// TODO Once #3693 is done, we can use the same function in UtilityFunction
namespace detail {
/**
* @brief set of helper functions for splitting for different types
* @return splitting function for a given type
*/
template<typename T>
struct SplitFunctionHelper {};

template<>
struct SplitFunctionHelper<std::string> {
    static constexpr auto FUNCTION = [](std::string x) {
        return x;
    };
};

template<>
struct SplitFunctionHelper<uint64_t> {
    static constexpr auto FUNCTION = [](std::string&& str) {
        return uint64_t(std::atoll(str.c_str()));
    };
};

template<>
struct SplitFunctionHelper<uint32_t> {
    static constexpr auto FUNCTION = [](std::string&& str) {
        return uint32_t(std::atoi(str.c_str()));
    };
};

template<>
struct SplitFunctionHelper<int> {
    static constexpr auto FUNCTION = [](std::string&& str) {
        return std::atoi(str.c_str());
    };
};

template<>
struct SplitFunctionHelper<double> {
    static constexpr auto FUNCTION = [](std::string&& str) {
        return std::atof(str.c_str());
    };
};
}// namespace detail

// TODO Once #3693 is done, we can use the same function in UtilityFunction
/**
* @brief splits a string given a delimiter into multiple substrings stored in a T vector
* the delimiter is allowed to be a string rather than a char only.
* @param data - the string that is to be split
* @param delimiter - the string that is to be split upon e.g. / or -
* @param fromStringtoT - the function that converts a string to an arbitrary type T
* @return
*/
template<typename T>
std::vector<T> splitWithStringDelimiter(const std::string& inputString,
                                        const std::string& delim,
                                        std::function<T(std::string)> fromStringToT = detail::SplitFunctionHelper<T>::FUNCTION) {
    std::string copy = inputString;
    size_t pos = 0;
    std::vector<T> elems;
    while ((pos = copy.find(delim)) != std::string::npos) {
        elems.push_back(fromStringToT(copy.substr(0, pos)));
        copy.erase(0, pos + delim.length());
    }
    if (!copy.substr(0, pos).empty()) {
        elems.push_back(fromStringToT(copy.substr(0, pos)));
    }

    return elems;
}

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
 * @brief Merges a vector of TupleBuffers into one TupleBuffer. If the buffers in the vector do not fit into one TupleBuffer, the
 *        buffers that do not fit will be discarded.
 * @param buffersToBeMerged
 * @param schema
 * @param bufferManager
 * @return merged TupleBuffer
 */
Runtime::TupleBuffer
mergeBuffers(std::vector<Runtime::TupleBuffer>& buffersToBeMerged, const SchemaPtr schema,
             BufferManagerPtr bufferManager);

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