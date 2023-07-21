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
#ifndef NES_UTILITYFUNCTIONS_HPP
#define NES_UTILITYFUNCTIONS_HPP

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Execution/MemoryProvider/ColumnMemoryProvider.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <utility>

namespace NES::Runtime::Execution::Util {
/**
 * @brief Creates a TupleBuffer from recordPtr
 * @param recordPtr
 * @param schema
 * @param bufferManager
 * @return Filled tupleBuffer
 */
Runtime::TupleBuffer getBufferFromPointer(uint8_t* recordPtr, const SchemaPtr& schema, BufferManagerPtr bufferManager);

/**
 * @brief Writes from the nautilusRecord to the record at index recordIndex
 * @param recordIndex
 * @param baseBufferPtr
 * @param nautilusRecord
 * @param schema
 * @param bufferManager
 */
void writeNautilusRecord(uint64_t recordIndex,
                         int8_t* baseBufferPtr,
                         Nautilus::Record nautilusRecord,
                         SchemaPtr schema,
                         BufferManagerPtr bufferManager);

/**
 * @brief Merges a vector of TupleBuffers into one TupleBuffer. If the buffers in the vector do not fit into one TupleBuffer, the
 *        buffers that do not fit will be discarded.
 * @param buffersToBeMerged
 * @param schema
 * @param bufferManager
 * @return merged TupleBuffer
 */
Runtime::TupleBuffer mergeBuffers(std::vector<Runtime::TupleBuffer>& buffersToBeMerged,
                                  const SchemaPtr schema,
                                  Runtime::BufferManagerPtr bufferManager);

/**
 * @brief this function iterates through all buffers and merges all buffers into a newly created vector so that the new buffers
 * contain as much tuples as possible. Additionally, there are only tuples in a buffer that belong to the same window
 * @param buffers
 * @param schema
 * @param timeStampFieldName
 * @param bufferManager
 * @return buffer of tuples
 */
std::vector<Runtime::TupleBuffer> mergeBuffersSameWindow(std::vector<Runtime::TupleBuffer>& buffers,
                                                         SchemaPtr schema,
                                                         const std::string& timeStampFieldName,
                                                         BufferManagerPtr bufferManager,
                                                         uint64_t windowSize);

/**
 * @brief Iterates through buffersToSort and sorts each buffer ascending to sortFieldName
 * @param buffersToSort
 * @param schema
 * @param sortFieldName
 * @param bufferManager
 * @return sorted buffers
 */
std::vector<Runtime::TupleBuffer> sortBuffersInTupleBuffer(std::vector<Runtime::TupleBuffer>& buffersToSort,
                                                           SchemaPtr schema,
                                                           const std::string& sortFieldName,
                                                           BufferManagerPtr bufferManager);

/**
 * @brief Creates a TupleBuffer from a Nautilus::Record
 * @param nautilusRecord
 * @param schema
 * @param bufferManager
 * @return Filled TupleBuffer
 */
Runtime::TupleBuffer
getBufferFromRecord(const Nautilus::Record& nautilusRecord, SchemaPtr schema, BufferManagerPtr bufferManager);

/**
 * @brief create CSV lines from the tuples
 * @param tbuffer the tuple buffer
 * @param schema how to read the tuples from the buffer
 * @return a full string stream as string
 */
std::string printTupleBufferAsCSV(Runtime::TupleBuffer tbuffer, const SchemaPtr& schema);

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

/**
 * @brief function to replace all string occurrences
 * @param data input string will be replaced in-place
 * @param toSearch search string
 * @param replaceStr replace string
 */
void findAndReplaceAll(std::string& data, const std::string& toSearch, const std::string& replaceStr);

/**
 * @brief Returns a vector that contains all the physical types from the schema
 * @param schema
 * @return std::vector<PhysicalTypePtr>
 */
std::vector<PhysicalTypePtr> getPhysicalTypes(SchemaPtr schema);

/**
 * @brief checks if the buffers contain the same tuples
 * @param buffer1
 * @param buffer2
 * @param schema
 * @return True if the buffers contain the same tuples
 */
bool checkIfBuffersAreEqual(Runtime::TupleBuffer buffer1, Runtime::TupleBuffer buffer2, uint64_t schemaSizeInByte);

/**
 * @brief Gets the physical type of a given type given as template parameter
 * @return PhysicalTypePtr
 */
template<typename T>
PhysicalTypePtr getPhysicalTypePtr() {
    DefaultPhysicalTypeFactory physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr type;
    if (typeid(int32_t) == typeid(T)) {
        type = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt32());
    } else if (typeid(uint32_t) == typeid(T)) {
        type = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createUInt32());
    } else if (typeid(int64_t) == typeid(T)) {
        type = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    } else if (typeid(uint64_t) == typeid(T)) {
        type = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createUInt64());
    } else if (typeid(int16_t) == typeid(T)) {
        type = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt16());
    } else if (typeid(uint16_t) == typeid(T)) {
        type = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createUInt16());
    } else if (typeid(int8_t) == typeid(T)) {
        type = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt8());
    } else if (typeid(uint8_t) == typeid(T)) {
        type = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createUInt8());
    } else if (typeid(float) == typeid(T)) {
        type = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createFloat());
    } else if (typeid(double) == typeid(T)) {
        type = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createDouble());
    } else {
        NES_THROW_RUNTIME_ERROR("Type not supported");
    }
    return type;
}

}// namespace NES::Runtime::Execution::Util

#endif//NES_UTILITYFUNCTIONS_HPP
