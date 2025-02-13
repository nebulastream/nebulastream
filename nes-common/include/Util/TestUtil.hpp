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

#pragma once

#include <cstdint>
#include <cstring>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <MemoryLayout/RowLayout.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Common.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>

namespace NES::TestUtil
{

inline bool
checkIfBuffersAreEqual(const Memory::TupleBuffer& leftBuffer, const Memory::TupleBuffer& rightBuffer, const uint64_t schemaSizeInByte)
{
    NES_DEBUG("Checking if the buffers are equal, so if they contain the same tuples...");
    if (leftBuffer.getNumberOfTuples() != rightBuffer.getNumberOfTuples())
    {
        NES_DEBUG("Buffers do not contain the same tuples, as they do not have the same number of tuples");
        return false;
    }

    std::set<uint64_t> sameTupleIndices;
    for (auto idxBuffer1 = 0UL; idxBuffer1 < leftBuffer.getNumberOfTuples(); ++idxBuffer1)
    {
        bool idxFoundInBuffer2 = false;
        for (auto idxBuffer2 = 0UL; idxBuffer2 < rightBuffer.getNumberOfTuples(); ++idxBuffer2)
        {
            if (sameTupleIndices.contains(idxBuffer2))
            {
                continue;
            }

            const auto startPosBuffer1 = leftBuffer.getBuffer() + schemaSizeInByte * idxBuffer1;
            const auto startPosBuffer2 = rightBuffer.getBuffer() + schemaSizeInByte * idxBuffer2;
            if (std::memcmp(startPosBuffer1, startPosBuffer2, schemaSizeInByte) == 0)
            {
                sameTupleIndices.insert(idxBuffer2);
                idxFoundInBuffer2 = true;
                break;
            }
        }

        if (!idxFoundInBuffer2)
        {
            NES_DEBUG("Buffers do not contain the same tuples, as tuple could not be found in both buffers for idx: {}", idxBuffer1);
            return false;
        }
    }

    return (sameTupleIndices.size() == leftBuffer.getNumberOfTuples());
}

inline std::string dynamicTupleToString(
    Memory::MemoryLayouts::TestTupleBuffer& buffer, const Memory::MemoryLayouts::DynamicTuple& dynamicTuple, const Schema& schema)
{
    std::stringstream ss;
    for (uint32_t i = 0; i < schema.getFieldCount(); ++i)
    {
        const auto dataType = schema.getFieldByIndex(i)->getDataType();
        Memory::MemoryLayouts::DynamicField currentField = dynamicTuple.operator[](i);
        if (NES::Util::instanceOf<VariableSizedDataType>(dataType))
        {
            const auto index = currentField.read<Memory::TupleBuffer::NestedTupleBufferKey>();
            const auto string = Memory::MemoryLayouts::readVarSizedData(buffer.getBuffer(), index);
            ss << string << ",";
        }
        else
        {
            ss << currentField.toString() << (i == schema.getFieldCount() - 1 ? "" : ",");
        }
    }
    return ss.str();
}

inline std::string testTupleBufferToString(const Memory::TupleBuffer& buffer, const std::shared_ptr<Schema>& schema)
{
    auto testTupleBuffer = Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buffer, schema);
    if (testTupleBuffer.getBuffer().getNumberOfTuples() == 0)
    {
        return "";
    }

    std::stringstream str;
    for (const auto tupleIterator : testTupleBuffer)
    {
        str << dynamicTupleToString(testTupleBuffer, tupleIterator, *schema) << '\n';
    }
    return str.str();
}

inline Memory::TupleBuffer copyStringDataToTupleBuffer(const std::string_view rawData, NES::Memory::TupleBuffer tupleBuffer)
{
    PRECONDITION(
        tupleBuffer.getBufferSize() >= rawData.size(),
        "{} < {}, size of TupleBuffer is not sufficient to contain string",
        tupleBuffer.getBufferSize(),
        rawData.size());
    std::memcpy(tupleBuffer.getBuffer(), rawData.data(), rawData.size());
    tupleBuffer.setNumberOfTuples(rawData.size());
    return tupleBuffer;
}

/// Called by 'createTupleBufferFromTuples' to create a tuple from values.
template <bool containsVarSized = false, typename... Values>
void createTuple(Memory::MemoryLayouts::TestTupleBuffer* testTupleBuffer, Memory::BufferManager& bufferManager, const Values&... values)
{
    /// Iterate over all values in the current tuple and add them to the expected KV pairs.
    auto testTuple = std::make_tuple(values...);
    if constexpr (containsVarSized)
    {
        testTupleBuffer->pushRecordToBuffer(testTuple, &bufferManager);
    }
    else
    {
        testTupleBuffer->pushRecordToBuffer(testTuple);
    }
}

/// Takes a schema, a buffer manager and tuples.
/// Creates a TestTupleBuffer with row layout using the schema and the buffer manager.
/// Unfolds the tuples into the TestTupleBuffer.
/// Example usage (assumes a bufferManager (shared_ptr to BufferManager object) is available):
///     using TestTuple = std::tuple<int, bool>;
///     SchemaPtr schema = Schema::create()->addField("INT", BasicType::INT32)->addField("BOOL", BasicType::BOOLEAN);
///     auto testTupleBuffer = TestUtil::createTupleBufferFromTuples(schema, *bufferManager,
///         TestTuple(42, true), TestTuple(43, false), TestTuple(44, true), TestTuple(45, false));
template <typename TupleSchema, bool containsVarSized = false, bool PrintDebug = false>
Memory::TupleBuffer
createTupleBufferFromTuples(std::shared_ptr<Schema> schema, Memory::BufferManager& bufferManager, const std::vector<TupleSchema>& tuples)
{
    PRECONDITION(schema != nullptr, "Cannot create a test tuple buffer from a schema that is null");
    PRECONDITION(bufferManager.getAvailableBuffers() != 0, "Cannot create a test tuple buffer, if there are no buffers available");
    auto rowLayout = Memory::MemoryLayouts::RowLayout::create(schema, bufferManager.getBufferSize());
    auto testTupleBuffer = std::make_unique<Memory::MemoryLayouts::TestTupleBuffer>(rowLayout, bufferManager.getBufferBlocking());

    for (const auto& testTuple : tuples)
    {
        if constexpr (containsVarSized)
        {
            testTupleBuffer->pushRecordToBuffer(testTuple, &bufferManager);
        }
        else
        {
            testTupleBuffer->pushRecordToBuffer(testTuple);
        }
    }

    if constexpr (PrintDebug)
    {
        NES_DEBUG("test tuple buffer is: {}", testTupleBuffer->toString(schema));
    }
    return testTupleBuffer->getBuffer();
}
}
