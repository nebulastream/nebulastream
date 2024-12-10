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
#include <tuple>
#include <API/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/TestTupleBuffer.hpp>

namespace NES::TestUtil
{

/// Called by 'createTestTupleBufferFromTuples' to create a tuple from values.
template <bool containsVarSized = false, typename... Values>
void createTuple(Memory::MemoryLayouts::TestTupleBuffer* testTupleBuffer, Memory::BufferManager& bufferManager, const Values&... values)
{
    // Iterate over all values in the current tuple and add them to the expected KV pairs.
    auto testTuple = std::make_tuple(values...);
    if constexpr (containsVarSized)
    {
        testTupleBuffer->pushRecordToBuffer(testTuple, &bufferManager);
    } else
    {
        testTupleBuffer->pushRecordToBuffer(testTuple);
    }
}

/// Takes a schema, a buffer manager and tuples.
/// Creates a TestTupleBuffer with row layout using the schema and the buffer manager.
/// Unfolds the tuples into the TestTupleBuffer.
/// Example usage (assumes a bufferManager (shared_ptr to BufferManager object) is available):
/*
     using TestTuple = std::tuple<int, bool>;
     SchemaPtr schema = Schema::create()->addField("INT", BasicType::INT32)->addField("BOOL", BasicType::BOOLEAN);
     auto testTupleBuffer = TestUtil::createTestTupleBufferFromTuples(schema, *bufferManager,
         TestTuple(42, true), TestTuple(43, false), TestTuple(44, true), TestTuple(45, false));
*/
template <bool containsVarSized = false, bool printBuffer = false, typename... Tuples>
auto createTestTupleBufferFromTuples(
    std::shared_ptr<Schema> schema, Memory::BufferManager& bufferManager, const Tuples&... tuples)
{
    auto rowLayout = Memory::MemoryLayouts::RowLayout::create(schema, bufferManager.getBufferSize());
    auto testTupleBuffer = std::make_unique<Memory::MemoryLayouts::TestTupleBuffer>(rowLayout, bufferManager.getBufferBlocking());
    (std::apply([&](const auto&... values) { createTuple<containsVarSized>(testTupleBuffer.get(), bufferManager, values...); }, tuples), ...);

    if constexpr(printBuffer)
    {
        NES_DEBUG("test tuple buffer is: {}", testTupleBuffer->toString(schema, true));
    }
    return testTupleBuffer;
}
}