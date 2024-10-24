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

#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Sources/Parsers/NESBinaryParser.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TimeMeasurement.hpp>
#include <chrono>
#include <string>
#include <utility>

namespace NES {

NESBinaryParser::NESBinaryParser(const SchemaPtr& schema, bool addIngestionTime)
    : Parser({}), addIngestionTime(addIngestionTime) {
    tupleSize = schema->getSchemaSizeInBytes();
    if (this->addIngestionTime) {
        auto ingestionFieldName = "ingestionTime";
        const auto& field = schema->getField(ingestionFieldName);
        NES_ASSERT(field, "'ingestionTime' field missing in the input schema");
        ingestionTimeFiledName = field->getName();
    }
}

#ifdef NES_DEBUG_MODE
/**
 * NES Binary Format is only implemented for schemas without any varsized fields
 */
static void assertValidMemoryLayout(const Runtime::MemoryLayouts::MemoryLayout& layout) {
    for (const auto& type : layout.getPhysicalTypes()) {
        NES_ASSERT(!type->type->isText(), "NES Binary Parser is not implemented for Text Data");
    }
}
#endif

bool NESBinaryParser::writeInputTupleToTupleBuffer(std::string_view binaryBuffer,
                                                   uint64_t,
                                                   Runtime::MemoryLayouts::TestTupleBuffer& dynamicBuffer,
                                                   const SchemaPtr&,
                                                   const Runtime::BufferManagerPtr&) {

    auto inputBufferSize = binaryBuffer.size();
#ifdef NES_DEBUG_MODE
    NES_TRACE("Size of the read binary buffer {}, size of the schema {}, size of the tuple buffer {}",
              inputBufferSize,
              tupleSize,
              dynamicBuffer.getBuffer().getBufferSize());
    NES_ASSERT(inputBufferSize % tupleSize == 0,
               "Received buffer contains partial record. Adjust the socketBufferSize such that complete tuples can be read.");
    NES_ASSERT(inputBufferSize <= dynamicBuffer.getBuffer().getBufferSize(),
               "Received buffer should be less or equal to tuple buffer size.");
    assertValidMemoryLayout(*dynamicBuffer.getMemoryLayout());
#endif
    auto numberOfTuples = inputBufferSize / tupleSize;
    std::memcpy(dynamicBuffer.getBuffer().getBuffer(), binaryBuffer.data(), inputBufferSize);
    if (addIngestionTime) {
        //Iterate over all tuples and add ingestion time
        for (uint64_t i = 0; i < numberOfTuples; i++) {
            dynamicBuffer[i][ingestionTimeFiledName].write<uint64_t>(static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now().time_since_epoch())
                    .count()));
        }
    }
    dynamicBuffer.getBuffer().setNumberOfTuples(numberOfTuples);
    return true;
}

}// namespace NES
