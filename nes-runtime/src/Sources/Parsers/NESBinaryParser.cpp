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
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <absl/numeric/bits.h>
#include <absl/types/span.h>
#include <string>
#include <utility>

namespace NES {

NESBinaryParser::NESBinaryParser() : Parser({}) {}

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
                                                   const SchemaPtr& schema,
                                                   const Runtime::BufferManagerPtr&) {
#ifdef NES_DEBUG_MODE
    NES_ASSERT(binaryBuffer.size() % schema->getSchemaSizeInBytes() == 0, "Buffer size does not match expected buffer size");
    NES_ASSERT(binaryBuffer.size() <= dynamicBuffer.getBuffer().getBufferSize(), "Buffer size missmatch");
    assertValidMemoryLayout(*dynamicBuffer.getMemoryLayout());
#endif
    std::memcpy(dynamicBuffer.getBuffer().getBuffer(), binaryBuffer.data(), binaryBuffer.size());
    dynamicBuffer.getBuffer().setNumberOfTuples(binaryBuffer.size() / schema->getSchemaSizeInBytes());
    return true;
}
}// namespace NES
