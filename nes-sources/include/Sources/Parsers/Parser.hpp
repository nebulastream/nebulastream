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

#include <string>
#include <string_view>
#include <vector>
#include <API/Schema.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>

namespace NES
{
class PhysicalType;
using PhysicalTypePtr = std::shared_ptr<PhysicalType>;
}

namespace NES::Sources
{

/// Base class for all input data parsers in NES
class Parser
{
public:
    Parser() = default;
    virtual ~Parser() = default;

    /// takes a tuple as string, casts its values to the correct types and writes it to the TupleBuffer
    virtual bool writeInputTupleToTupleBuffer(
        std::string_view inputString,
        uint64_t tupleCount,
        NES::Memory::MemoryLayouts::TestTupleBuffer& testTupleBuffer,
        const Schema& schema,
        NES::Memory::AbstractBufferProvider& bufferManager)
        = 0;

    /// casts a value in string format to the correct type and writes it to the TupleBuffer
    static void writeBasicTypeToTupleBuffer(
        std::string inputString,
        NES::Memory::MemoryLayouts::DynamicField& testTupleBufferDynamicField,
        const BasicPhysicalType& basicPhysicalType);
};

}
