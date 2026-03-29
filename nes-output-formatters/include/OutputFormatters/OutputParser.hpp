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
#include <unordered_map>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>

namespace NES
{

/// Base class for all output parser implementations
class OutputParser
{
public:
    OutputParser() noexcept = default;
    virtual ~OutputParser() noexcept = default;

    /// Parse the VarVal to a string and write it into the record buffer.
    /// Parsers that are dealing with underlying datatypes such as FIXEDSIZED and STRUCT need the parserTypes-map to construct the parsers of the underlying values.
    /// ValueType is needed to pass the whole DataType of the value of value to the parser as reference. The reason we do this is that this is the only way to obtain valid pointers to
    /// field names of struct fields.
    virtual nautilus::val<uint64_t> parseAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress,
        const std::unordered_map<DataType::Type, std::string>& parserTypes,
        const DataType& valueType) const
        = 0;
};
}
