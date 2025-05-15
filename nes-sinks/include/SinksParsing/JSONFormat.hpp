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
#include <SinksParsing/Format.hpp>

#include <memory>
#include <ostream>
#include <variant>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Sinks
{

class JSONFormat : public Format
{
public:
    /// Stores precalculated offsets based on the input schema.
    /// The JSONFormat class constructs the formatting context during its construction and stores it as a member to speed up
    /// the actual formatting.
    struct FormattingContext
    {
        size_t schemaSizeInBytes{};
        std::vector<size_t> offsets;
        std::vector<std::string> names;
        std::vector<DataType> physicalTypes;
    };

    explicit JSONFormat(Schema schema);

    /// Return formatted content of TupleBuffer, contains timestamp if specified in config.
    std::string getFormattedBuffer(const Memory::TupleBuffer& inputBuffer) const override;

    /// Reads a TupleBuffer and uses the supplied 'schema' to format it to JSON. Returns result as a string.
    static std::string tupleBufferToFormattedJSONString(Memory::TupleBuffer tbuffer, const FormattingContext& formattingContext);
    std::ostream& toString(std::ostream& os) const override { return os << *this; }
    friend std::ostream& operator<<(std::ostream& out, const JSONFormat& format);

private:
    FormattingContext formattingContext;
};

}
