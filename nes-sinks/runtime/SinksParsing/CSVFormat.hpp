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
#include <cstddef>
#include <memory>
#include <ostream>
#include <variant>
#include <vector>
#include <API/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <fmt/ostream.h>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/VariableSizedDataPhysicalType.hpp>

namespace NES::Sinks
{

class CSVFormat
{
public:
    /// Stores precalculated offsets based on the input schema.
    /// The CSVFormat class constructs the formatting context during its construction and stores it as a member to speed up
    /// the actual formatting.
    struct FormattingContext
    {
        size_t schemaSizeInBytes{};
        std::vector<size_t> offsets;
        std::vector<std::variant<std::shared_ptr<VariableSizedDataPhysicalType>, std::shared_ptr<BasicPhysicalType>>> physicalTypes;
    };

    explicit CSVFormat(Schema schema);
    virtual ~CSVFormat() noexcept = default;

    /// Returns the schema of formatted according to the specific SinkFormat represented as string.
    std::string getFormattedSchema() const;

    /// Return formatted content of TupleBuffer, contains timestamp if specified in config.
    std::string getFormattedBuffer(const Memory::TupleBuffer& inputBuffer);

    /// Reads a TupleBuffer and uses the supplied 'schema' to format it to CSV. Returns result as a string.
    static std::string tupleBufferToFormattedCSVString(Memory::TupleBuffer tbuffer, const FormattingContext& formattingContext);

    friend std::ostream& operator<<(std::ostream& out, const CSVFormat& format);

private:
    FormattingContext formattingContext;
    Schema schema;
};

}

FMT_OSTREAM(NES::Sinks::CSVFormat);
