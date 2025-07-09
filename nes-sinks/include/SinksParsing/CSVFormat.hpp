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

#include <cstddef>
#include <ostream>
#include <string>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <fmt/core.h>
#include <fmt/ostream.h>

namespace NES::Sinks
{

class CSVFormat : public Format
{
public:
    /// Stores precalculated offsets based on the input schema.
    /// The CSVFormat class constructs the formatting context during its construction and stores it as a member to speed up
    /// the actual formatting.
    struct FormattingContext
    {
        size_t schemaSizeInBytes{};
        std::vector<size_t> offsets;
        std::vector<DataType> physicalTypes;
    };

    CSVFormat(const Schema& schema);
    explicit CSVFormat(const Schema& schema, bool escapeStrings);

    /// Return formatted content of TupleBuffer, contains timestamp if specified in config.
    [[nodiscard]] std::string getFormattedBuffer(const Memory::TupleBuffer& inputBuffer) const override;

    /// Reads a TupleBuffer and uses the supplied 'schema' to format it to CSV. Returns result as a string.
    [[nodiscard]] std::string
    tupleBufferToFormattedCSVString(Memory::TupleBuffer tbuffer, const FormattingContext& formattingContext) const;

    std::ostream& toString(std::ostream& os) const override { return os << *this; }
    friend std::ostream& operator<<(std::ostream& out, const CSVFormat& format);

private:
    FormattingContext formattingContext;
    bool escapeStrings;
};

}
