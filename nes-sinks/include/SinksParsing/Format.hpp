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
#include <memory>
#include <ostream>
#include <ranges>
#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <fmt/ostream.h>
#include <magic_enum/magic_enum.hpp>

namespace NES::Sinks
{

class Format
{
public:
    explicit Format(Schema schema) : schema(std::move(schema)) { }
    virtual ~Format() noexcept = default;

    /// Returns the schema of formatted according to the specific SinkFormat represented as string.
    std::string getFormattedSchema() const
    {
        PRECONDITION(schema.hasFields(), "Encountered schema without fields.");
        std::stringstream ss;
        ss << schema.getFields().front().name << ":" << magic_enum::enum_name(schema.getFields().front().dataType.type);
        for (const auto& field : schema.getFields() | std::views::drop(1))
        {
            ss << ',' << field.name << ':' << magic_enum::enum_name(field.dataType.type);
        }
        return fmt::format("{}\n", ss.str());
    }


    /// Return formatted content of TupleBuffer, contains timestamp if specified in config.
    virtual std::string getFormattedBuffer(const Memory::TupleBuffer& inputBuffer) const = 0;

    virtual std::ostream& toString(std::ostream&) const = 0;
    friend std::ostream& operator<<(std::ostream& os, const Format& obj) { return obj.toString(os); }

protected:
    Schema schema;
};

}

template <std::derived_from<NES::Sinks::Format> Format>
struct fmt::formatter<Format> : fmt::ostream_formatter
{
};
