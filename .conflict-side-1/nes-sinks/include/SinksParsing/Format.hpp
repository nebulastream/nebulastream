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
#include <concepts>
#include <cstdint>
#include <ostream>
#include <ranges>
#include <sstream>
#include <string>
#include <utility>
#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <fmt/base.h>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

class Format
{
public:
    explicit Format(const Schema& schema) : schema(schema) { }

    virtual ~Format() noexcept = default;

    /// @brief Reads the variable sized data. Similar as loadAssociatedVarSizedValue, but returns a string
    /// @return Variable sized data as a string
    static std::string readVarSizedDataAsString(const TupleBuffer& tupleBuffer, VariableSizedAccess variableSizedAccess)
    {
        /// Getting the pointer to the @class VariableSizedData with the first 32-bit storing the size.
        const auto varSizedSpan = TupleBufferRef::loadAssociatedVarSizedValue(tupleBuffer, variableSizedAccess);
        const auto* const strPtrContent = reinterpret_cast<const char*>(varSizedSpan.data());
        return std::string{strPtrContent, variableSizedAccess.getSize().getRawSize()};
    }

    /// Returns the schema of formatted according to the specific SinkFormat represented as string.
    [[nodiscard]] std::string getFormattedSchema() const
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
    [[nodiscard]] virtual std::string getFormattedBuffer(const TupleBuffer& inputBuffer) const = 0;

    virtual std::ostream& toString(std::ostream&) const = 0;

    friend std::ostream& operator<<(std::ostream& os, const Format& obj) { return obj.toString(os); }

protected:
    Schema schema;
};

}

template <std::derived_from<NES::Format> Format>
struct fmt::formatter<Format> : fmt::ostream_formatter
{
};
