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
#include <ostream>
#include <ranges>
#include <sstream>
#include <string>
#include <utility>
#include <DataTypes/UnboundSchema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <fmt/base.h>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

class Format
{
public:
    explicit Format(const UnboundSchema& schema) : schema(schema) { }

    virtual ~Format() noexcept = default;

    /// Returns the schema of formatted according to the specific SinkFormat represented as string.
    [[nodiscard]] std::string getFormattedSchema() const
    {
        PRECONDITION(!std::ranges::empty(schema), "Encountered schema without fields.");
        return fmt::format(
            "{}\n",
            fmt::join(
                schema
                    | std::views::transform([](const auto& field)
                                            { return fmt::format("{}:{}", field.getName(), magic_enum::enum_name(field.getDataType().type)); }),
                ","));
    }

    /// Return formatted content of TupleBuffer, contains timestamp if specified in config.
    [[nodiscard]] virtual std::string getFormattedBuffer(const TupleBuffer& inputBuffer) const = 0;

    virtual std::ostream& toString(std::ostream&) const = 0;

    friend std::ostream& operator<<(std::ostream& os, const Format& obj) { return obj.toString(os); }

protected:
    UnboundSchema schema;
};

}

template <std::derived_from<NES::Format> Format>
struct fmt::formatter<Format> : fmt::ostream_formatter
{
};
