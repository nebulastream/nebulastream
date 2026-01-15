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

#include <ostream>
#include <ranges>
#include <sstream>
#include <string>
#include <DataTypes/Schema.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <magic_enum/magic_enum.hpp>
#include <BackpressureChannel.hpp>
#include <ExecutablePipelineStage.hpp>

namespace NES
{

class Sink : public ExecutablePipelineStage
{
public:
    explicit Sink(BackpressureController backpressureController, const Schema& schema) : backpressureController(std::move(backpressureController)), schema(schema) { }

    /// Returns the schema represented as string.
    std::string getFormattedSchema()
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

    ~Sink() override = default;
    friend std::ostream& operator<<(std::ostream& out, const Sink& sink);

    BackpressureController backpressureController;
    Schema schema;
};

}

namespace fmt
{
template <>
struct formatter<NES::Sink> : ostream_formatter
{
};
}
