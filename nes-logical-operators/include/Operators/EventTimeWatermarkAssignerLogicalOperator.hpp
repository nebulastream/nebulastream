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

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/TimeUnit.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

class EventTimeWatermarkAssignerLogicalOperator : public LogicalOperatorHelper<EventTimeWatermarkAssignerLogicalOperator>
{
    friend class LogicalOperatorHelper<EventTimeWatermarkAssignerLogicalOperator>;
public:
    EventTimeWatermarkAssignerLogicalOperator(LogicalFunction onField, const Windowing::TimeUnit& unit);
    EventTimeWatermarkAssignerLogicalOperator() = default;

    [[nodiscard]] LogicalFunction getOnField() const;
    [[nodiscard]] Windowing::TimeUnit getUnit() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;
    [[nodiscard]] std::string_view getName() const noexcept override;
    [[nodiscard]] LogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const override;

protected:
    static constexpr std::string_view NAME = "EventTimeWatermarkAssigner";
    struct Data
    {
        std::optional<LogicalFunction> onField;
        Windowing::TimeUnit unit = Windowing::TimeUnit::Milliseconds();
    };
    Data data{};
};
}
