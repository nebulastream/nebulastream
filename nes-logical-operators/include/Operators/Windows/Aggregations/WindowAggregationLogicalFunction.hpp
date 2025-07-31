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
#include <string>
#include <string_view>
#include <Schema/Schema.hpp>
#include <DataTypes/DataType.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <Functions/LogicalFunction.hpp>

namespace NES
{

class WindowAggregationLogicalFunction
{
public:
    virtual ~WindowAggregationLogicalFunction() = default;

    [[nodiscard]] DataType getAggregateType() const;
    [[nodiscard]] LogicalFunction getInputFunction() const;

    [[nodiscard]] std::string toString() const;
    bool operator==(const std::shared_ptr<WindowAggregationLogicalFunction>& otherWindowAggregationLogicalFunction) const;

    /// @brief Infers the dataType of the function given the current schema and the typeInferencePhaseContext.
    [[nodiscard]] virtual std::shared_ptr<WindowAggregationLogicalFunction> withInferredType(const Schema& schema) = 0;

    [[nodiscard]] virtual SerializableAggregationFunction serialize() const = 0;

    [[nodiscard]] virtual std::string_view getName() const noexcept = 0;

protected:
    explicit WindowAggregationLogicalFunction(DataType aggregateType, LogicalFunction inputFunction);
    explicit WindowAggregationLogicalFunction(LogicalFunction inputFunction);

    DataType aggregateType;
    LogicalFunction inputFunction;
};
}


template <>
struct std::hash<NES::WindowAggregationLogicalFunction>
{
    size_t operator()(const NES::WindowAggregationLogicalFunction& windowAggregationLogicalFunction) const
    {
        return std::hash<std::string_view>{}(windowAggregationLogicalFunction.getName());
    }
};