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
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Traits/OriginIdAssignerTrait.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <Windowing/WindowMetaData.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

class WindowedAggregationLogicalOperator : public LogicalOperatorHelper<WindowedAggregationLogicalOperator>
{
    friend class LogicalOperatorHelper<WindowedAggregationLogicalOperator>;
public:
    WindowedAggregationLogicalOperator(
        std::vector<FieldAccessLogicalFunction> groupingKey,
        std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> aggregationFunctions,
        std::shared_ptr<Windowing::WindowType> windowType);


    [[nodiscard]] std::vector<std::string> getGroupByKeyNames() const;
    [[nodiscard]] bool isKeyed() const;

    [[nodiscard]] std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> getWindowAggregation() const;
    void setWindowAggregation(std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> windowAggregation);

    [[nodiscard]] std::shared_ptr<Windowing::WindowType> getWindowType() const;
    void setWindowType(std::shared_ptr<Windowing::WindowType> windowType);

    [[nodiscard]] std::vector<FieldAccessLogicalFunction> getGroupingKeys() const;

    [[nodiscard]] std::string getWindowStartFieldName() const;
    [[nodiscard]] std::string getWindowEndFieldName() const;
    [[nodiscard]] const WindowMetaData& getWindowMetaData() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;
    [[nodiscard]] std::string_view getName() const noexcept override;

    [[nodiscard]] LogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const override;

private:
    static constexpr std::string_view NAME = "WindowedAggregation";
    struct Data {
        std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> aggregationFunctions;
        std::shared_ptr<Windowing::WindowType> windowType;
        std::vector<FieldAccessLogicalFunction> groupingKey;
        WindowMetaData windowMetaData;
        OriginIdAssignerTrait originIdTrait;
    };
    Data data;
};
}

