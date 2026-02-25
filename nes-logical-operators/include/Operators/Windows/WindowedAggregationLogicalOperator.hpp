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
#include <utility>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <Windowing/WindowMetaData.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

class WindowedAggregationLogicalOperator final : public OriginIdAssigner
{
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


    [[nodiscard]] bool operator==(const WindowedAggregationLogicalOperator& rhs) const;

    [[nodiscard]] WindowedAggregationLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] WindowedAggregationLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] WindowedAggregationLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const;

private:
    static constexpr std::string_view NAME = "WindowedAggregation";
    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> aggregationFunctions;
    std::shared_ptr<Windowing::WindowType> windowType;
    std::vector<FieldAccessLogicalFunction> groupingKey;
    WindowMetaData windowMetaData;

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    Schema inputSchema, outputSchema;
};

template <>
struct Reflector<WindowedAggregationLogicalOperator>
{
    Reflected operator()(const WindowedAggregationLogicalOperator& op) const;
};

template <>
struct Unreflector<WindowedAggregationLogicalOperator>
{
    WindowedAggregationLogicalOperator operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<WindowedAggregationLogicalOperator>);

}

namespace NES::detail
{
struct ReflectedWindowAggregationLogicalOperator
{
    std::vector<std::pair<std::string, Reflected>> aggregations;
    std::vector<FieldAccessLogicalFunction> keys;
    Reflected windowType;
};
}
