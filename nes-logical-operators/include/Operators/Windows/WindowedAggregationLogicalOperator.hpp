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
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Variant.hpp>
#include <Util/PlanRenderer.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>
#include "Operators/Reprojecter.hpp"

namespace NES
{


class WindowedAggregationLogicalOperator final : public OriginIdAssigner, public Reprojecter
{
public:
    struct ProjectedAggregation
    {
        std::shared_ptr<WindowAggregationLogicalFunction> function;
        Identifier name;

        friend bool operator==(const ProjectedAggregation& lhs, const ProjectedAggregation& rhs);
    };

    using GroupingKeyType = NES::VariantContainer<
        std::vector,
        std::pair<UnboundFieldAccessLogicalFunction, std::optional<Identifier>>,
        std::pair<FieldAccessLogicalFunction, std::optional<Identifier>>>;

    WindowedAggregationLogicalOperator(
        WindowedAggregationLogicalOperator::GroupingKeyType groupingKey,
        std::vector<ProjectedAggregation> aggregationFunctions,
        std::shared_ptr<Windowing::WindowType> windowType,
        Windowing::TimeCharacteristic timeCharacteristic);
    explicit WindowedAggregationLogicalOperator(LogicalOperator child, DescriptorConfig::Config arguments);


    [[nodiscard]] bool isKeyed() const;


    [[nodiscard]] std::vector<ProjectedAggregation> getWindowAggregation() const;

    [[nodiscard]] NES::VariantContainer<std::vector, UnboundFieldAccessLogicalFunction, FieldAccessLogicalFunction>
    getGroupingKeys() const;
    [[nodiscard]] std::unordered_map<Field, std::unordered_set<Field>> getAccessedFieldsForOutput() const override;
    [[nodiscard]] const GroupingKeyType& getGroupingKeysWithName() const;

    [[nodiscard]] std::shared_ptr<Windowing::WindowType> getWindowType() const;
    [[nodiscard]] const UnboundFieldBase<1>& getWindowStartField() const;
    [[nodiscard]] const UnboundFieldBase<1>& getWindowEndField() const;
    [[nodiscard]] std::variant<Windowing::UnboundTimeCharacteristic, Windowing::BoundTimeCharacteristic> getCharacteristic() const;


    [[nodiscard]] bool operator==(const WindowedAggregationLogicalOperator& rhs) const;
    void serialize(SerializableOperator&) const;

    [[nodiscard]] WindowedAggregationLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] WindowedAggregationLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;

    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] WindowedAggregationLogicalOperator withInferredSchema() const;
    [[nodiscard]] const DynamicBase* getDynamicBase() const;

    struct ConfigParameters
    {
        static inline const DescriptorConfig::ConfigParameter<AggregationFunctionList> WINDOW_AGGREGATIONS{
            "windowAggregations",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(WINDOW_AGGREGATIONS, config); }};

        static inline const DescriptorConfig::ConfigParameter<ProjectionList> WINDOW_KEYS{
            "windowKeys",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(WINDOW_KEYS, config); }};

        static inline const DescriptorConfig::ConfigParameter<Identifier> WINDOW_START_FIELD_NAME{
            "windowStartFieldName",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(WINDOW_START_FIELD_NAME, config); }};

        static inline const DescriptorConfig::ConfigParameter<Identifier> WINDOW_END_FIELD_NAME{
            "windowEndFieldName",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(WINDOW_END_FIELD_NAME, config); }};

        static inline const DescriptorConfig::ConfigParameter<SerializableWindowType> WINDOW_TYPE{
            "windowInfos",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(WINDOW_TYPE, config); }};

        static inline const DescriptorConfig::ConfigParameter<SerializableTimeCharacteristic> TIME_CHARACTERISTIC{
            "timeCharacteristic",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(TIME_CHARACTERISTIC, config); }};

        static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
            = DescriptorConfig::createConfigParameterContainerMap(
                WINDOW_AGGREGATIONS,
                WINDOW_TYPE,
                TIME_CHARACTERISTIC,
                WINDOW_KEYS,
                WINDOW_START_FIELD_NAME,
                WINDOW_END_FIELD_NAME);
    };

public:
    WeakLogicalOperator self;

private:
    static constexpr std::string_view NAME = "WindowedAggregation";

    std::optional<LogicalOperator> child;
    std::shared_ptr<Windowing::WindowType> windowType;
    GroupingKeyType groupingKeys;
    std::vector<ProjectedAggregation> aggregationFunctions;

    /// Set during schema inference
    std::optional<std::array<UnboundFieldBase<1>, 2>> startEndField;
    std::optional<SchemaBase<UnboundFieldBase<1>, false>> outputSchema;
    Windowing::TimeCharacteristic timestampField;

    TraitSet traitSet;

    friend struct std::hash<WindowedAggregationLogicalOperator>;
};

static_assert(LogicalOperatorConcept<WindowedAggregationLogicalOperator>);

}

template <>
struct std::hash<NES::WindowedAggregationLogicalOperator>
{
    std::size_t operator()(const NES::WindowedAggregationLogicalOperator& windowedAggregationLogicalOperator) const noexcept;
};

template <>
struct std::hash<NES::WindowedAggregationLogicalOperator::ProjectedAggregation>
{
    std::size_t operator()(const NES::WindowedAggregationLogicalOperator::ProjectedAggregation& aggregation) const noexcept;
};