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
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Variant.hpp>
#include <Util/PlanRenderer.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <SerializableVariantDescriptor.pb.h>


namespace NES
{
class SerializableOperator;

namespace detail
{
template <typename T>
using ArrayOfTwo = std::array<T, 2>;
}

using JoinTimeCharacteristic = NES::VariantContainerFrom<NES::detail::ArrayOfTwo, Windowing::TimeCharacteristic>;

class JoinLogicalOperator final : public OriginIdAssigner
{
public:
    enum class JoinType : uint8_t
    {
        INNER_JOIN,
        CARTESIAN_PRODUCT
    };

    explicit JoinLogicalOperator(
        LogicalFunction joinFunction,
        std::shared_ptr<Windowing::WindowType> windowType,
        JoinType joinType,
        JoinTimeCharacteristic timeCharacteristics);
    explicit JoinLogicalOperator(std::array<LogicalOperator, 2> children, DescriptorConfig::Config config);

    static std::optional<JoinTimeCharacteristic> createJoinTimeCharacteristic(
        std::array<std::variant<Windowing::UnboundTimeCharacteristic, Windowing::BoundTimeCharacteristic>, 2> timestampFields);

    [[nodiscard]] LogicalFunction getJoinFunction() const;
    [[nodiscard]] std::shared_ptr<Windowing::WindowType> getWindowType() const;
    [[nodiscard]] const UnboundFieldBase<1>& getStartField() const;
    [[nodiscard]] const UnboundFieldBase<1>& getEndField() const;
    [[nodiscard]] JoinTimeCharacteristic getJoinTimeCharacteristics() const;


    [[nodiscard]] bool operator==(const JoinLogicalOperator& rhs) const;
    void serialize(SerializableOperator&) const;

    [[nodiscard]] JoinLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] Schema getOutputSchema() const;
    [[nodiscard]] JoinLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] std::array<LogicalOperator, 2> getBothChildren() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] JoinLogicalOperator withInferredSchema() const;

    struct ConfigParameters
    {
        static inline const DescriptorConfig::ConfigParameter<EnumWrapper, JoinType> JOIN_TYPE{
            "joinType",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(JOIN_TYPE, config); }};

        static inline const DescriptorConfig::ConfigParameter<EnumWrapper, FunctionList> JOIN_FUNCTION{
            "joinFunctionName",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(JOIN_TYPE, config); }};

        static inline const DescriptorConfig::ConfigParameter<IdentifierList> WINDOW_START_FIELD_NAME{
            "windowStartFieldName",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(WINDOW_START_FIELD_NAME, config); }};

        static inline const DescriptorConfig::ConfigParameter<std::string> WINDOW_END_FIELD_NAME{
            "windowEndFieldName",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(WINDOW_END_FIELD_NAME, config); }};

        static inline const DescriptorConfig::ConfigParameter<SerializableWindowType> WINDOW_TYPE{
            "windowType",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(WINDOW_TYPE, config); }};

        static inline const DescriptorConfig::ConfigParameter<SerializableWindowType> TIME_CHARACTERISTIC_1{
            "timeCharacteristic1",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(TIME_CHARACTERISTIC_1, config); }};

        static inline const DescriptorConfig::ConfigParameter<SerializableWindowType> TIME_CHARACTERISTIC_2{
            "timeCharacteristic2",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(TIME_CHARACTERISTIC_2, config); }};

        static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
            = DescriptorConfig::createConfigParameterContainerMap(
                JOIN_TYPE,
                JOIN_FUNCTION,
                WINDOW_TYPE,
                TIME_CHARACTERISTIC_1,
                TIME_CHARACTERISTIC_2,
                WINDOW_START_FIELD_NAME,
                WINDOW_END_FIELD_NAME);
    };

public:
    WeakLogicalOperator self;

private:
    static constexpr std::string_view NAME = "Join";

    std::shared_ptr<Windowing::WindowType> windowType;
    JoinType joinType;
    std::optional<std::array<LogicalOperator, 2>> children;
    LogicalFunction joinFunction;

    /// Set during schema inference
    std::optional<std::array<UnboundFieldBase<1>, 2>> startEndFields;
    std::optional<SchemaBase<UnboundFieldBase<1>, false>> outputSchema;
    JoinTimeCharacteristic timestampFields;

    TraitSet traitSet;
    friend struct std::hash<JoinLogicalOperator>;
};

static_assert(LogicalOperatorConcept<JoinLogicalOperator>);
}

template <>
struct std::hash<NES::JoinLogicalOperator>
{
    std::size_t operator()(const NES::JoinLogicalOperator& joinLogicalOperator) const noexcept;
};

template <>
struct std::hash<NES::JoinTimeCharacteristic>
{
    std::size_t operator()(const NES::JoinTimeCharacteristic& joinTimeCharacteristic) const noexcept;
};
