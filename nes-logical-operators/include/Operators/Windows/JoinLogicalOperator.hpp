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
#include <Configurations/Enums/EnumWrapper.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/OriginIdAssignerTrait.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <Windowing/WindowMetaData.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{
class SerializableOperator;

class JoinLogicalOperator final : public LogicalOperatorConcept
{
public:
    enum class JoinType : uint8_t
    {
        INNER_JOIN,
        CARTESIAN_PRODUCT
    };

    explicit JoinLogicalOperator(LogicalFunction joinFunction, std::shared_ptr<Windowing::WindowType> windowType, JoinType joinType);

    [[nodiscard]] LogicalFunction getJoinFunction() const;
    [[nodiscard]] Schema getLeftSchema() const;
    [[nodiscard]] Schema getRightSchema() const;
    [[nodiscard]] std::shared_ptr<Windowing::WindowType> getWindowType() const;
    [[nodiscard]] std::string getWindowStartFieldName() const;
    [[nodiscard]] std::string getWindowEndFieldName() const;
    [[nodiscard]] const WindowMetaData& getWindowMetaData() const;


    [[nodiscard]] bool operator==(const LogicalOperatorConcept& rhs) const override;
    [[nodiscard]] SerializableOperator serialize() const override;

    [[nodiscard]] LogicalOperator withTraitSet(TraitSet traitSet) const override;
    [[nodiscard]] TraitSet getTraitSet() const override;

    [[nodiscard]] LogicalOperator withChildren(std::vector<LogicalOperator> children) const override;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const override;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const override;
    [[nodiscard]] Schema getOutputSchema() const override;

    [[nodiscard]] std::vector<std::vector<OriginId>> getInputOriginIds() const override;
    [[nodiscard]] std::vector<OriginId> getOutputOriginIds() const override;
    [[nodiscard]] LogicalOperator withInputOriginIds(std::vector<std::vector<OriginId>> ids) const override;
    [[nodiscard]] LogicalOperator withOutputOriginIds(std::vector<OriginId> ids) const override;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;
    [[nodiscard]] std::string_view getName() const noexcept override;

    [[nodiscard]] LogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const override;


    struct ConfigParameters
    {
        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<NES::Configurations::EnumWrapper, JoinType> JOIN_TYPE{
            "joinType",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return NES::Configurations::DescriptorConfig::tryGet(JOIN_TYPE, config); }};

        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<NES::Configurations::EnumWrapper, FunctionList>
            JOIN_FUNCTION{
                "joinFunctionName",
                std::nullopt,
                [](const std::unordered_map<std::string, std::string>& config)
                { return NES::Configurations::DescriptorConfig::tryGet(JOIN_TYPE, config); }};

        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<std::string> WINDOW_START_FIELD_NAME{
            "windowStartFieldName",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return NES::Configurations::DescriptorConfig::tryGet(WINDOW_START_FIELD_NAME, config); }};

        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<std::string> WINDOW_END_FIELD_NAME{
            "windowEndFieldName",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return NES::Configurations::DescriptorConfig::tryGet(WINDOW_END_FIELD_NAME, config); }};

        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<WindowInfos> WINDOW_INFOS{
            "windowInfo",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return NES::Configurations::DescriptorConfig::tryGet(WINDOW_INFOS, config); }};

        static inline std::unordered_map<std::string, NES::Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
            = NES::Configurations::DescriptorConfig::createConfigParameterContainerMap(
                JOIN_TYPE, JOIN_FUNCTION, WINDOW_INFOS, WINDOW_START_FIELD_NAME, WINDOW_END_FIELD_NAME);
    };

private:
    static constexpr std::string_view NAME = "Join";
    LogicalFunction joinFunction;
    std::shared_ptr<Windowing::WindowType> windowType;
    WindowMetaData windowMetaData;
    JoinType joinType;
    OriginIdAssignerTrait originIdTrait;

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    std::vector<std::vector<OriginId>> inputOriginIds;
    std::vector<OriginId> outputOriginIds;
    Schema leftInputSchema, rightInputSchema, outputSchema;
};
}
