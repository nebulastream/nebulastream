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
#include <string>
#include <API/Schema.hpp>
#include <Configurations/Descriptor.hpp>
#include <LogicalFunctions/Function.hpp>
#include <Identifiers/Identifiers.hpp>
#include <LogicalOperators/Operator.hpp>
#include <Traits/OriginIdAssignerTrait.hpp>
#include <WindowTypes/Types/WindowType.hpp>

namespace NES::Logical
{

class SerializableOperator;

class JoinOperator final : public OperatorConcept
{
public:
    enum class JoinType : uint8_t
    {
        INNER_JOIN,
        CARTESIAN_PRODUCT
    };

    explicit JoinOperator(
        Function joinFunction,
        std::shared_ptr<Windowing::WindowType> windowType,
        JoinType joinType);

    /// Operator specific member
    [[nodiscard]] Function getJoinFunction() const;
    [[nodiscard]] Schema getLeftSchema() const;
    [[nodiscard]] Schema getRightSchema() const;
    [[nodiscard]] std::shared_ptr<Windowing::WindowType> getWindowType() const;
    [[nodiscard]] std::string getWindowStartFieldName() const;
    [[nodiscard]] std::string getWindowEndFieldName() const;


    /// OperatorConcept member
    [[nodiscard]] bool operator==(const OperatorConcept& rhs) const override;
    [[nodiscard]] NES::SerializableOperator serialize() const override;

    [[nodiscard]] Optimizer::TraitSet getTraitSet() const override;

    [[nodiscard]] Operator withChildren(std::vector<Operator> children) const override;
    [[nodiscard]] std::vector<Operator> getChildren() const override;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const override;
    [[nodiscard]] Schema getOutputSchema() const override;

    [[nodiscard]] std::vector<std::vector<OriginId>> getInputOriginIds() const override;
    [[nodiscard]] std::vector<OriginId> getOutputOriginIds() const override;
    [[nodiscard]] Operator withInputOriginIds(std::vector<std::vector<OriginId>> ids) const override;
    [[nodiscard]] Operator withOutputOriginIds(std::vector<OriginId> ids) const override;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;
    [[nodiscard]] std::string_view getName() const noexcept override;

    [[nodiscard]] Operator withInferredSchema(std::vector<Schema> inputSchemas) const override;


    /// Serialization
    struct ConfigParameters
    {
        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<NES::Configurations::EnumWrapper, JoinType> JOIN_TYPE{
            "joinType", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                return NES::Configurations::DescriptorConfig::tryGet(JOIN_TYPE, config);
            }};

        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<NES::Configurations::EnumWrapper, FunctionList> JOIN_FUNCTION{
            "joinFunctionName", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                return NES::Configurations::DescriptorConfig::tryGet(JOIN_TYPE, config);
            }};

        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<std::string> WINDOW_START_FIELD_NAME{
            "windowStartFieldName", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                return NES::Configurations::DescriptorConfig::tryGet(WINDOW_START_FIELD_NAME, config);
            }};

        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<std::string> WINDOW_END_FIELD_NAME{
            "windowEndFieldName", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                return NES::Configurations::DescriptorConfig::tryGet(WINDOW_END_FIELD_NAME, config);
            }};

        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<WindowInfos> WINDOW_INFOS{
            "windowInfo", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                return NES::Configurations::DescriptorConfig::tryGet(WINDOW_INFOS, config);
            }};

        static inline std::unordered_map<std::string, NES::Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
            = NES::Configurations::DescriptorConfig::createConfigParameterContainerMap(
                JOIN_TYPE, JOIN_FUNCTION, WINDOW_INFOS, WINDOW_START_FIELD_NAME, WINDOW_END_FIELD_NAME);
    };

private:
    /// Operator specific member
    static constexpr std::string_view NAME = "Join";
    Function joinFunction;
    std::shared_ptr<Windowing::WindowType> windowType;
    std::string windowStartFieldName, windowEndFieldName;
    JoinType joinType;
    Optimizer::OriginIdAssignerTrait originIdTrait;

    /// OperatorConcept member
    std::vector<Operator> children;
    std::vector<std::vector<OriginId>> inputOriginIds;
    std::vector<OriginId> outputOriginIds;
    Schema leftInputSchema, rightInputSchema, outputSchema;
};
}
