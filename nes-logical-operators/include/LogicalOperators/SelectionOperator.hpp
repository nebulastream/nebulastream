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
#include <Configurations/Descriptor.hpp>
#include <LogicalFunctions/Function.hpp>
#include <Identifiers/Identifiers.hpp>
#include <LogicalOperators/Operator.hpp>

using namespace NES;
namespace NES::Logical
{

/// Selection operator, which contains an function as a predicate.
class SelectionOperator : public OperatorConcept
{
public:
    explicit SelectionOperator(Function predicate);

    /// Operator specific member
    [[nodiscard]] Function getPredicate() const;

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
        static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> SELECTION_FUNCTION_NAME{
            "selectionFunctionName", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                return Configurations::DescriptorConfig::tryGet(SELECTION_FUNCTION_NAME, config);
            }};

        static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
            = Configurations::DescriptorConfig::createConfigParameterContainerMap(SELECTION_FUNCTION_NAME);
    };

private:
    /// Operator specific member
    static constexpr std::string_view NAME = "Selection";
    Function predicate;

    /// OperatorConcept member
    std::vector<Operator> children;
    Schema inputSchema, outputSchema;
    std::vector<OriginId> inputOriginIds;
    std::vector<OriginId> outputOriginIds;
};
}
