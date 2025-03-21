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
#include <Abstract/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>

namespace NES
{

/// Selection operator, which contains an function as a predicate.
class SelectionLogicalOperator : public LogicalOperatorConcept
{
public:
    explicit SelectionLogicalOperator(LogicalFunction predicate);
    std::string_view getName() const noexcept override;

    [[nodiscard]] LogicalFunction getPredicate() const;
    void setPredicate(LogicalFunction newPredicate);

    [[nodiscard]] bool operator==(LogicalOperatorConcept const& rhs) const override;

    bool inferSchema();

    [[nodiscard]] SerializableOperator serialize() const override;

    static std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
    validateAndFormat(std::unordered_map<std::string, std::string> config);

    struct ConfigParameters
    {
        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<std::string> SELECTION_FUNCTION_NAME{
            "selectionFunctionName", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                return NES::Configurations::DescriptorConfig::tryGet(SELECTION_FUNCTION_NAME, config);
            }};

        static inline std::unordered_map<std::string, NES::Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
            = NES::Configurations::DescriptorConfig::createConfigParameterContainerMap(SELECTION_FUNCTION_NAME);
    };

    std::string toString() const override;

    std::vector<Operator> getChildren() const override
    {
        return children;
    }

    void setChildren(std::vector<Operator> children) override
    {
        this->children = children;
    }

    Optimizer::TraitSet getTraitSet() const override
    {
        return {};
    }

    std::vector<Schema> getInputSchemas() const override { return {inputSchema}; };
    Schema getOutputSchema() const override { return outputSchema;}
    std::vector<std::vector<OriginId>> getInputOriginIds() const override { return {}; }
    std::vector<OriginId> getOutputOriginIds() const override { return {}; }

private:
    std::vector<Operator> children;
    static constexpr std::string_view NAME = "Selection";
    LogicalFunction predicate;
    Schema inputSchema, outputSchema;
};
}
