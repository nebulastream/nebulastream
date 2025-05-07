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
#include <LogicalFunctions/FieldAssignmentFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <LogicalOperators/Operator.hpp>

namespace NES::Logical
{

/// Map operator, which contains a field assignment function that manipulates a field of the record.
class MapOperator : public OperatorConcept
{
public:
    MapOperator(const FieldAssignmentFunction& mapFunction);

    /// Operator specific member
    [[nodiscard]] const FieldAssignmentFunction& getMapFunction() const;

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
    static NES::Configurations::DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);
    struct ConfigParameters
    {
        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<std::string> MAP_FUNCTION_NAME{
            "mapFunction", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                return NES::Configurations::DescriptorConfig::tryGet(MAP_FUNCTION_NAME, config);
            }};

        static inline std::unordered_map<std::string, NES::Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
            = NES::Configurations::DescriptorConfig::createConfigParameterContainerMap(MAP_FUNCTION_NAME);
    };

private:
    /// Operator specific member
    static constexpr std::string_view NAME = "Map";
    FieldAssignmentFunction mapFunction;

    /// OperatorConcept member
    std::vector<Operator> children;
    Schema inputSchema, outputSchema;
    std::vector<OriginId> inputOriginIds;
    std::vector<OriginId> outputOriginIds;
};

}
