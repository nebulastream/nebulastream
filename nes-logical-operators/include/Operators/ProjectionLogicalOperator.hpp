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
#include <Operators/LogicalOperator.hpp>

namespace NES
{

/// The projection operator only narrows down the fields of an input schema to a smaller subset. The map operator handles renaming and adding new fields.
class ProjectionLogicalOperator : public LogicalOperatorConcept
{
public:
    explicit ProjectionLogicalOperator(std::vector<LogicalFunction> functions);
    ~ProjectionLogicalOperator() override = default;
    std::string_view getName() const noexcept override;

    const std::vector<LogicalFunction>& getFunctions() const;

    [[nodiscard]] bool operator==(LogicalOperatorConcept const& rhs) const override;

    bool inferSchema();

    [[nodiscard]] SerializableOperator serialize() const override;

    static std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
    validateAndFormat(std::unordered_map<std::string, std::string> config);

    std::vector<LogicalOperator> getChildren() const override {return {};};
    void setChildren(std::vector<LogicalOperator>) override {};
    Optimizer::TraitSet getTraitSet() const override { return {};};

    struct ConfigParameters
    {
        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<std::string> PROJECTION_FUNCTION_NAME{
            "projectionFunctionName", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                return NES::Configurations::DescriptorConfig::tryGet(PROJECTION_FUNCTION_NAME, config);
            }};

        static inline std::unordered_map<std::string, NES::Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
            = NES::Configurations::DescriptorConfig::createConfigParameterContainerMap(PROJECTION_FUNCTION_NAME);
    };

    [[nodiscard]] std::string toString() const override;

    std::vector<Schema> getInputSchemas() const override { return {inputSchema}; };
    Schema getOutputSchema() const override { return outputSchema;}
    std::vector<std::vector<OriginId>> getInputOriginIds() const override { return {}; }
    std::vector<OriginId> getOutputOriginIds() const override { return {}; }

    void setInputOriginIds(std::vector<std::vector<OriginId>> ids) override
    {
        inputOriginIds = ids;
    }

    void setOutputOriginIds(std::vector<OriginId> ids) override
    {
        outputOriginIds = ids;
    }
private:
    static constexpr std::string_view NAME = "Projection";
    std::vector<LogicalFunction> functions;
    Schema inputSchema, outputSchema;
    std::vector<std::vector<OriginId>> inputOriginIds;
    std::vector<OriginId> outputOriginIds;
};

}
