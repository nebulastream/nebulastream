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
#include <Operators/LogicalOperator.hpp>

namespace NES
{

class UnionLogicalOperator : public LogicalOperatorConcept
{
public:
    explicit UnionLogicalOperator();
    std::string_view getName() const noexcept override;

    ///infer schema of two child operators
    bool inferSchema();
    void inferInputOrigins();
    [[nodiscard]] bool operator==(LogicalOperatorConcept const& rhs) const override;

    [[nodiscard]] SerializableOperator serialize() const override;

    static std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
    validateAndFormat(std::unordered_map<std::string, std::string> config);

    struct ConfigParameters
    {
        static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
            = Configurations::DescriptorConfig::createConfigParameterContainerMap();
    };

protected:
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

    std::vector<Schema> getInputSchemas() const override { return {leftInputSchema, rightInputSchema}; };
    Schema getOutputSchema() const override { return outputSchema;}
    std::vector<std::vector<OriginId>> getInputOriginIds() const override { return {}; }
    std::vector<OriginId> getOutputOriginIds() const override { return {}; }

private:
    static constexpr std::string_view NAME = "Union";
    std::vector<Operator> children;
    Schema leftInputSchema, rightInputSchema, outputSchema;
};


}
