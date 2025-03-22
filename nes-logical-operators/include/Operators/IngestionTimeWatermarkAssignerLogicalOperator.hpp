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

#include <string>
#include <string_view>
#include <Operators/LogicalOperator.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

class IngestionTimeWatermarkAssignerLogicalOperator final : public LogicalOperatorConcept
{
public:
    static constexpr std::string_view NAME = "IngestionTimeWatermarkAssigner";
    IngestionTimeWatermarkAssignerLogicalOperator();
    std::string_view getName() const noexcept override;

    [[nodiscard]] bool operator==(LogicalOperatorConcept const& rhs) const override;
    bool inferSchema();

    [[nodiscard]] SerializableOperator serialize() const override;
    [[nodiscard]] std::string toString() const override;

    std::vector<LogicalOperator> getChildren() const override
    {
        return children;
    }

    void setChildren(std::vector<LogicalOperator> children) override
    {
        this->children = children;
    }

    Optimizer::TraitSet getTraitSet() const override
    {
        return {};
    }

    std::vector<std::vector<OriginId>> getInputOriginIds() const override { return {}; }
    std::vector<OriginId> getOutputOriginIds() const override { return {}; }

    std::vector<Schema> getInputSchemas() const override { return {inputSchema}; };
    Schema getOutputSchema() const override { return outputSchema; };

protected:
    std::vector<LogicalOperator> children;
    Schema inputSchema;
    Schema outputSchema;
};

}
