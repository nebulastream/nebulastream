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
#include <string>
#include <Sinks/SinkDescriptor.hpp>
#include <Plans/Operator.hpp>
#include <Operators/LogicalOperator.hpp>

namespace NES
{

struct SinkLogicalOperator final : LogicalOperatorConcept
{
    /// During deserialization, we don't need to know/use the name of the sink anymore.
    SinkLogicalOperator() = default;
    /// During query parsing, we require the name of the sink and need to assign it an id.
    SinkLogicalOperator(std::string sinkName) : sinkName(std::move(sinkName)) {};
    [[nodiscard]] bool operator==(LogicalOperatorConcept const& rhs) const override;
    std::string_view getName() const noexcept override;
    bool inferSchema();

    virtual void inferInputOrigins(){};

    std::string sinkName;
    Sinks::SinkDescriptor sinkDescriptor;

    [[nodiscard]] SerializableOperator serialize() const override;

    [[nodiscard]] std::vector<Operator> getChildren() const override
    {
        return children;
    }

    [[nodiscard]] Optimizer::TraitSet getTraitSet() const override { return {}; }
    void setChildren(std::vector<Operator> children) override { this->children = children; }
    std::vector<Schema> getInputSchemas() const override { return {inputSchema}; };
    Schema getOutputSchema() const override { return outputSchema; }
    std::vector<std::vector<OriginId>> getInputOriginIds() const override { return {}; }
    std::vector<OriginId> getOutputOriginIds() const override { return {}; }

    std::string toString() const override;

private:
    std::vector<Operator> children;
    Schema inputSchema, outputSchema;
};
}
