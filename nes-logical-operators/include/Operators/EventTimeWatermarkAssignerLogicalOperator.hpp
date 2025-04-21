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

#include <API/TimeUnit.hpp>
#include <Abstract/LogicalFunction.hpp>
#include <Operators/LogicalOperator.hpp>

namespace NES
{

class EventTimeWatermarkAssignerLogicalOperator : public LogicalOperatorConcept
{
public:
    EventTimeWatermarkAssignerLogicalOperator(LogicalFunction onField, Windowing::TimeUnit unit);
    std::string_view getName() const noexcept override;

    [[nodiscard]] bool operator==(const LogicalOperatorConcept& rhs) const override;
    //bool inferSchema();

    [[nodiscard]] SerializableOperator serialize() const override;
    [[nodiscard]] std::string toString() const override;

    void setChildren(std::vector<LogicalOperator> children) override { this->children = children; }

    std::vector<LogicalOperator> getChildren() const override { return children; }

    Optimizer::TraitSet getTraitSet() const override { return {}; }

    std::vector<std::vector<OriginId>> getInputOriginIds() const override { return {}; }
    std::vector<OriginId> getOutputOriginIds() const override { return {}; }

    std::vector<Schema> getInputSchemas() const override { return {inputSchema}; };
    Schema getOutputSchema() const override { return outputSchema; };

private:
    static constexpr std::string_view NAME = "EventTimeWatermarkAssigner";
    LogicalFunction onField;
    Windowing::TimeUnit unit;

    std::vector<LogicalOperator> children;
    Schema inputSchema, outputSchema;
};
}
