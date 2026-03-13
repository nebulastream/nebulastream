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
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

class IngestionTimeWatermarkAssignerLogicalOperator final
{
public:
    IngestionTimeWatermarkAssignerLogicalOperator();

    [[nodiscard]] bool operator==(const IngestionTimeWatermarkAssignerLogicalOperator& rhs) const;

    [[nodiscard]] IngestionTimeWatermarkAssignerLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] IngestionTimeWatermarkAssignerLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] IngestionTimeWatermarkAssignerLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const;


protected:
    static constexpr std::string_view NAME = "IngestionTimeWatermarkAssigner";

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    Schema inputSchema;
    Schema outputSchema;
};

template <>
struct Reflector<IngestionTimeWatermarkAssignerLogicalOperator>
{
    Reflected operator()(const IngestionTimeWatermarkAssignerLogicalOperator& op) const;
};

template <>
struct Unreflector<IngestionTimeWatermarkAssignerLogicalOperator>
{
    IngestionTimeWatermarkAssignerLogicalOperator operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<IngestionTimeWatermarkAssignerLogicalOperator>);

}
