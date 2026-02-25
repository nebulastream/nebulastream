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

class UnionLogicalOperator
{
public:
    explicit UnionLogicalOperator();

    [[nodiscard]] bool operator==(const UnionLogicalOperator& rhs) const;

    [[nodiscard]] UnionLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] UnionLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] UnionLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const;

    /// Set the schemas directly without inference used for operator registration
    [[nodiscard]] UnionLogicalOperator setInputSchemas(std::vector<Schema> inputSchemas) const;
    [[nodiscard]] UnionLogicalOperator setOutputSchema(const Schema& outputSchema) const;

private:
    static constexpr std::string_view NAME = "Union";

    std::vector<LogicalOperator> children;
    std::vector<Schema> inputSchemas;
    Schema outputSchema;
    TraitSet traitSet;
};

template <>
struct Reflector<UnionLogicalOperator>
{
    Reflected operator()(const UnionLogicalOperator&) const;
};

template <>
struct Unreflector<UnionLogicalOperator>
{
    UnionLogicalOperator operator()(const Reflected&, const ReflectionContext&) const;
};

static_assert(LogicalOperatorConcept<UnionLogicalOperator>);

}
