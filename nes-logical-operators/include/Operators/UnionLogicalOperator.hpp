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

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <SerializableOperator.pb.h>


namespace NES
{

class UnionLogicalOperator
{
public:
    explicit UnionLogicalOperator();
    explicit UnionLogicalOperator(std::vector<LogicalOperator> children);

    [[nodiscard]] bool operator==(const UnionLogicalOperator& rhs) const;
    void serialize(SerializableOperator&) const;

    [[nodiscard]] UnionLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] UnionLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    [[nodiscard]] UnionLogicalOperator withInferredSchema() const;

public:
    WeakLogicalOperator self;

private:
    static constexpr std::string_view NAME = "Union";

    std::vector<LogicalOperator> children;

    /// Set during schema inference
    std::optional<SchemaBase<UnboundFieldBase<1>, false>> outputSchema;

    TraitSet traitSet;
    friend struct std::hash<UnionLogicalOperator>;
};

static_assert(LogicalOperatorConcept<UnionLogicalOperator>);

}
template <>
struct std::hash<NES::UnionLogicalOperator>
{
    uint64_t operator()(const NES::UnionLogicalOperator& op) const noexcept;
};
