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

#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Schema/Schema.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <SerializableOperator.pb.h>
#include "Configurations/Descriptor.hpp"

namespace NES
{

class IngestionTimeWatermarkAssignerLogicalOperator
{
public:
    IngestionTimeWatermarkAssignerLogicalOperator();
    /// I do not understand why, but if we just pass the child in an explicit constructor,
    /// this class stops being convertible to itself (how is that even a thing) and thus doesn't fulfill the concept.
    IngestionTimeWatermarkAssignerLogicalOperator(LogicalOperator child, DescriptorConfig::Config);

    [[nodiscard]] bool operator==(const IngestionTimeWatermarkAssignerLogicalOperator& rhs) const;
    void serialize(SerializableOperator&) const;

    [[nodiscard]] IngestionTimeWatermarkAssignerLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] IngestionTimeWatermarkAssignerLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;

    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] IngestionTimeWatermarkAssignerLogicalOperator withInferredSchema() const;

public:
    WeakLogicalOperator self;

protected:
    static constexpr std::string_view NAME = "IngestionTimeWatermarkAssigner";

    /// TOOD make non-optional once all ctors require children
    std::optional<LogicalOperator> child;

    /// Set during schema inference
    std::optional<SchemaBase<UnboundFieldBase<1>, false>> outputSchema;

    TraitSet traitSet;

    friend struct std::hash<IngestionTimeWatermarkAssignerLogicalOperator>;
};

static_assert(LogicalOperatorConcept<IngestionTimeWatermarkAssignerLogicalOperator>);

}
template <>
struct std::hash<NES::IngestionTimeWatermarkAssignerLogicalOperator>
{
    uint64_t operator()(const NES::IngestionTimeWatermarkAssignerLogicalOperator& op) const noexcept;
};
