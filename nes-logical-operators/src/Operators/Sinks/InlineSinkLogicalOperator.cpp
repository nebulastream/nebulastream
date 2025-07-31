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

#include <Operators/Sinks/InlineSinkLogicalOperator.hpp>

#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include <Schema/Schema.hpp>
#include <DataTypes/UnboundSchema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>
#include <SerializableOperator.pb.h>
#include <Util/Hash.hpp>

namespace NES
{

InlineSinkLogicalOperator::InlineSinkLogicalOperator(
    Identifier sinkType, SchemaBase<UnboundFieldBase<1>, true> schema, std::unordered_map<Identifier, std::string> config)
    : targetSchema(std::move(schema)), sinkType(std::move(sinkType)), sinkConfig(std::move(config))
{
}

InlineSinkLogicalOperator InlineSinkLogicalOperator::withInferredSchema() const
{
    PRECONDITION(false, "Schema inference should happen on SinkLogicalOperator");
    std::unreachable();
}

Identifier InlineSinkLogicalOperator::getSinkType() const
{
    return sinkType;
}

std::unordered_map<Identifier, std::string> InlineSinkLogicalOperator::getSinkConfig() const
{
    return sinkConfig;
}

SchemaBase<UnboundFieldBase<1>, true> InlineSinkLogicalOperator::getTargetSchema() const
{
    return targetSchema;
}

bool InlineSinkLogicalOperator::operator==(const InlineSinkLogicalOperator& rhs) const
{
    return this->sinkType == rhs.sinkType && this->targetSchema == rhs.targetSchema&& this->sinkConfig == rhs.sinkConfig;
}

std::string InlineSinkLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("INLINE_SINK(opId: {}, name: {}, traitSet: {})", id, NAME, traitSet.explain(verbosity));
    }
    return fmt::format("INLINE_SINK({})", NAME);
}

std::string_view InlineSinkLogicalOperator::getName() noexcept
{
    return NAME;
}

InlineSinkLogicalOperator InlineSinkLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

TraitSet InlineSinkLogicalOperator::getTraitSet() const
{
    return traitSet;
}

InlineSinkLogicalOperator InlineSinkLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = std::move(children);
    return copy;
}

Schema InlineSinkLogicalOperator::getOutputSchema() const
{
    INVARIANT(false, "SinkLogicalOperator does not define a output schema");
    std::unreachable();
}

std::vector<LogicalOperator> InlineSinkLogicalOperator::getChildren() const
{
    return children;
}

void InlineSinkLogicalOperator::serialize(SerializableOperator&)
{
    PRECONDITION(false, "no serialize for InlineSinkLogicalOperator defined. Serialization happens with SinkLogicalOperator");
}

}

uint64_t std::hash<NES::InlineSinkLogicalOperator>::operator()(const NES::InlineSinkLogicalOperator& op) const noexcept
{
    return folly::hash::hash_combine(op.getTargetSchema(), op.getSinkType(), op.getSinkConfig());
}
