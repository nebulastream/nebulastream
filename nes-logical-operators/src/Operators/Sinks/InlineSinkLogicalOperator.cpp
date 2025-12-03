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

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
InlineSinkLogicalOperator InlineSinkLogicalOperator::withInferredSchema(const std::vector<Schema>&) const
{
    PRECONDITION(false, "Schema inference should happen on SinkLogicalOperator");
    return *this;
}

std::string InlineSinkLogicalOperator::getSinkType() const
{
    return sinkType;
}

std::unordered_map<std::string, std::string> InlineSinkLogicalOperator::getSinkConfig() const
{
    return sinkConfig;
}

Schema InlineSinkLogicalOperator::getSchema() const
{
    return schema;
}

bool InlineSinkLogicalOperator::operator==(const InlineSinkLogicalOperator& rhs) const
{
    return this->sinkType == rhs.sinkType && this->schema == rhs.schema && this->sinkConfig == rhs.sinkConfig;
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

std::vector<Schema> InlineSinkLogicalOperator::getInputSchemas() const
{
    return {schema};
};

Schema InlineSinkLogicalOperator::getOutputSchema() const
{
    return schema;
}

std::vector<LogicalOperator> InlineSinkLogicalOperator::getChildren() const
{
    return children;
}

InlineSinkLogicalOperator::InlineSinkLogicalOperator(
    std::string type, const Schema& schema, std::unordered_map<std::string, std::string> config)
    : schema(schema), sinkType(std::move(type)), sinkConfig(std::move(config))
{
}

Reflected Reflector<InlineSinkLogicalOperator>::operator()(const InlineSinkLogicalOperator&) const
{
    PRECONDITION(false, "no serialize for InlineSinkLogicalOperator defined. Serialization happens with SinkLogicalOperator");
    throw CannotSerialize("no serialize for InlineSinkLogicalOperator defined. Serialization happens with SinkLogicalOperator");
}

InlineSinkLogicalOperator Unreflector<InlineSinkLogicalOperator>::operator()(const Reflected&) const
{
    PRECONDITION(false, "no serialize for InlineSinkLogicalOperator defined. Serialization happens with SinkLogicalOperator");
    throw CannotDeserialize("no serialize for InlineSinkLogicalOperator defined. Serialization happens with SinkLogicalOperator");
}

}
