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

#include <Distributed/BridgeLogicalOperator.hpp>

#include <stdexcept>
#include <vector>

#include <ErrorHandling.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/Trait.hpp>

namespace NES {

bool BridgeLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    return getOutputSchema() == rhs.getOutputSchema() && getInputSchemas() == rhs.getInputSchemas()
        && getInputOriginIds() == rhs.getInputOriginIds() && getOutputOriginIds() == rhs.getOutputOriginIds();
}

LogicalOperator BridgeLogicalOperator::withInferredSchema(std::vector<Schema> /*inputSchemas*/) const
{
    throw std::logic_error("Method should not be called");
}

LogicalOperator BridgeLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = traitSet;
    return copy;
}

TraitSet BridgeLogicalOperator::getTraitSet() const
{
    return traitSet;
}

LogicalOperator BridgeLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> BridgeLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema BridgeLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

void BridgeLogicalOperator::setOutputSchema(Schema schema)
{
    outputSchema = schema;
}

std::vector<std::vector<OriginId>> BridgeLogicalOperator::getInputOriginIds() const
{
    return {inputOriginIds};
}

std::vector<OriginId> BridgeLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

LogicalOperator BridgeLogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    PRECONDITION(ids.size() == 1, "Bridge should have one input");
    auto copy = *this;
    copy.inputOriginIds = ids[0];
    return copy;
}

LogicalOperator BridgeLogicalOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}

std::vector<LogicalOperator> BridgeLogicalOperator::getChildren() const
{
    return children;
}

SerializableOperator BridgeLogicalOperator::serialize() const
{
    throw std::logic_error("BridgeLogicalOperator should be part of the global query plan and should not be serialized");
}

}
