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

#include <utility>
#include <Operators/LogicalOperators/LogicalBatchJoinDescriptor.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Join::Experimental
{

LogicalBatchJoinDescriptor::LogicalBatchJoinDescriptor(
    NodeFunctionFieldAccessPtr keyTypeBuild,
    NodeFunctionFieldAccessPtr keyTypeProbe,
    uint64_t numberOfInputEdgesLeft,
    uint64_t numberOfInputEdgesRight)
    : keyTypeBuild(std::move(keyTypeBuild))
    , keyTypeProbe(std::move(keyTypeProbe))
    , numberOfInputEdgesBuild(numberOfInputEdgesLeft)
    , numberOfInputEdgesProbe(numberOfInputEdgesRight)
{
    PRECONDITION(this->keyTypeBuild, "Invalid build join key type. key type is nullptr.");
    PRECONDITION(this->keyTypeProbe, "Invalid probe join key type. key type is nullptr.");

    PRECONDITION(this->numberOfInputEdgesBuild > 0, "Number of build edges was 0, which is not valid.");
    PRECONDITION(this->numberOfInputEdgesProbe > 0, "Number of probe edges was 0, which is not valid.");
}

LogicalBatchJoinDescriptorPtr LogicalBatchJoinDescriptor::create(
    const NodeFunctionFieldAccessPtr& keyTypeBuild,
    const NodeFunctionFieldAccessPtr& keyTypeProbe,
    uint64_t numberOfInputEdgesLeft,
    uint64_t numberOfInputEdgesRight)
{
    return std::make_shared<Join::Experimental::LogicalBatchJoinDescriptor>(
        keyTypeBuild, keyTypeProbe, numberOfInputEdgesLeft, numberOfInputEdgesRight);
}

NodeFunctionFieldAccessPtr LogicalBatchJoinDescriptor::getBuildJoinKey() const
{
    return keyTypeBuild;
}

NodeFunctionFieldAccessPtr LogicalBatchJoinDescriptor::getProbeJoinKey() const
{
    return keyTypeProbe;
}

SchemaPtr LogicalBatchJoinDescriptor::getBuildSchema() const
{
    return buildSchema;
}

SchemaPtr LogicalBatchJoinDescriptor::getProbeSchema() const
{
    return probeSchema;
}

uint64_t LogicalBatchJoinDescriptor::getNumberOfInputEdgesBuild() const
{
    return numberOfInputEdgesBuild;
}

uint64_t LogicalBatchJoinDescriptor::getNumberOfInputEdgesProbe() const
{
    return numberOfInputEdgesProbe;
}

void LogicalBatchJoinDescriptor::updateInputSchemas(SchemaPtr buildSchema, SchemaPtr probeSchema)
{
    this->buildSchema = std::move(buildSchema);
    this->probeSchema = std::move(probeSchema);
}

void LogicalBatchJoinDescriptor::updateOutputDefinition(SchemaPtr outputSchema)
{
    this->outputSchema = std::move(outputSchema);
}

SchemaPtr LogicalBatchJoinDescriptor::getOutputSchema() const
{
    return outputSchema;
}
void LogicalBatchJoinDescriptor::setNumberOfInputEdgesBuild(uint64_t numberOfInputEdgesLeft)
{
    LogicalBatchJoinDescriptor::numberOfInputEdgesBuild = numberOfInputEdgesLeft;
}
void LogicalBatchJoinDescriptor::setNumberOfInputEdgesProbe(uint64_t numberOfInputEdgesRight)
{
    LogicalBatchJoinDescriptor::numberOfInputEdgesProbe = numberOfInputEdgesRight;
}

}; /// namespace NES::Join::Experimental
