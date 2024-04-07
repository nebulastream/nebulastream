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

#include <Util/Logger/Logger.hpp>
#include <Operators/LogicalOperators/LogicalBatchJoinDescriptor.hpp>
#include <utility>

namespace NES::Join::Experimental {

LogicalBatchJoinDescriptor::LogicalBatchJoinDescriptor(FieldAccessExpressionNodePtr keyTypeBuild,
                                                       FieldAccessExpressionNodePtr keyTypeProbe,
                                                       uint64_t numberOfInputEdgesLeft,
                                                       uint64_t numberOfInputEdgesRight)
    : keyTypeBuild(std::move(keyTypeBuild)), keyTypeProbe(std::move(keyTypeProbe)),
      numberOfInputEdgesBuild(numberOfInputEdgesLeft), numberOfInputEdgesProbe(numberOfInputEdgesRight) {

    NES_ASSERT(this->keyTypeBuild, "Invalid left join key type");
    NES_ASSERT(this->keyTypeProbe, "Invalid right join key type");

    NES_ASSERT(this->numberOfInputEdgesBuild > 0, "Invalid number of left edges");
    NES_ASSERT(this->numberOfInputEdgesProbe > 0, "Invalid number of right edges");
}

LogicalBatchJoinDescriptorPtr LogicalBatchJoinDescriptor::create(const FieldAccessExpressionNodePtr& keyTypeBuild,
                                                                 const FieldAccessExpressionNodePtr& keyTypeProbe,
                                                                 uint64_t numberOfInputEdgesLeft,
                                                                 uint64_t numberOfInputEdgesRight) {
    return std::make_shared<Join::Experimental::LogicalBatchJoinDescriptor>(keyTypeBuild,
                                                                            keyTypeProbe,
                                                                            numberOfInputEdgesLeft,
                                                                            numberOfInputEdgesRight);
}

FieldAccessExpressionNodePtr LogicalBatchJoinDescriptor::getBuildJoinKey() { return keyTypeBuild; }

FieldAccessExpressionNodePtr LogicalBatchJoinDescriptor::getProbeJoinKey() { return keyTypeProbe; }

SchemaPtr LogicalBatchJoinDescriptor::getBuildSchema() { return buildSchema; }

SchemaPtr LogicalBatchJoinDescriptor::getProbeSchema() { return probeSchema; }

uint64_t LogicalBatchJoinDescriptor::getNumberOfInputEdgesBuild() const { return numberOfInputEdgesBuild; }

uint64_t LogicalBatchJoinDescriptor::getNumberOfInputEdgesProbe() const { return numberOfInputEdgesProbe; }

void LogicalBatchJoinDescriptor::updateInputSchemas(SchemaPtr buildSchema, SchemaPtr probeSchema) {
    this->buildSchema = std::move(buildSchema);
    this->probeSchema = std::move(probeSchema);
}

void LogicalBatchJoinDescriptor::updateOutputDefinition(SchemaPtr outputSchema) { this->outputSchema = std::move(outputSchema); }

SchemaPtr LogicalBatchJoinDescriptor::getOutputSchema() const { return outputSchema; }
void LogicalBatchJoinDescriptor::setNumberOfInputEdgesBuild(uint64_t numberOfInputEdgesLeft) {
    LogicalBatchJoinDescriptor::numberOfInputEdgesBuild = numberOfInputEdgesLeft;
}
void LogicalBatchJoinDescriptor::setNumberOfInputEdgesProbe(uint64_t numberOfInputEdgesRight) {
    LogicalBatchJoinDescriptor::numberOfInputEdgesProbe = numberOfInputEdgesRight;
}

};// namespace NES::Join::Experimental
