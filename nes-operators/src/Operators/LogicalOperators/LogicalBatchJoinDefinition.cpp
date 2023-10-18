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
#include <Operators/LogicalOperators/LogicalBatchJoinDefinition.hpp>
#include <utility>

namespace NES::Join::Experimental {

LogicalBatchJoinDefinition::LogicalBatchJoinDefinition(FieldAccessExpressionNodePtr keyTypeBuild,
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

LogicalBatchJoinDefinitionPtr LogicalBatchJoinDefinition::create(const FieldAccessExpressionNodePtr& keyTypeBuild,
                                                                 const FieldAccessExpressionNodePtr& keyTypeProbe,
                                                                 uint64_t numberOfInputEdgesLeft,
                                                                 uint64_t numberOfInputEdgesRight) {
    return std::make_shared<Join::Experimental::LogicalBatchJoinDefinition>(keyTypeBuild,
                                                                            keyTypeProbe,
                                                                            numberOfInputEdgesLeft,
                                                                            numberOfInputEdgesRight);
}

FieldAccessExpressionNodePtr LogicalBatchJoinDefinition::getBuildJoinKey() { return keyTypeBuild; }

FieldAccessExpressionNodePtr LogicalBatchJoinDefinition::getProbeJoinKey() { return keyTypeProbe; }

SchemaPtr LogicalBatchJoinDefinition::getBuildSchema() { return buildSchema; }

SchemaPtr LogicalBatchJoinDefinition::getProbeSchema() { return probeSchema; }

uint64_t LogicalBatchJoinDefinition::getNumberOfInputEdgesBuild() const { return numberOfInputEdgesBuild; }

uint64_t LogicalBatchJoinDefinition::getNumberOfInputEdgesProbe() const { return numberOfInputEdgesProbe; }

void LogicalBatchJoinDefinition::updateInputSchemas(SchemaPtr buildSchema, SchemaPtr probeSchema) {
    this->buildSchema = std::move(buildSchema);
    this->probeSchema = std::move(probeSchema);
}

void LogicalBatchJoinDefinition::updateOutputDefinition(SchemaPtr outputSchema) { this->outputSchema = std::move(outputSchema); }

SchemaPtr LogicalBatchJoinDefinition::getOutputSchema() const { return outputSchema; }
void LogicalBatchJoinDefinition::setNumberOfInputEdgesBuild(uint64_t numberOfInputEdgesLeft) {
    LogicalBatchJoinDefinition::numberOfInputEdgesBuild = numberOfInputEdgesLeft;
}
void LogicalBatchJoinDefinition::setNumberOfInputEdgesProbe(uint64_t numberOfInputEdgesRight) {
    LogicalBatchJoinDefinition::numberOfInputEdgesProbe = numberOfInputEdgesRight;
}

};// namespace NES::Join::Experimental
