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

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <sstream>
#include <utility>

namespace NES {

SourceLogicalOperator::SourceLogicalOperator(SourceDescriptorPtr const& sourceDescriptor, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), OriginIdAssignmentOperator(id), sourceDescriptor(sourceDescriptor) {}

SourceLogicalOperator::SourceLogicalOperator(SourceDescriptorPtr const& sourceDescriptor,
                                                     OperatorId id,
                                                     OriginId originId)
    : Operator(id), LogicalUnaryOperator(id), OriginIdAssignmentOperator(id, originId),
      sourceDescriptor(sourceDescriptor) {}

bool SourceLogicalOperator::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<SourceLogicalOperator>()->getId() == id;
}

bool SourceLogicalOperator::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<SourceLogicalOperator>()) {
        auto sourceOperator = rhs->as<SourceLogicalOperator>();
        return sourceOperator->getSourceDescriptor()->equal(sourceDescriptor);
    }
    return false;
}

std::string SourceLogicalOperator::toString() const {
    std::stringstream ss;
    ss << "SOURCE(opId: " << id << ", statisticId: " << statisticId << ","
       << sourceDescriptor->getLogicalSourceName() << "," << sourceDescriptor->toString() << ")";
    return ss.str();
}

SourceDescriptorPtr SourceLogicalOperator::getSourceDescriptor() const { return sourceDescriptor; }

bool SourceLogicalOperator::inferSchema() {
    inputSchema = sourceDescriptor->getSchema();
    outputSchema = sourceDescriptor->getSchema();
    return true;
}

void SourceLogicalOperator::setSourceDescriptor(SourceDescriptorPtr sourceDescriptor) {
    this->sourceDescriptor = std::move(sourceDescriptor);
}

void SourceLogicalOperator::setProjectSchema(SchemaPtr schema) { projectSchema = std::move(schema); }

OperatorPtr SourceLogicalOperator::copy() {
    auto copy = LogicalOperatorFactory::createSourceOperator(sourceDescriptor, id, originId);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setZ3Signature(z3Signature);
    copy->setOperatorState(operatorState);
    copy->setStatisticId(statisticId);
    if (copy->instanceOf<SourceLogicalOperator>()) {
        copy->as<SourceLogicalOperator>()->setProjectSchema(projectSchema);
    }
    for (const auto& pair : properties) {
        copy->addProperty(pair.first, pair.second);
    }
    return copy;
}

void SourceLogicalOperator::inferStringSignature() {
    //Update the signature
    auto hashCode = hashGenerator("SOURCE(" + sourceDescriptor->getLogicalSourceName() + ")");
    hashBasedSignature[hashCode] = {"SOURCE(" + sourceDescriptor->getLogicalSourceName() + ")"};
}

void SourceLogicalOperator::inferInputOrigins() {
    // Data sources have no input origins.
}

std::vector<OriginId> SourceLogicalOperator::getOutputOriginIds() const {
    return OriginIdAssignmentOperator::getOutputOriginIds();
}

}// namespace NES
