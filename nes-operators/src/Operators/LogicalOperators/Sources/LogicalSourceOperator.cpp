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
#include <Operators/LogicalOperators/Sources/LogicalSourceOperator.hpp>
#include <sstream>
#include <utility>

namespace NES {

LogicalSourceOperator::LogicalSourceOperator(SourceDescriptorPtr const& sourceDescriptor, OperatorId id)
    : OperatorNode(id), LogicalUnaryOperator(id), OriginIdAssignmentOperator(id), sourceDescriptor(sourceDescriptor) {}

LogicalSourceOperator::LogicalSourceOperator(SourceDescriptorPtr const& sourceDescriptor,
                                                     OperatorId id,
                                                     OriginId originId)
    : OperatorNode(id), LogicalUnaryOperator(id), OriginIdAssignmentOperator(id, originId),
      sourceDescriptor(sourceDescriptor) {}

bool LogicalSourceOperator::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<LogicalSourceOperator>()->getId() == id;
}

bool LogicalSourceOperator::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<LogicalSourceOperator>()) {
        auto sourceOperator = rhs->as<LogicalSourceOperator>();
        return sourceOperator->getSourceDescriptor()->equal(sourceDescriptor);
    }
    return false;
}

std::string LogicalSourceOperator::toString() const {
    std::stringstream ss;
    ss << "SOURCE(" << id << "," << sourceDescriptor->getLogicalSourceName() << "," << sourceDescriptor->toString() << ")";
    return ss.str();
}

SourceDescriptorPtr LogicalSourceOperator::getSourceDescriptor() const { return sourceDescriptor; }

bool LogicalSourceOperator::inferSchema() {
    inputSchema = sourceDescriptor->getSchema();
    outputSchema = sourceDescriptor->getSchema();
    return true;
}

void LogicalSourceOperator::setSourceDescriptor(SourceDescriptorPtr sourceDescriptor) {
    this->sourceDescriptor = std::move(sourceDescriptor);
}

void LogicalSourceOperator::setProjectSchema(SchemaPtr schema) { projectSchema = std::move(schema); }

OperatorNodePtr LogicalSourceOperator::copy() {
    auto copy = LogicalOperatorFactory::createSourceOperator(sourceDescriptor, id, originId);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setZ3Signature(z3Signature);
    if (copy->instanceOf<LogicalSourceOperator>()) {
        copy->as<LogicalSourceOperator>()->setProjectSchema(projectSchema);
    }
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

void LogicalSourceOperator::inferStringSignature() {
    //Update the signature
    auto hashCode = hashGenerator("SOURCE(" + sourceDescriptor->getLogicalSourceName() + ")");
    hashBasedSignature[hashCode] = {"SOURCE(" + sourceDescriptor->getLogicalSourceName() + ")"};
}

void LogicalSourceOperator::inferInputOrigins() {
    // Data sources have no input origins.
}

const std::vector<OriginId> LogicalSourceOperator::getOutputOriginIds() const {
    return OriginIdAssignmentOperator::getOutputOriginIds();
}

}// namespace NES
