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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Operators/LogicalOperators/LogicalRenameSourceOperator.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

LogicalRenameSourceOperator::LogicalRenameSourceOperator(const std::string& newSourceName, uint64_t id)
    : OperatorNode(id), LogicalUnaryOperator(id), newSourceName(newSourceName) {}

bool LogicalRenameSourceOperator::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<LogicalRenameSourceOperator>()->getId() == id;
}

bool LogicalRenameSourceOperator::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<LogicalRenameSourceOperator>()) {
        auto otherRename = rhs->as<LogicalRenameSourceOperator>();
        return newSourceName == otherRename->newSourceName;
    }
    return false;
};

std::string LogicalRenameSourceOperator::toString() const {
    std::stringstream ss;
    ss << "RENAME_STREAM(" << id << ", newSourceName=" << newSourceName << ")";
    return ss.str();
}

bool LogicalRenameSourceOperator::inferSchema() {
    if (!LogicalUnaryOperator::inferSchema()) {
        return false;
    }
    //Update output schema by changing the qualifier and corresponding attribute names
    auto newQualifierName = newSourceName + Schema::ATTRIBUTE_NAME_SEPARATOR;
    for (auto& field : outputSchema->fields) {
        //Extract field name without qualifier
        auto fieldName = field->getName();
        //Add new qualifier name to the field and update the field name
        field->setName(newQualifierName + fieldName);
    }
    return true;
}

std::string LogicalRenameSourceOperator::getNewSourceName() { return newSourceName; }

OperatorNodePtr LogicalRenameSourceOperator::copy() {
    auto copy = LogicalOperatorFactory::createRenameSourceOperator(newSourceName, id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setHashBasedSignature(hashBasedSignature);
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

void LogicalRenameSourceOperator::inferStringSignature() {
    OperatorNodePtr operatorNode = shared_from_this()->as<OperatorNode>();
    NES_TRACE("LogicalRenameSourceOperator: Inferring String signature for {}", operatorNode->toString());
    NES_ASSERT(!children.empty(), "LogicalRenameSourceOperator: Rename Source should have children.");
    //Infer query signatures for child operators
    for (auto& child : children) {
        const LogicalOperatorNodePtr childOperator = child->as<LogicalOperator>();
        childOperator->inferStringSignature();
    }
    std::stringstream signatureStream;
    auto childSignature = children[0]->as<LogicalOperator>()->getHashBasedSignature();
    signatureStream << "RENAME_STREAM(newStreamName=" << newSourceName << ")." << *childSignature.begin()->second.begin();

    //Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
}// namespace NES
