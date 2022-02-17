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
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/RenameStreamOperatorNode.hpp>
#include <Optimizer/QuerySignatures/QuerySignatureUtil.hpp>
#include <Util/Logger.hpp>

namespace NES {

RenameStreamOperatorNode::RenameStreamOperatorNode(const std::string& newStreamName, uint64_t id)
    : OperatorNode(id), LogicalUnaryOperatorNode(id), newStreamName(newStreamName) {}

bool RenameStreamOperatorNode::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<RenameStreamOperatorNode>()->getId() == id;
}

bool RenameStreamOperatorNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<RenameStreamOperatorNode>()) {
        auto otherRename = rhs->as<RenameStreamOperatorNode>();
        return newStreamName == otherRename->newStreamName;
    }
    return false;
};

std::string RenameStreamOperatorNode::toString() const {
    std::stringstream ss;
    ss << "RENAME_STREAM(" << id << ", newStreamName=" << newStreamName << ")";
    return ss.str();
}

bool RenameStreamOperatorNode::inferSchema() {
    if (!LogicalUnaryOperatorNode::inferSchema()) {
        return false;
    }
    //Update output schema by changing the qualifier and corresponding attribute names
    auto newQualifierName = newStreamName + Schema::ATTRIBUTE_NAME_SEPARATOR;
    for (auto& field : outputSchema->fields) {
        //Extract field name without qualifier
        auto fieldName = field->getName();
        //Add new qualifier name to the field and update the field name
        field->setName(newQualifierName + fieldName);
    }
    return true;
}

std::string RenameStreamOperatorNode::getNewStreamName() { return newStreamName; }

OperatorNodePtr RenameStreamOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createRenameStreamOperator(newStreamName, id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setHashBasedSignature(hashBasedSignature);
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

void RenameStreamOperatorNode::inferStringSignature() {
    OperatorNodePtr operatorNode = shared_from_this()->as<OperatorNode>();
    NES_TRACE("RenameStreamOperatorNode: Inferring String signature for " << operatorNode->toString());
    NES_ASSERT(!children.empty(), "RenameStreamOperatorNode: Rename Stream should have children.");
    //Infer query signatures for child operators
    for (auto& child : children) {
        const LogicalOperatorNodePtr childOperator = child->as<LogicalOperatorNode>();
        childOperator->inferStringSignature();
    }
    std::stringstream signatureStream;
    auto childSignature = children[0]->as<LogicalOperatorNode>()->getHashBasedSignature();
    signatureStream << "RENAME_STREAM(newStreamName=" << newStreamName << ")." << *childSignature.begin()->second.begin();

    //Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
}// namespace NES
