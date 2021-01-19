/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/RenameStreamOperatorNode.hpp>
#include <Optimizer/Utils/QuerySignatureUtil.hpp>
#include <Util/Logger.hpp>

namespace NES {

RenameStreamOperatorNode::RenameStreamOperatorNode(const std::string newStreamName, uint64_t id)
    : newStreamName(newStreamName), UnaryOperatorNode(id) {}

bool RenameStreamOperatorNode::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<RenameStreamOperatorNode>()->getId() == id;
}

bool RenameStreamOperatorNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<RenameStreamOperatorNode>()) {
        auto otherRename = rhs->as<RenameStreamOperatorNode>();
        return newStreamName == otherRename->newStreamName;
    }
    return false;
};

const std::string RenameStreamOperatorNode::toString() const {
    std::stringstream ss;
    ss << "RENAME(" << id << ", newStreamName=" << newStreamName << ")";
    return ss.str();
}

std::string RenameStreamOperatorNode::getStringBasedSignature() {
    return children[0]->as<LogicalOperatorNode>()->getStringBasedSignature();
}

bool RenameStreamOperatorNode::inferSchema() {
    UnaryOperatorNode::inferSchema();
    //TODO: add magic here
    //replace OldStreamName.Att1 to NewStreamName.Att1

    outputSchema->qualifyingName = newStreamName + Schema::ATTRIBUTE_NAME_SEPARATOR;
    for (auto& field : outputSchema->fields) {
        auto& fieldName = field->name;
        unsigned long separatorLocation = fieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR);
        if (separatorLocation != std::string::npos) {
            fieldName = fieldName.substr(separatorLocation + 1, fieldName.length());
        }
        field->name = outputSchema->qualifyingName + fieldName;
    }

    return true;
}

const std::string RenameStreamOperatorNode::getNewStreamName() { return newStreamName; }

OperatorNodePtr RenameStreamOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createRenameStreamOperator(newStreamName, id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}
}// namespace NES
