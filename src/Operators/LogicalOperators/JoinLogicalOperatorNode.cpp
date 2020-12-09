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
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Optimizer/Utils/OperatorToQuerySignatureUtil.hpp>
#include <Util/Logger.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <z3++.h>

namespace NES {

JoinLogicalOperatorNode::JoinLogicalOperatorNode(Join::LogicalJoinDefinitionPtr joinDefinition, OperatorId id)
    : joinDefinition(joinDefinition), LogicalOperatorNode(id) {}

bool JoinLogicalOperatorNode::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<JoinLogicalOperatorNode>()->getId() == id;
}

const std::string JoinLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "Join(" << id << ")";
    return ss.str();
}

Join::LogicalJoinDefinitionPtr JoinLogicalOperatorNode::getJoinDefinition() { return joinDefinition; }

bool JoinLogicalOperatorNode::inferSchema() {
    OperatorNode::inferSchema();
    if (getChildren().size() != 2) {
        NES_THROW_RUNTIME_ERROR("JoinLogicalOperator: Join need two child operators.");
    }
    auto child1 = getChildren()[0]->as<LogicalOperatorNode>();
    auto child2 = getChildren()[1]->as<LogicalOperatorNode>();

    auto schema1 = child1->getInputSchema();
    auto schema2 = child2->getInputSchema();

    if (!schema1->equals(schema2)) {
        NES_THROW_RUNTIME_ERROR("JoinLogicalOperator: the two input streams have different schema.");
    }

    // infer the data type of the key field.
    joinDefinition->getJoinKey()->inferStamp(inputSchema);

    outputSchema = Schema::create()
                       ->addField(createField("start", UINT64))
                       ->addField(createField("end", UINT64))
                       ->addField(AttributeField::create("key", joinDefinition->getJoinKey()->getStamp()))
                       ->addField(AttributeField::create("value", joinDefinition->getJoinKey()->getStamp()));

    return true;
}

OperatorNodePtr JoinLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createJoinOperator(joinDefinition, id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

bool JoinLogicalOperatorNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<JoinLogicalOperatorNode>()) {
        return true;
    }
    return false;
}

}// namespace NES