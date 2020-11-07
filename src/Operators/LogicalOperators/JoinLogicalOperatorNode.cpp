#include <API/Schema.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Optimizer/Utils/OperatorToZ3ExprUtil.hpp>
#include <Util/Logger.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <z3++.h>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>

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

Join::LogicalJoinDefinitionPtr JoinLogicalOperatorNode::getJoinDefinition() {
    return joinDefinition;
}

bool JoinLogicalOperatorNode::inferSchema() {
    OperatorNode::inferSchema();
    if (getChildren().size() != 2) {
        NES_THROW_RUNTIME_ERROR("JoinLogicalOperator: Join need two child operators.");
        return false;
    }
    auto child1 = getChildren()[0]->as<LogicalOperatorNode>();
    auto child2 = getChildren()[1]->as<LogicalOperatorNode>();

    auto schema1 = child1->getInputSchema();
    auto schema2 = child2->getInputSchema();

    if (!schema1->equals(schema2)) {
        NES_THROW_RUNTIME_ERROR("JoinLogicalOperator: the two input streams have different schema.");
        return false;
    }
    // infer the data type of the key field.
    joinDefinition->getJoinKey()->inferStamp(inputSchema);

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

z3::expr JoinLogicalOperatorNode::getZ3Expression(z3::context& context) {
    OperatorNodePtr operatorNode = shared_from_this()->as<OperatorNode>();
    return OperatorToZ3ExprUtil::createForOperator(operatorNode, context);
}

}// namespace NES