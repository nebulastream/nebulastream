
#include <DataTypes/DataType.hpp>
#include <Nodes/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
namespace NES {

NegateExpressionNode::NegateExpressionNode() : LogicalUnaryExpressionNode() {}

bool NegateExpressionNode::equal(const NodePtr rhs) const {
    if (this->isIdentical(rhs)) {
        return true;
    }
    return rhs->instanceOf<NegateExpressionNode>();
}

const std::string NegateExpressionNode::toString() const {
    return "NegateNode(" + stamp->toString() + ")";
}

ExpressionNodePtr NegateExpressionNode::create(const ExpressionNodePtr child) {
    auto equals = std::make_shared<NegateExpressionNode>();
    equals->setChild(child);
    return equals;
}

void NegateExpressionNode::inferStamp(SchemaPtr schema) {
    // delegate stamp inference of children
    ExpressionNode::inferStamp(schema);
    // check if children stamp is correct
    if (!child()->isPredicate()) {
        NES_THROW_RUNTIME_ERROR(
            "Negate Expression Node: the stamp of child must be boolean, but was: " + child()->getStamp()->toString());
    }
}

}// namespace NES