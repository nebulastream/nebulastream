#include <Nodes/Expressions/PredicateNode.hpp>
#include <boost/iostreams/detail/iostream.hpp>
namespace NES {
PredicateNode::PredicateNode(const BinaryOperationType comparisionType,
                             const ExpressionNodePtr left,
                             const ExpressionNodePtr right) : BinaryExpressionNode(createDataType(BOOLEAN),
                                                                                   comparisionType,
                                                                                   left,
                                                                                   right) {

}

bool PredicateNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<PredicateNode>()) {
        auto otherPredicateNode = rhs->as<PredicateNode>();
        return otherPredicateNode->operationType == operationType &&
            getLeft()->equal(otherPredicateNode->getLeft()) &&
            getRight()->equal(otherPredicateNode->getRight());
    }
    return false;
}

const std::string PredicateNode::toString() const {
    std::stringstream ss;
    ss << "PREDICATE()";
    return ss.str();
}

PredicateNodePtr createPredicateNode(const BinaryOperationType predicateType,
                                     const ExpressionNodePtr left,
                                     const ExpressionNodePtr right) {
    return std::make_shared<PredicateNode>(predicateType, left, right);
}
}