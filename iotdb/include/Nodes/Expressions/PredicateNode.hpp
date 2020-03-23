#ifndef NES_INCLUDE_NODES_EXPRESSIONS_PREDICATE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_PREDICATE_HPP_
#include <Nodes/Expressions/BinaryExpressionNode.hpp>
#include <Nodes/Expressions/BinaryOperationType.hpp>
#include <string>
namespace NES {
class PredicateNode : public BinaryExpressionNode {
  public:
    PredicateNode(const BinaryOperationType comparisionType, const ExpressionNodePtr left, const ExpressionNodePtr right);
    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
};
typedef std::shared_ptr<PredicateNode> PredicateNodePtr;
PredicateNodePtr createPredicateNode(const BinaryOperationType comparisionType,
                                     const ExpressionNodePtr left,
                                     const ExpressionNodePtr right);
}

#endif //NES_INCLUDE_NODES_EXPRESSIONS_PREDICATE_HPP_
