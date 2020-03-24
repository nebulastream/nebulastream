#ifndef NES_INCLUDE_NODES_EXPRESSIONS_EXPRESSION_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_EXPRESSION_HPP_
#include <memory>
#include <Nodes/Node.hpp>
#include <API/Types/DataTypes.hpp>
namespace NES {
/**
 * @brief this indicates an expression, which is a parameter for a FilterOperator or a MapOperator.
 * Each expression declares a stamp, which expresses the data type of this expression.
 * A stamp can be of a concrete type or invalid if the data type was not yet inferred.
 */
class ExpressionNode : public Node {
  public:
    ExpressionNode(DataTypePtr stamp);
    ~ExpressionNode() = default;
    bool isPredicate();
  protected:
    /**
     * @brief declares the type of this expression.
     */
    DataTypePtr stamp;
};
typedef std::shared_ptr<ExpressionNode> ExpressionNodePtr;
}
#endif //NES_INCLUDE_NODES_EXPRESSIONS_EXPRESSION_HPP_
