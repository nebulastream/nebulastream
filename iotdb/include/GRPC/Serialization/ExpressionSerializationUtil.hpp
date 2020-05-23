#ifndef NES_INCLUDE_UTIL_EXPRESSIONSERIALIZATIONUTIL_HPP_
#define NES_INCLUDE_UTIL_EXPRESSIONSERIALIZATIONUTIL_HPP_

#include <memory>

namespace NES {

class ExpressionNode;
typedef std::shared_ptr<ExpressionNode> ExpressionNodePtr;

class SerializableExpression;

class ExpressionSerializationUtil {
  public:
    static SerializableExpression* serializeExpression(ExpressionNodePtr expressionNode, SerializableExpression* serializedExpression);
    static ExpressionNodePtr deserializeExpression(SerializableExpression* serializedExpression);
    static void serializeLogicalExpressions(ExpressionNodePtr sharedPtr, SerializableExpression* pExpression);
    static void serializeArithmeticalExpressions(ExpressionNodePtr expression, SerializableExpression* serializedExpression);
    static ExpressionNodePtr deserializeLogicalExpressions(SerializableExpression* serializedExpression);
    static ExpressionNodePtr deserializeArithmeticalExpressions(SerializableExpression* serializedExpression);
};
}// namespace NES

#endif//NES_INCLUDE_UTIL_EXPRESSIONSERIALIZATIONUTIL_HPP_
