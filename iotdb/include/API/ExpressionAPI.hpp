#ifndef EXPRESSIONAPI_HPP
#define EXPRESSIONAPI_HPP

#include <string>
#include <memory>
#include <API/Types/DataTypes.hpp>

namespace NES {

/**
 * @brief This file contains the user facing api to create expression nodes in a fluent and easy way.
 */
class ExpressionNode;
typedef std::shared_ptr<ExpressionNode> ExpressionNodePtr;

class ValueType;
typedef std::shared_ptr<ValueType> ValueTypePtr;

/**
 * @brief A expression item represents the leaf in an expression tree.
 * It is converted to an constant value expression or a field access expression.
 */
class ExpressionItem{
  public:
    ExpressionItem(int8_t value);
    ExpressionItem(uint8_t value);
    ExpressionItem(int16_t value);
    ExpressionItem(uint16_t value);
    ExpressionItem(int32_t value);
    ExpressionItem(uint32_t value);
    ExpressionItem(int64_t value);
    ExpressionItem(uint64_t value);
    ExpressionItem(float value);
    ExpressionItem(double value);
    ExpressionItem(bool value);
    ExpressionItem(const char* value);
    ExpressionItem(ValueTypePtr value);
    ExpressionItem(ExpressionNodePtr ref);

    /**
     * @brief Gets the expression node of this expression item.
     */
    ExpressionNodePtr getExpressionNode();

  private:
    ExpressionNodePtr expression;
};

/**
 * @brief Attribute(name) allows the user to reference a field in his expression.
 * Attribute("f1") < 10
 * todo rename to field if conflict with legacy code is resolved.
 * @param fieldName
 */
ExpressionItem Attribute(std::string name);

/**
 * @brief Attribute(name, type) allows the user to reference a field, with a specific type in his expression.
 * Field("f1", Int) < 10.
 * todo remove this case if we added type inference at runtime from the operator tree.
 * todo rename to field if conflict with legacy code is resolved.
 * @param fieldName, type
 */
ExpressionItem Attribute(std::string name, BasicType type);

/**
 * @brief Defines common operations between expression nodes.
 */
ExpressionNodePtr operator&&(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator||(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator==(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator!=(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator<=(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator>=(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator<(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator>(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator!(ExpressionNodePtr exp);


ExpressionNodePtr operator-(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator--(ExpressionNodePtr exp);
ExpressionNodePtr operator+(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator++(ExpressionNodePtr exp);
ExpressionNodePtr operator*(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator/(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);

/**
 * @brief Defines common operations between a constant and an expression node.
 */
ExpressionNodePtr operator&&(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator||(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator==(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator!=(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator<=(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator>=(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator<(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator>(ExpressionItem leftExp, ExpressionNodePtr rightExp);

ExpressionNodePtr operator-(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator+(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator*(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator/(ExpressionItem leftExp, ExpressionNodePtr rightExp);

/**
 * @brief Defines common operations between an expression node and a constant.
 */
ExpressionNodePtr operator&&(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator||(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator==(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator!=(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator<=(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator>=(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator<(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator>(ExpressionNodePtr leftExp, ExpressionItem rightExp);

ExpressionNodePtr operator-(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator+(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator*(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator/(ExpressionNodePtr leftExp, ExpressionItem rightExp);

/**
 * @brief Defines common operations between two constants.
 */
ExpressionNodePtr operator&&(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator||(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator==(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator!=(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator<=(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator>=(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator<(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator>(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator!(ExpressionItem exp);

ExpressionNodePtr operator-(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator+(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator*(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator/(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator++(ExpressionItem exp);
ExpressionNodePtr operator--(ExpressionItem exp);

} //end of namespace NES
#endif 
