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

    ExpressionItem& operator=(ExpressionNodePtr c);
    ExpressionItem& operator=(ExpressionItem c);
    /**
     * @brief Creates a constant expression node for this constant value.
     */
    ExpressionNodePtr createExpressionNode();

  private:
    ExpressionNodePtr expression;
};


ExpressionItem Field(std::string name);
ExpressionItem Field(std::string name, BasicType type);

/**
 * @brief A wrapper to allow the use of simple constants in the query api, e.g, field < 10
 */
class Constant : public ExpressionItem {
  public:

    /**
     * @brief Creates a constant expression node for this constant value.
     */
    ExpressionNodePtr createExpressionNode() override;
  private:
    /**
     * @brief the constant value.
     */
    const ValueTypePtr value;
};

/**
 * @brief Field(name) allows the user to reference a field in his expression.
 * Field("f1") < 10
 * @param fieldName
 */
//ExpressionNodePtr Field(std::string fieldName);


/**
 * @brief Field(name, type) allows the user to reference a field, with a specific type in his expression.
 * Field("f1", Int) < 10.
 * todo if we can infer the type at runtime from the operator tree.
 * @param fieldName, type
 */
//ExpressionNodePtr Field(std::string fieldName, BasicType type);

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

} //end of namespace NES
#endif 
