/*
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

#ifndef NES_CLIENT_INCLUDE_API_EXPRESSIONS_EXPRESSIONS_HPP_
#define NES_CLIENT_INCLUDE_API_EXPRESSIONS_EXPRESSIONS_HPP_

#include <Common/DataTypes/BasicTypes.hpp>
#include <memory>
#include <string>
#include <vector>

namespace NES {

/**
 * @brief This file contains the user facing api to create expression nodes in a fluent and easy way.
 */

class ExpressionNode;
using ExpressionNodePtr = std::shared_ptr<ExpressionNode>;

class ValueType;
using ValueTypePtr = std::shared_ptr<ValueType>;

class FieldAssignmentExpressionNode;
using FieldAssignmentExpressionNodePtr = std::shared_ptr<FieldAssignmentExpressionNode>;

/**
 * @brief A expression item represents the leaf in an expression tree.
 * It is converted to an constant value expression or a field access expression.
 */
class ExpressionItem {
  public:
    ExpressionItem(int8_t value);            //NOLINT(google-explicit-constructor)
    ExpressionItem(uint8_t value);           //NOLINT(google-explicit-constructor)
    ExpressionItem(int16_t value);           //NOLINT(google-explicit-constructor)
    ExpressionItem(uint16_t value);          //NOLINT(google-explicit-constructor)
    ExpressionItem(int32_t value);           //NOLINT(google-explicit-constructor)
    ExpressionItem(uint32_t value);          //NOLINT(google-explicit-constructor)
    ExpressionItem(int64_t value);           //NOLINT(google-explicit-constructor)
    ExpressionItem(uint64_t value);          //NOLINT(google-explicit-constructor)
    ExpressionItem(float value);             //NOLINT(google-explicit-constructor)
    ExpressionItem(double value);            //NOLINT(google-explicit-constructor)
    ExpressionItem(bool value);              //NOLINT(google-explicit-constructor)
    ExpressionItem(const char* value);       //NOLINT(google-explicit-constructor)
    ExpressionItem(std::string const& value);//NOLINT(google-explicit-constructor)
    ExpressionItem(ValueTypePtr value);      //NOLINT(google-explicit-constructor)
    ExpressionItem(ExpressionNodePtr exp);

    ExpressionItem(ExpressionItem const&) = default;
    ExpressionItem(ExpressionItem&&) = default;

    FieldAssignmentExpressionNodePtr operator=(ExpressionItem);
    FieldAssignmentExpressionNodePtr operator=(ExpressionNodePtr);

    /**
     * @brief Gets the expression node of this expression item.
     */
    [[nodiscard]] ExpressionNodePtr getExpressionNode() const;
    operator ExpressionNodePtr();

    /**
     * @brief Rename the expression item
     * @param name : the new name
     * @return the updated expression item
     */
    ExpressionItem as(std::string name);

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
 * todo remove this case if we added type inference at Runtime from the operator tree.
 * todo rename to field if conflict with legacy code is resolved.
 * @param fieldName, type
 */
ExpressionItem Attribute(std::string name, BasicType type);

/**
 * @brief WHEN(condition,value) can only be used as part of the left vector in a CASE() expression.
 * Allows to only return the value expression if condition is met.
 * @param conditionExp : a logical condition which will be evaluated.
 * @param valueExp : the value to return if the condition is the first true one.
 */
ExpressionNodePtr WHEN(const ExpressionNodePtr& conditionExp, const ExpressionNodePtr& valueExp);
ExpressionNodePtr WHEN(ExpressionItem conditionExp, ExpressionNodePtr valueExp);
ExpressionNodePtr WHEN(ExpressionNodePtr conditionExp, ExpressionItem valueExp);
ExpressionNodePtr WHEN(ExpressionItem conditionExp, ExpressionItem valueExp);

/**
 * @brief CASE({WHEN(condition,value),WHEN(condition, value)} , value) allows to evaluate all
 * WHEN expressions from the vector list and only return the first one where the condition evaluated to true, or the value of the default expression.
 * The CASE({WHEN()},default) is evaluated as a concatenated ternary operator in C++.
 * @param whenExpressions : a vector of at least one WHEN expression to evaluate.
 * @param defaultValueExp : an expression which will be returned if no WHEN condition evaluated to true.
 */
ExpressionNodePtr CASE(const std::vector<ExpressionNodePtr>& whenExpressions, ExpressionNodePtr defaultValueExp);
ExpressionNodePtr CASE(std::vector<ExpressionNodePtr> whenExpressions, ExpressionItem defaultValueExp);

}//end of namespace NES
#endif  // NES_CLIENT_INCLUDE_API_EXPRESSIONS_EXPRESSIONS_HPP_
