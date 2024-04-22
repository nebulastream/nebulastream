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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_EXPRESSIONS_FUNCTIONS_FUNCTIONEXPRESSIONNODE_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_EXPRESSIONS_FUNCTIONS_FUNCTIONEXPRESSIONNODE_HPP_
#include <Operators/Expressions/ExpressionNode.hpp>
namespace NES {

class LogicalFunction;
class ValueType;
using ValueTypePtr = std::shared_ptr<ValueType>;

/**
 * @brief This expression node represents a function with a specific name.
 * Internally it stores a LogicalFunction, which is used for inference.
 */
class FunctionExpression final : public ExpressionNode {
  public:
    /**
     * @brief Factory method to create a ConstantValueExpressionNode.
     */
    static ExpressionNodePtr
    create(const DataTypePtr& stamp, const std::string& functionName, const std::vector<ExpressionNodePtr>& arguments);

    /**
     * @brief On a function value expression infer stamp invokes inferStamp on the child LogicalFunction
     * @param typeInferencePhaseContext TypeInferencePhaseContext
     * @param schema current logical schema
     */
    void inferStamp(SchemaPtr schema) override;

    /**
     * @brief Creates a string representation of the function
     * @return
     */
    std::string toString() const override;

    /**
     * @brief Compares if another node is equal to this function
     * @param otherNode
     * @return true if they are equal
     */
    bool equal(NodePtr const& rhs) const override;

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override;

    /**
     * @brief Returns the function name of this function
     * @return const std::string&
     */
    const std::string& getFunctionName() const;

    /**
     * @brief Returns the set of arguments of this function.
     * @return std::vector<ExpressionNodePtr>
     */
    std::vector<ExpressionNodePtr> getArguments() const;

    ~FunctionExpression() noexcept override = default;

    explicit FunctionExpression(DataTypePtr stamp, std::string functionName, std::unique_ptr<LogicalFunction> function);

  private:
    const std::string functionName;
    const std::unique_ptr<LogicalFunction> function;
};

}// namespace NES
#endif // NES_OPERATORS_INCLUDE_OPERATORS_EXPRESSIONS_FUNCTIONS_FUNCTIONEXPRESSIONNODE_HPP_
