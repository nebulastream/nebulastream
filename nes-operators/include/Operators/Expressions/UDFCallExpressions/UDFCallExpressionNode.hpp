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

#ifndef NES_CORE_INCLUDE_NODES_EXPRESSIONS_UDFCALLEXPRESSIONS_UDFCALLEXPRESSIONNODE_HPP_
#define NES_CORE_INCLUDE_NODES_EXPRESSIONS_UDFCALLEXPRESSIONS_UDFCALLEXPRESSIONNODE_HPP_

#include <Operators/LogicalOperators/UDFs/UDFDescriptor.hpp>
#include <Operators/Expressions/ConstantValueExpressionNode.hpp>
#include <Operators/Expressions/ExpressionNode.hpp>
#include <Operators/Expressions/UDFCallExpressions/UDFCallExpressionNode.hpp>

namespace NES {

class ConstantValueExpressionNode;
using ConstantValueExpressionNodePtr = std::shared_ptr<ConstantValueExpressionNode>;

/**
 * @brief This node represents a CALL expression, for calling user-defined functions
 */
class UDFCallExpressionNode : public ExpressionNode {
  public:
    explicit UDFCallExpressionNode(UDFCallExpressionNode* other);
    explicit UDFCallExpressionNode(const ConstantValueExpressionNodePtr& udfName,
                                   std::vector<ExpressionNodePtr> functionArguments);
    ~UDFCallExpressionNode() = default;

    /**
     * @brief a function call needs the name of a udf and can take 0 or more function arguments.
     * @param udfName name of the udf
     * @param functionArguments 0 or more function arguments
     * @return a UdfCallExpressionNode
     */
    static ExpressionNodePtr create(const ConstantValueExpressionNodePtr& udfName,
                                    const std::vector<ExpressionNodePtr>& functionArguments);
    /**
     * @brief determine the stamp of the Udf call by checking the return type of the function
     * An error is thrown when no UDF descriptor is set.
     * @param typeInferencePhaseContext
     * @param schema
     */
    void inferStamp(const Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext, SchemaPtr schema) override;

    bool equal(NodePtr const& rhs) const final;
    std::string toString() const override;

    /**
     * @brief The udfName node is set as child[0] and a vector which holds the
     * function arguments is saved.
     * @param udfName name of the UDF that needs to be called
     * @param functionArguments function arguments for the UDF
     */
    void setChildren(const ConstantValueExpressionNodePtr& udfName, std::vector<ExpressionNodePtr> functionArguments);

    /**
     * @return the name of the UDF as a ConstantValueExpressionNode
     */
    ExpressionNodePtr getUDFNameNode() const;

    /**
     * @return a vector containing all function arguments passed to the UDF
     */
    std::vector<ExpressionNodePtr> getFunctionArguments();

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override;

    /**
     * @brief It is difficult to infer the stamp of any UDF, since the ExpressionNode has
     * no way to check the return type of the function. We therefore need (for now) to
     * set the UDFDescriptor manually to retrieve the return type.
     * @param pyUdfDescriptor The (python) udf descriptor
     */
    void setUDFDescriptorPtr(const Catalogs::UDF::UDFDescriptorPtr& UDFDescriptor);

    /**
     * @return a string with the UDF name
     */
    const std::string& getUDFName() const;

  private:
    Catalogs::UDF::UDFDescriptorPtr UDFDescriptor;
    std::vector<ExpressionNodePtr> functionArguments;
    ConstantValueExpressionNodePtr udfName;
};

}// namespace NES

#endif// NES_CORE_INCLUDE_NODES_EXPRESSIONS_UDFCALLEXPRESSIONS_UDFCALLEXPRESSIONNODE_HPP_
