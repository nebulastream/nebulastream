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

#ifndef NES_INCLUDE_NODES_EXPRESSIONS_UDFCALLEXPRESSIONS_UDFCALLEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_UDFCALLEXPRESSIONS_UDFCALLEXPRESSIONNODE_HPP_

#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/UdfCallExpressions/UdfCallExpressionNode.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Catalogs/UDF/UdfDescriptor.hpp>

namespace NES {

class ConstantValueExpressionNode;
using ConstantValueExpressionNodePtr = std::shared_ptr<ConstantValueExpressionNode>;

/**
 * @brief This node represents a CALL expression, for calling user-defined functions
 */
class UdfCallExpressionNode : public ExpressionNode {
  public:
    UdfCallExpressionNode();
    explicit UdfCallExpressionNode(UdfCallExpressionNode* other);
    ~UdfCallExpressionNode() = default;

    /**
     * @brief a function call needs the name of a udf and can take 0 or more function arguments.
     * @param udfName name of the udf
     * @param functionArguments 0 or more function arguments
     * @return a UdfCallExpressionNode
     */
    static ExpressionNodePtr create(const ConstantValueExpressionNodePtr& udfFunctionName,
                                    std::vector<ExpressionNodePtr> functionArguments);
    /**
    * @brief determine the stamp of the Udf call by checking the return type of the function
     * An error is thrown when no UDF descriptor is set.
    */
    void inferStamp(const Optimizer::TypeInferencePhaseContext& ctx, SchemaPtr schema) override;

    std::string toString() const override;

    /**
     * @brief The udfName node is set as child[0] and a UdfArgumentsNode is generated which holds the
     * function arguments and is set as child[1].
     * @param udfName name of the UDF that needs to be called
     * @param functionArguments function arguments for the UDF
     */
    void setChildren(const ExpressionNodePtr& udfFunctionName, std::vector<ExpressionNodePtr> functionArgs);

    /**
     * @return the name of the UDF as a ConstantValueExpressionNode
     */
    ExpressionNodePtr getUdfNameNode();

    /**
     * @return a UdfArgumentsNode containing all function arguments passed to the UDF
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
     * set the UdfDescriptor manually to retrieve the return type.
     * @param pyUdfDescriptor The (python) udf descriptor
     */
    void setUdfDescriptorPtr(const Catalogs::UdfDescriptorPtr& udfDescriptor);
    void setUdfName(const ConstantValueExpressionNodePtr& udfFunctionName);
    const std::string& getUdfName() const;

  private:
    /**
     * For now we keep a PythonUdfDescriptorPtr for a minimal working example. As soon as
     * support for other languages will be added we can use the base class UdfDescriptor instead.
     */
    Catalogs::UdfDescriptorPtr udfDescriptorPtr;
    std::vector<ExpressionNodePtr> functionArguments;
    std::string udfName;
};

}// namespace NES::Experimental

#endif//NES_INCLUDE_NODES_EXPRESSIONS_UDFCALLEXPRESSIONS_UDFCALLEXPRESSIONNODE_HPP_