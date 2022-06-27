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

//TODO Change to UDF_ENABLED? since it will also be implemented for other languages
#ifdef PYTHON_UDF_ENABLED
#ifndef NES_INCLUDE_NODES_EXPRESSIONS_UDFCALLEXPRESSIONS_UDFCALLEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_UDFCALLEXPRESSIONS_UDFCALLEXPRESSIONNODE_HPP_

#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/UdfCallExpressions/UdfCallExpressionNode.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>

namespace NES::Experimental {

class ConstantValueExpressionNode;
using ConstantValueExpressionNodePtr = std::shared_ptr<NES::ConstantValueExpressionNode>;

/**
 * @brief
 */
class UdfCallExpressionNode : public ExpressionNode {
  public:
    UdfCallExpressionNode();
    explicit UdfCallExpressionNode(UdfCallExpressionNode* other);
    ~UdfCallExpressionNode() override = default;

    /**
     * @brief UDF calls always need to start with the UDF name that has to be called.
     * Additionally, a call can have 0 or more function arguments. To solve this, create
     * is defined as a variadic function (taking a variable number of arguments)
     * @tparam Args
     * @param udfName
     * @param functionArguments
     * @return
     */
    template<typename... Args>
    static ExpressionNodePtr create(const ConstantValueExpressionNodePtr& udfName,
                                    Args... functionArguments);
    /**
    * @brief
    */
    void inferStamp(SchemaPtr schema) override;

    /**
    * @brief
    */
    std::string toString() const override;

    /**
     * @brief The udfName node is set as child[0] and a UdfArgumentsNode is generated which holds the
     * function arguments and is set as child[1].
     * @param udfName name of the UDF that needs to be called
     * @param functionArguments function arguments for the UDF
     */
    void setChildren(const ExpressionNodePtr& udfName, std::vector<ExpressionNodePtr> functionArguments);

    /**
     * @return the name of the UDF as a ConstantValueExpressionNode
     */
    ExpressionNodePtr getUdfName();

    /**
     * @return a UdfArgumentsNode containing all function arguments passed to the UDF
     */
    ExpressionNodePtr getFunctionArgumentsNode();

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
    void setPythonUdfDescriptorPtr(const PythonUdfDescriptorPtr& pyUdfDescriptor);

  private:
    PythonUdfDescriptorPtr pythonUdfDescriptorPtr;
};

}// namespace NES::Experimental

#endif//NES_INCLUDE_NODES_EXPRESSIONS_UDFCALLEXPRESSIONS_UDFCALLEXPRESSIONNODE_HPP_
#endif// PYTHON_UDF_ENABLED