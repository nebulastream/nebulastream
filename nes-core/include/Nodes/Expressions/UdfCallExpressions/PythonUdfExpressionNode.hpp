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

#ifdef PYTHON_UDF_ENABLED
#ifndef NES_INCLUDE_NODES_EXPRESSIONS_FUNCTIONCALLEXPRESSIONS_PYTHONUDFEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_FUNCTIONCALLEXPRESSIONS_PYTHONUDFEXPRESSIONNODE_HPP_

#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/UdfCallExpressions/UdfCallExpressionNode.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>

namespace NES::Experimental {

class ConstantValueExpressionNode;
using ConstantValueExpressionNodePtr = std::shared_ptr<NES::ConstantValueExpressionNode>;

/**
 * @brief
 */
class PythonUdfExpressionNode : public ExpressionNode, public UdfCallExpressionNode {
  public:
    PythonUdfExpressionNode();
    explicit PythonUdfExpressionNode(PythonUdfExpressionNode* other);
    ~PythonUdfExpressionNode() override = default;

    /**
    * @brief
    */
    static ExpressionNodePtr create(ConstantValueExpressionNodePtr const& udfName,
                                    ConstantValueExpressionNodePtr const& functionArguments);

    /**
    * @brief
    */
    void inferStamp(SchemaPtr schema) override;

    /**
    * @brief
    */
    std::string toString() const override;

    /**
    * @brief
    */
    void setChildren(const ExpressionNodePtr& udfName, const ExpressionNodePtr& functionArguments);

    /**
    * @brief
    */
    ExpressionNodePtr getUdfName();

    /**
    * @brief
    */
    ExpressionNodePtr getFunctionArguments();

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override;
};

}// namespace NES::Experimental

#endif//NES_INCLUDE_NODES_EXPRESSIONS_FUNCTIONCALLEXPRESSIONS_PYTHONUDFEXPRESSIONNODE_HPP_
#endif// PYTHON_UDF_ENABLED