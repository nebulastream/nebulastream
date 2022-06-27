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
#ifndef NES_INCLUDE_NODES_EXPRESSIONS_UDFCALLEXPRESSIONS_UDFARGUMENTSNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_UDFCALLEXPRESSIONS_UDFARGUMENTSNODE_HPP_

#include <Nodes/Expressions/ExpressionNode.hpp>

namespace NES::Experimental {

class UdfArgumentsNode : public ExpressionNode {
  public:
    UdfArgumentsNode();
    explicit UdfArgumentsNode(UdfArgumentsNode* other);
    ~UdfArgumentsNode() override = default;

    /**
     *
     * @param functionArguments
     * @return
     */
    static ExpressionNodePtr create(std::vector<ExpressionNodePtr> functionArguments);

    /**
     *
     * @param functionArgs
     */
    static void setFunctionArguments(std::vector<ExpressionNodePtr> functionArgs);

    std::vector<ExpressionNodePtr> getFunctionArguments();

    std::string toString() const override;

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override;

  private:
    /**
     * @brief Function arguments can be any kind of ExpressionNode
     */
    static std::vector<ExpressionNodePtr> functionArguments;
};

}// namespace NES::Experimental

#endif//NES_INCLUDE_NODES_EXPRESSIONS_UDFCALLEXPRESSIONS_UDFARGUMENTSNODE_HPP_
#endif// PYTHON_UDF_ENABLED