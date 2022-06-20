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

#ifndef NES_INCLUDE_NODES_EXPRESSIONS_FUNCTIONCALLEXPRESSIONS_UDFCALLEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_FUNCTIONCALLEXPRESSIONS_UDFCALLEXPRESSIONNODE_HPP_

#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Nodes/Expressions/UnaryExpressionNode.hpp>
#include <utility>

namespace NES::Experimental {
/**
 * @brief This class indicates that a node is a udf call expression.
 */
class UdfCallExpressionNode {
  public:
    UdfCallExpressionNode() = default;
    virtual ~UdfCallExpressionNode() noexcept = default;
};

}// namespace NES::Experimental

#endif// NES_INCLUDE_NODES_EXPRESSIONS_FUNCTIONCALLEXPRESSIONS_UDFCALLEXPRESSIONNODE_HPP_