/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_WINDOWING_WINDOW_OPERATOR_NODE_HPP_
#define NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_WINDOWING_WINDOW_OPERATOR_NODE_HPP_
#include <Operators/AbstractOperators/Arity/UnaryOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorForwardRefs.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>

namespace NES {

class WindowOperatorNode;
using WindowOperatorNodePtr = std::shared_ptr<WindowOperatorNode>;

/**
 * @brief Window operator, which defines the window definition.
 */
class WindowOperatorNode : public LogicalUnaryOperatorNode {
  public:
    WindowOperatorNode(Windowing::LogicalWindowDefinitionPtr const& windowDefinition, OperatorId id);
    /**
    * @brief Gets the window definition of the window operator.
    * @return LogicalWindowDefinitionPtr
    */
    Windowing::LogicalWindowDefinitionPtr getWindowDefinition() const;

  protected:
    const Windowing::LogicalWindowDefinitionPtr windowDefinition;
};

}// namespace NES

#endif// NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_WINDOWING_WINDOW_OPERATOR_NODE_HPP_
