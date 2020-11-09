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

#ifndef LOGICAL_OPERATOR_NODE_HPP
#define LOGICAL_OPERATOR_NODE_HPP

#include <API/ParameterTypes.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/LogicalOperatorForwardRefs.hpp>
#include <Operators/OperatorNode.hpp>

namespace z3 {

class expr;
typedef std::shared_ptr<expr> ExprPtr;

class context;
typedef std::shared_ptr<context> ContextPtr;
}// namespace z3

namespace NES {

class LogicalOperatorNode : public OperatorNode {

  public:

    LogicalOperatorNode(OperatorId id);

    /**
     * @brief Get the First Order Logic formula representation by the Z3 expression
     * @param context: the shared pointer to the z3::context
     * @return object of type Z3:expr
     */
    virtual void inferZ3Expression(z3::ContextPtr context) = 0;

    /**
     * @brief Get the Z3 expression for the logical operator
     * @return reference to the Z3 expression
     */
    z3::expr& getZ3Expression();

  protected:
    z3::ExprPtr expr;
};

}// namespace NES

#endif// LOGICAL_OPERATOR_NODE_HPP
