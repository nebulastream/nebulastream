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
#ifndef NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_LOGICAL_UNARY_OPERATOR_NODE_HPP_
#define NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_LOGICAL_UNARY_OPERATOR_NODE_HPP_

#include <Operators/AbstractOperators/Arity/UnaryOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>

namespace NES {

/**
 * @brief Logical unary operator. It hat at most one input data stream.
 */
class LogicalUnaryOperatorNode : public LogicalOperatorNode, public UnaryOperatorNode {

  public:
    explicit LogicalUnaryOperatorNode(OperatorId id);

    /**
    * @brief infers the input and out schema of this operator depending on its child.
    * @throws Exception if the schema could not be infers correctly or if the inferred types are not valid.
    * @return true if schema was correctly inferred
    */
    bool inferSchema() override;
};
}// namespace NES

#endif// NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_LOGICAL_UNARY_OPERATOR_NODE_HPP_
