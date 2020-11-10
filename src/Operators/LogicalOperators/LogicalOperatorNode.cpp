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

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Optimizer/Utils/OperatorToZ3ExprUtil.hpp>
#include <z3++.h>

namespace NES {

LogicalOperatorNode::LogicalOperatorNode(uint64_t id) : expr(nullptr), OperatorNode(id) {}

z3::expr& LogicalOperatorNode::getZ3Expression() {
    return *expr;
}
void LogicalOperatorNode::inferZ3Expression(z3::ContextPtr context) {
    OperatorNodePtr operatorNode = shared_from_this()->as<OperatorNode>();
    NES_TRACE("Inferring Z3 expressions for " << operatorNode->toString());
    expr = std::make_shared<z3::expr>(OperatorToZ3ExprUtil::createForOperator(operatorNode, *context));
    for (auto child : children) {
        child->as<LogicalOperatorNode>()->inferZ3Expression(context);
    }
}

}// namespace NES
