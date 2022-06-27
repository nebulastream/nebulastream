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

#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/UdfExpressions.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/UdfCallExpressions/UdfCallExpressionNode.hpp>
#include <utility>

namespace NES::Experimental {

ExpressionNodePtr CALL(const NES::ExpressionItem& udfName, ExpressionNodePtr arguments...) {
    auto udfNameExpression = udfName.getExpressionNode();
    if (!udfNameExpression->instanceOf<NES::ConstantValueExpressionNode>()) {
        NES_ERROR("UDF name has to be a ConstantValueExpression but it was a " + udfNameExpression->toString());
    }
    auto udfNameConstantValueExpression = udfNameExpression->as<NES::ConstantValueExpressionNode>();
    return UdfCallExpressionNode::create(udfNameConstantValueExpression, std::move(arguments));
}

}// namespace NES::Experimental