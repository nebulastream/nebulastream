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

#include <API/Functions/Functions.hpp>
#include <API/Functions/LogicalFunctions.hpp>
#include <Functions/FunctionExpression.hpp>
#include <Functions/WellKnownFunctions.hpp>

namespace NES
{

ExpressionValue operator||(ExpressionValue functionLeft, ExpressionValue functionRight)
{
    return make_expression<FunctionExpression>({functionLeft, functionRight}, WellKnown::Or);
}

ExpressionValue operator&&(ExpressionValue functionLeft, ExpressionValue functionRight)
{
    return make_expression<FunctionExpression>({functionLeft, functionRight}, WellKnown::And);
}

ExpressionValue operator==(ExpressionValue functionLeft, ExpressionValue functionRight)
{
    return make_expression<FunctionExpression>({functionLeft, functionRight}, WellKnown::Equal);
}

ExpressionValue operator!=(ExpressionValue functionLeft, ExpressionValue functionRight)
{
    return make_expression<FunctionExpression>({functionLeft, functionRight}, WellKnown::NotEqual);
}

ExpressionValue operator<=(ExpressionValue functionLeft, ExpressionValue functionRight)
{
    return make_expression<FunctionExpression>({functionLeft, functionRight}, WellKnown::LessEqual);
}

ExpressionValue operator<(ExpressionValue functionLeft, ExpressionValue functionRight)
{
    return make_expression<FunctionExpression>({functionLeft, functionRight}, WellKnown::Less);
}

ExpressionValue operator>=(ExpressionValue functionLeft, ExpressionValue functionRight)
{
    return make_expression<FunctionExpression>({functionLeft, functionRight}, WellKnown::GreaterEqual);
}

ExpressionValue operator>(ExpressionValue functionLeft, ExpressionValue functionRight)
{
    return make_expression<FunctionExpression>({functionLeft, functionRight}, WellKnown::Greater);
}

ExpressionValue operator!(ExpressionValue exp)
{
    return make_expression<FunctionExpression>({exp}, WellKnown::Negate);
}
ExpressionValue operator!(const FunctionItem& exp)
{
    return make_expression<FunctionExpression>({exp.getNodeFunction()}, WellKnown::Negate);
}

}
