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

#include <utility>
#include <API/Functions/Functions.hpp>
#include <Functions/LogicalFunctions/AndFunctionNode.hpp>
#include <Functions/LogicalFunctions/EqualsFunctionNode.hpp>
#include <Functions/LogicalFunctions/GreaterEqualsFunctionNode.hpp>
#include <Functions/LogicalFunctions/GreaterFunctionNode.hpp>
#include <Functions/LogicalFunctions/LessEqualsFunctionNode.hpp>
#include <Functions/LogicalFunctions/LessFunctionNode.hpp>
#include <Functions/LogicalFunctions/NegateFunctionNode.hpp>
#include <Functions/LogicalFunctions/OrFunctionNode.hpp>

namespace NES
{

FunctionNodePtr operator||(FunctionNodePtr leftExp, FunctionNodePtr rightExp)
{
    return OrFunctionNode::create(std::move(leftExp), std::move(rightExp));
}

FunctionNodePtr operator&&(FunctionNodePtr leftExp, FunctionNodePtr rightExp)
{
    return AndFunctionNode::create(std::move(leftExp), std::move(rightExp));
}

FunctionNodePtr operator==(FunctionNodePtr leftExp, FunctionNodePtr rightExp)
{
    return EqualsFunctionNode::create(std::move(leftExp), std::move(rightExp));
}

FunctionNodePtr operator!=(FunctionNodePtr leftExp, FunctionNodePtr rightExp)
{
    return NegateFunctionNode::create(EqualsFunctionNode::create(std::move(leftExp), std::move(rightExp)));
}

FunctionNodePtr operator<=(FunctionNodePtr leftExp, FunctionNodePtr rightExp)
{
    return LessEqualsFunctionNode::create(std::move(leftExp), std::move(rightExp));
}

FunctionNodePtr operator<(FunctionNodePtr leftExp, FunctionNodePtr rightExp)
{
    return LessFunctionNode::create(std::move(leftExp), std::move(rightExp));
}

FunctionNodePtr operator>=(FunctionNodePtr leftExp, FunctionNodePtr rightExp)
{
    return GreaterEqualsFunctionNode::create(std::move(leftExp), std::move(rightExp));
}

FunctionNodePtr operator>(FunctionNodePtr leftExp, FunctionNodePtr rightExp)
{
    return GreaterFunctionNode::create(std::move(leftExp), std::move(rightExp));
}

FunctionNodePtr operator!(FunctionNodePtr exp)
{
    return NegateFunctionNode::create(std::move(exp));
}
FunctionNodePtr operator!(FunctionItem exp)
{
    return !exp.getFunctionNode();
}

} /// namespace NES
