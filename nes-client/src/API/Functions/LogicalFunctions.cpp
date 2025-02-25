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
#include <Functions/LogicalFunctions/NodeFunctionAnd.hpp>
#include <Functions/LogicalFunctions/NodeFunctionEquals.hpp>
#include <Functions/LogicalFunctions/NodeFunctionGreater.hpp>
#include <Functions/LogicalFunctions/NodeFunctionGreaterEquals.hpp>
#include <Functions/LogicalFunctions/NodeFunctionLess.hpp>
#include <Functions/LogicalFunctions/NodeFunctionLessEquals.hpp>
#include <Functions/LogicalFunctions/NodeFunctionNegate.hpp>
#include <Functions/LogicalFunctions/NodeFunctionOr.hpp>

namespace NES
{

NodeFunctionPtr operator||(NodeFunctionPtr functionLeft, NodeFunctionPtr functionRight)
{
    return NodeFunctionOr::create(std::move(functionLeft), std::move(functionRight));
}

NodeFunctionPtr operator&&(NodeFunctionPtr functionLeft, NodeFunctionPtr functionRight)
{
    return NodeFunctionAnd::create(std::move(functionLeft), std::move(functionRight));
}

NodeFunctionPtr operator==(NodeFunctionPtr functionLeft, NodeFunctionPtr functionRight)
{
    return NodeFunctionEquals::create(std::move(functionLeft), std::move(functionRight));
}

NodeFunctionPtr operator!=(NodeFunctionPtr functionLeft, NodeFunctionPtr functionRight)
{
    return NodeFunctionNegate::create(NodeFunctionEquals::create(std::move(functionLeft), std::move(functionRight)));
}

NodeFunctionPtr operator<=(NodeFunctionPtr functionLeft, NodeFunctionPtr functionRight)
{
    return NodeFunctionLessEquals::create(std::move(functionLeft), std::move(functionRight));
}

NodeFunctionPtr operator<(NodeFunctionPtr functionLeft, NodeFunctionPtr functionRight)
{
    return NodeFunctionLess::create(std::move(functionLeft), std::move(functionRight));
}

NodeFunctionPtr operator>=(NodeFunctionPtr functionLeft, NodeFunctionPtr functionRight)
{
    return NodeFunctionGreaterEquals::create(std::move(functionLeft), std::move(functionRight));
}

NodeFunctionPtr operator>(NodeFunctionPtr functionLeft, NodeFunctionPtr functionRight)
{
    return NodeFunctionGreater::create(std::move(functionLeft), std::move(functionRight));
}

NodeFunctionPtr operator!(NodeFunctionPtr exp)
{
    return NodeFunctionNegate::create(std::move(exp));
}
NodeFunctionPtr operator!(FunctionItem exp)
{
    return !exp.getNodeFunction();
}

}
