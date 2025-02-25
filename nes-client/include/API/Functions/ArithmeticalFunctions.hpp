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

#pragma once

#include <memory>
#include <API/Functions/Functions.hpp>

namespace NES
{

class FunctionItem;
class NodeFunction;
using NodeFunctionPtr = std::shared_ptr<NodeFunction>;

/// Defines common arithmetical operations between function nodes.
NodeFunctionPtr operator-(NodeFunctionPtr leftExp, NodeFunctionPtr rightExp);
NodeFunctionPtr operator+(NodeFunctionPtr leftExp, NodeFunctionPtr rightExp);
NodeFunctionPtr operator*(NodeFunctionPtr leftExp, NodeFunctionPtr rightExp);
NodeFunctionPtr operator/(NodeFunctionPtr leftExp, NodeFunctionPtr rightExp);
NodeFunctionPtr operator%(NodeFunctionPtr leftExp, NodeFunctionPtr rightExp);
NodeFunctionPtr MOD(NodeFunctionPtr leftExp, NodeFunctionPtr rightExp);
NodeFunctionPtr POWER(NodeFunctionPtr leftExp, NodeFunctionPtr rightExp);
NodeFunctionPtr ABS(const NodeFunctionPtr& exp);
NodeFunctionPtr SQRT(const NodeFunctionPtr& exp);
NodeFunctionPtr EXP(const NodeFunctionPtr& exp);
NodeFunctionPtr ROUND(const NodeFunctionPtr& exp);
NodeFunctionPtr CEIL(const NodeFunctionPtr& exp);
NodeFunctionPtr FLOOR(const NodeFunctionPtr& exp);
NodeFunctionPtr operator++(NodeFunctionPtr exp);
NodeFunctionPtr operator--(NodeFunctionPtr exp);
NodeFunctionPtr operator++(NodeFunctionPtr exp, int value);
NodeFunctionPtr operator--(NodeFunctionPtr exp, int value);

/// Defines common binary arithmetical operations between a constant and an function node.
NodeFunctionPtr operator+(FunctionItem leftExp, NodeFunctionPtr rightExp);
NodeFunctionPtr operator-(FunctionItem leftExp, NodeFunctionPtr rightExp);
NodeFunctionPtr operator*(FunctionItem leftExp, NodeFunctionPtr rightExp);
NodeFunctionPtr operator/(FunctionItem leftExp, NodeFunctionPtr rightExp);
NodeFunctionPtr operator%(FunctionItem leftExp, NodeFunctionPtr rightExp);
NodeFunctionPtr MOD(FunctionItem leftExp, NodeFunctionPtr rightExp);
NodeFunctionPtr POWER(FunctionItem leftExp, NodeFunctionPtr rightExp);

/// Defines common binary arithmetical operations between an function node and a constant.
NodeFunctionPtr operator+(NodeFunctionPtr leftExp, FunctionItem rightExp);
NodeFunctionPtr operator-(NodeFunctionPtr leftExp, FunctionItem rightExp);
NodeFunctionPtr operator*(NodeFunctionPtr leftExp, FunctionItem rightExp);
NodeFunctionPtr operator/(NodeFunctionPtr leftExp, FunctionItem rightExp);
NodeFunctionPtr operator%(NodeFunctionPtr leftExp, FunctionItem rightExp);
NodeFunctionPtr MOD(NodeFunctionPtr leftExp, FunctionItem rightExp);
NodeFunctionPtr POWER(NodeFunctionPtr leftExp, FunctionItem rightExp);

/// Defines common binary arithmetical operations between two function items
NodeFunctionPtr operator+(FunctionItem leftExp, FunctionItem rightExp);
NodeFunctionPtr operator-(FunctionItem leftExp, FunctionItem rightExp);
NodeFunctionPtr operator*(FunctionItem leftExp, FunctionItem rightExp);
NodeFunctionPtr operator/(FunctionItem leftExp, FunctionItem rightExp);
NodeFunctionPtr operator%(FunctionItem leftExp, FunctionItem rightExp);
NodeFunctionPtr MOD(FunctionItem leftExp, FunctionItem rightExp);
NodeFunctionPtr POWER(FunctionItem leftExp, FunctionItem rightExp);


/// Defines common unary arithmetical operations on an function items.
NodeFunctionPtr ABS(FunctionItem exp);
NodeFunctionPtr SQRT(FunctionItem exp);
NodeFunctionPtr EXP(FunctionItem exp);
NodeFunctionPtr ROUND(FunctionItem exp);
NodeFunctionPtr CEIL(FunctionItem exp);
NodeFunctionPtr FLOOR(FunctionItem exp);
NodeFunctionPtr operator++(FunctionItem exp);
NodeFunctionPtr operator--(FunctionItem exp);
NodeFunctionPtr operator++(FunctionItem exp, int);
NodeFunctionPtr operator--(FunctionItem exp, int);

}
