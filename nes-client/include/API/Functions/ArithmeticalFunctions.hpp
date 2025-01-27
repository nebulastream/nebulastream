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
#include <Functions/NodeFunction.hpp>

namespace NES
{


/// Defines common arithmetical operations between function nodes.
std::shared_ptr<NodeFunction>
operator-(const std::shared_ptr<NodeFunction>& functionLeft, const std::shared_ptr<NodeFunction>& functionRight);
std::shared_ptr<NodeFunction>
operator+(const std::shared_ptr<NodeFunction>& functionLeft, const std::shared_ptr<NodeFunction>& functionRight);
std::shared_ptr<NodeFunction>
operator*(const std::shared_ptr<NodeFunction>& functionLeft, const std::shared_ptr<NodeFunction>& functionRight);
std::shared_ptr<NodeFunction>
operator/(const std::shared_ptr<NodeFunction>& functionLeft, const std::shared_ptr<NodeFunction>& functionRight);
std::shared_ptr<NodeFunction>
operator%(const std::shared_ptr<NodeFunction>& functionLeft, const std::shared_ptr<NodeFunction>& functionRight);
std::shared_ptr<NodeFunction> MOD(const std::shared_ptr<NodeFunction>& functionLeft, const std::shared_ptr<NodeFunction>& functionRight);
std::shared_ptr<NodeFunction> POWER(const std::shared_ptr<NodeFunction>& functionLeft, const std::shared_ptr<NodeFunction>& functionRight);
std::shared_ptr<NodeFunction> ABS(const std::shared_ptr<NodeFunction>& otherFunction);
std::shared_ptr<NodeFunction> SQRT(const std::shared_ptr<NodeFunction>& otherFunction);
std::shared_ptr<NodeFunction> EXP(const std::shared_ptr<NodeFunction>& otherFunction);
std::shared_ptr<NodeFunction> ROUND(const std::shared_ptr<NodeFunction>& otherFunction);
std::shared_ptr<NodeFunction> CEIL(const std::shared_ptr<NodeFunction>& otherFunction);
std::shared_ptr<NodeFunction> FLOOR(const std::shared_ptr<NodeFunction>& otherFunction);
std::shared_ptr<NodeFunction> operator++(const std::shared_ptr<NodeFunction>& otherFunction);
std::shared_ptr<NodeFunction> operator--(const std::shared_ptr<NodeFunction>& otherFunction);
const std::shared_ptr<NodeFunction> operator++(const std::shared_ptr<NodeFunction>& otherFunction, int value);
const std::shared_ptr<NodeFunction> operator--(const std::shared_ptr<NodeFunction>& otherFunction, int value);

/// Defines common binary arithmetical operations between a constant and an function node.
std::shared_ptr<NodeFunction> operator+(const FunctionItem& functionLeft, const std::shared_ptr<NodeFunction>& functionRight);
std::shared_ptr<NodeFunction> operator-(const FunctionItem& functionLeft, const std::shared_ptr<NodeFunction>& functionRight);
std::shared_ptr<NodeFunction> operator*(const FunctionItem& functionLeft, const std::shared_ptr<NodeFunction>& functionRight);
std::shared_ptr<NodeFunction> operator/(const FunctionItem& functionLeft, const std::shared_ptr<NodeFunction>& functionRight);
std::shared_ptr<NodeFunction> operator%(const FunctionItem& functionLeft, const std::shared_ptr<NodeFunction>& functionRight);
std::shared_ptr<NodeFunction> MOD(const FunctionItem& functionLeft, const std::shared_ptr<NodeFunction>& functionRight);
std::shared_ptr<NodeFunction> POWER(const FunctionItem& functionLeft, const std::shared_ptr<NodeFunction>& functionRight);

/// Defines common binary arithmetical operations between an function node and a constant.
std::shared_ptr<NodeFunction> operator+(const std::shared_ptr<NodeFunction>& functionLeft, const FunctionItem& functionRight);
std::shared_ptr<NodeFunction> operator-(const std::shared_ptr<NodeFunction>& functionLeft, const FunctionItem& functionRight);
std::shared_ptr<NodeFunction> operator*(const std::shared_ptr<NodeFunction>& functionLeft, const FunctionItem& functionRight);
std::shared_ptr<NodeFunction> operator/(const std::shared_ptr<NodeFunction>& functionLeft, const FunctionItem& functionRight);
std::shared_ptr<NodeFunction> operator%(const std::shared_ptr<NodeFunction>& functionLeft, const FunctionItem& functionRight);
std::shared_ptr<NodeFunction> MOD(const std::shared_ptr<NodeFunction>& functionLeft, const FunctionItem& functionRight);
std::shared_ptr<NodeFunction> POWER(const std::shared_ptr<NodeFunction>& functionLeft, const FunctionItem& functionRight);

/// Defines common binary arithmetical operations between two function items
std::shared_ptr<NodeFunction> operator+(const FunctionItem& functionLeft, const FunctionItem& functionRight);
std::shared_ptr<NodeFunction> operator-(const FunctionItem& functionLeft, const FunctionItem& functionRight);
std::shared_ptr<NodeFunction> operator*(const FunctionItem& functionLeft, const FunctionItem& functionRight);
std::shared_ptr<NodeFunction> operator/(const FunctionItem& functionLeft, const FunctionItem& functionRight);
std::shared_ptr<NodeFunction> operator%(const FunctionItem& functionLeft, const FunctionItem& functionRight);
std::shared_ptr<NodeFunction> MOD(const FunctionItem& functionLeft, const FunctionItem& functionRight);
std::shared_ptr<NodeFunction> POWER(const FunctionItem& functionLeft, const FunctionItem& functionRight);


/// Defines common unary arithmetical operations on an function items.
std::shared_ptr<NodeFunction> ABS(const FunctionItem& otherFunction);
std::shared_ptr<NodeFunction> SQRT(const FunctionItem& otherFunction);
std::shared_ptr<NodeFunction> EXP(const FunctionItem& otherFunction);
std::shared_ptr<NodeFunction> ROUND(const FunctionItem& otherFunction);
std::shared_ptr<NodeFunction> CEIL(const FunctionItem& otherFunction);
std::shared_ptr<NodeFunction> FLOOR(const FunctionItem& otherFunction);
std::shared_ptr<NodeFunction> operator++(const FunctionItem& otherFunction);
std::shared_ptr<NodeFunction> operator--(const FunctionItem& otherFunction);
const std::shared_ptr<NodeFunction> operator++(const FunctionItem& otherFunction, int);
const std::shared_ptr<NodeFunction> operator--(const FunctionItem& otherFunction, int);

}
