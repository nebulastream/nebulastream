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


/// Defines common arithmetical operations between function nodes.
ExpressionValue operator-(ExpressionValue functionLeft, ExpressionValue functionRight);
ExpressionValue operator+(ExpressionValue functionLeft, ExpressionValue functionRight);
ExpressionValue operator*(ExpressionValue functionLeft, ExpressionValue functionRight);
ExpressionValue operator/(ExpressionValue functionLeft, ExpressionValue functionRight);
ExpressionValue operator%(ExpressionValue functionLeft, ExpressionValue functionRight);
ExpressionValue MOD(ExpressionValue functionLeft, ExpressionValue functionRight);
ExpressionValue POWER(ExpressionValue functionLeft, ExpressionValue functionRight);
ExpressionValue ABS(ExpressionValue otherFunction);
ExpressionValue SQRT(ExpressionValue otherFunction);
ExpressionValue EXP(ExpressionValue otherFunction);
ExpressionValue ROUND(ExpressionValue otherFunction);
ExpressionValue CEIL(ExpressionValue otherFunction);
ExpressionValue FLOOR(ExpressionValue otherFunction);
ExpressionValue operator++(ExpressionValue otherFunction);
ExpressionValue operator--(ExpressionValue otherFunction);
const ExpressionValue operator++(ExpressionValue otherFunction, int value);
const ExpressionValue operator--(ExpressionValue otherFunction, int value);

/// Defines common binary arithmetical operations between a constant and an function node.
ExpressionValue operator+(const FunctionItem& functionLeft, ExpressionValue functionRight);
ExpressionValue operator-(const FunctionItem& functionLeft, ExpressionValue functionRight);
ExpressionValue operator*(const FunctionItem& functionLeft, ExpressionValue functionRight);
ExpressionValue operator/(const FunctionItem& functionLeft, ExpressionValue functionRight);
ExpressionValue operator%(const FunctionItem& functionLeft, ExpressionValue functionRight);
ExpressionValue MOD(const FunctionItem& functionLeft, ExpressionValue functionRight);
ExpressionValue POWER(const FunctionItem& functionLeft, ExpressionValue functionRight);

/// Defines common binary arithmetical operations between an function node and a constant.
ExpressionValue operator+(ExpressionValue functionLeft, const FunctionItem& functionRight);
ExpressionValue operator-(ExpressionValue functionLeft, const FunctionItem& functionRight);
ExpressionValue operator*(ExpressionValue functionLeft, const FunctionItem& functionRight);
ExpressionValue operator/(ExpressionValue functionLeft, const FunctionItem& functionRight);
ExpressionValue operator%(ExpressionValue functionLeft, const FunctionItem& functionRight);
ExpressionValue MOD(ExpressionValue functionLeft, const FunctionItem& functionRight);
ExpressionValue POWER(ExpressionValue functionLeft, const FunctionItem& functionRight);

/// Defines common binary arithmetical operations between two function items
ExpressionValue operator+(const FunctionItem& functionLeft, const FunctionItem& functionRight);
ExpressionValue operator-(const FunctionItem& functionLeft, const FunctionItem& functionRight);
ExpressionValue operator*(const FunctionItem& functionLeft, const FunctionItem& functionRight);
ExpressionValue operator/(const FunctionItem& functionLeft, const FunctionItem& functionRight);
ExpressionValue operator%(const FunctionItem& functionLeft, const FunctionItem& functionRight);
ExpressionValue MOD(const FunctionItem& functionLeft, const FunctionItem& functionRight);
ExpressionValue POWER(const FunctionItem& functionLeft, const FunctionItem& functionRight);


/// Defines common unary arithmetical operations on an function items.
ExpressionValue ABS(const FunctionItem& otherFunction);
ExpressionValue SQRT(const FunctionItem& otherFunction);
ExpressionValue EXP(const FunctionItem& otherFunction);
ExpressionValue ROUND(const FunctionItem& otherFunction);
ExpressionValue CEIL(const FunctionItem& otherFunction);
ExpressionValue FLOOR(const FunctionItem& otherFunction);
ExpressionValue operator++(const FunctionItem& otherFunction);
ExpressionValue operator--(const FunctionItem& otherFunction);
const ExpressionValue operator++(const FunctionItem& otherFunction, int);
const ExpressionValue operator--(const FunctionItem& otherFunction, int);

}
