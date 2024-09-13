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

namespace NES
{

class FunctionNode;

class FunctionItem;
using FunctionNodePtr = std::shared_ptr<FunctionNode>;

/// Defines common arithmetical operations between function nodes.
FunctionNodePtr operator-(FunctionNodePtr leftExp, FunctionNodePtr rightExp);
FunctionNodePtr operator+(FunctionNodePtr leftExp, FunctionNodePtr rightExp);
FunctionNodePtr operator*(FunctionNodePtr leftExp, FunctionNodePtr rightExp);
FunctionNodePtr operator/(FunctionNodePtr leftExp, FunctionNodePtr rightExp);
FunctionNodePtr operator%(FunctionNodePtr leftExp, FunctionNodePtr rightExp);
FunctionNodePtr MOD(FunctionNodePtr leftExp, FunctionNodePtr rightExp);
FunctionNodePtr POWER(FunctionNodePtr leftExp, FunctionNodePtr rightExp);
FunctionNodePtr ABS(const FunctionNodePtr& exp);
FunctionNodePtr SQRT(const FunctionNodePtr& exp);
FunctionNodePtr EXP(const FunctionNodePtr& exp);
FunctionNodePtr LN(const FunctionNodePtr& exp);
FunctionNodePtr LOG2(const FunctionNodePtr& exp);
FunctionNodePtr LOG10(const FunctionNodePtr& exp);
FunctionNodePtr SIN(const FunctionNodePtr& exp);
FunctionNodePtr COS(const FunctionNodePtr& exp);
FunctionNodePtr RADIANS(const FunctionNodePtr& exp);
FunctionNodePtr ROUND(const FunctionNodePtr& exp);
FunctionNodePtr CEIL(const FunctionNodePtr& exp);
FunctionNodePtr FLOOR(const FunctionNodePtr& exp);
FunctionNodePtr operator++(FunctionNodePtr exp);
FunctionNodePtr operator--(FunctionNodePtr exp);
FunctionNodePtr operator++(FunctionNodePtr exp, int value);
FunctionNodePtr operator--(FunctionNodePtr exp, int value);

/// Defines common binary arithmetical operations between a constant and an function node.
FunctionNodePtr operator+(FunctionItem leftExp, FunctionNodePtr rightExp);
FunctionNodePtr operator-(FunctionItem leftExp, FunctionNodePtr rightExp);
FunctionNodePtr operator*(FunctionItem leftExp, FunctionNodePtr rightExp);
FunctionNodePtr operator/(FunctionItem leftExp, FunctionNodePtr rightExp);
FunctionNodePtr operator%(FunctionItem leftExp, FunctionNodePtr rightExp);
FunctionNodePtr MOD(FunctionItem leftExp, FunctionNodePtr rightExp);
FunctionNodePtr POWER(FunctionItem leftExp, FunctionNodePtr rightExp);

/// Defines common binary arithmetical operations between an function node and a constant.
FunctionNodePtr operator+(FunctionNodePtr leftExp, FunctionItem rightExp);
FunctionNodePtr operator-(FunctionNodePtr leftExp, FunctionItem rightExp);
FunctionNodePtr operator*(FunctionNodePtr leftExp, FunctionItem rightExp);
FunctionNodePtr operator/(FunctionNodePtr leftExp, FunctionItem rightExp);
FunctionNodePtr operator%(FunctionNodePtr leftExp, FunctionItem rightExp);
FunctionNodePtr MOD(FunctionNodePtr leftExp, FunctionItem rightExp);
FunctionNodePtr POWER(FunctionNodePtr leftExp, FunctionItem rightExp);

/// Defines common binary arithmetical operations between two function items
FunctionNodePtr operator+(FunctionItem leftExp, FunctionItem rightExp);
FunctionNodePtr operator-(FunctionItem leftExp, FunctionItem rightExp);
FunctionNodePtr operator*(FunctionItem leftExp, FunctionItem rightExp);
FunctionNodePtr operator/(FunctionItem leftExp, FunctionItem rightExp);
FunctionNodePtr operator%(FunctionItem leftExp, FunctionItem rightExp);
FunctionNodePtr MOD(FunctionItem leftExp, FunctionItem rightExp);
FunctionNodePtr POWER(FunctionItem leftExp, FunctionItem rightExp);


/// Defines common unary arithmetical operations on an function items.
/// FunctionNodePtr ABS(FunctionItem exp);
FunctionNodePtr SQRT(FunctionItem exp);
FunctionNodePtr EXP(FunctionItem exp);
FunctionNodePtr LN(FunctionItem exp);
FunctionNodePtr LOG2(FunctionItem exp);
FunctionNodePtr LOG10(FunctionItem exp);
FunctionNodePtr SIN(FunctionItem exp);
FunctionNodePtr COS(FunctionItem exp);
FunctionNodePtr RADIANS(FunctionItem exp);
FunctionNodePtr ROUND(FunctionItem exp);
FunctionNodePtr CEIL(FunctionItem exp);
FunctionNodePtr FLOOR(FunctionItem exp);
FunctionNodePtr operator++(FunctionItem exp);
FunctionNodePtr operator--(FunctionItem exp);
FunctionNodePtr operator++(FunctionItem exp, int);
FunctionNodePtr operator--(FunctionItem exp, int);

}
