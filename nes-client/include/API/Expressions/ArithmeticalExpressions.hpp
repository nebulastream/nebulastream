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

#ifndef NES_CLIENT_INCLUDE_API_EXPRESSIONS_ARITHMETICALEXPRESSIONS_HPP_
#define NES_CLIENT_INCLUDE_API_EXPRESSIONS_ARITHMETICALEXPRESSIONS_HPP_

#include <memory>

namespace NES {

class ExpressionNode;

class ExpressionItem;
using ExpressionNodePtr = std::shared_ptr<ExpressionNode>;

/**
 * @brief Defines common arithmetical operations between expression nodes.
 */
ExpressionNodePtr operator-(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator+(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator*(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator/(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator%(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr READ(const ExpressionNodePtr& exp);
ExpressionNodePtr readT(const ExpressionNodePtr& left, const ExpressionNodePtr& middle, const ExpressionNodePtr& right);
ExpressionNodePtr teintersects(const ExpressionNodePtr& left, const ExpressionNodePtr& middle, const ExpressionNodePtr& right);
ExpressionNodePtr tpointatstbox(const ExpressionNodePtr& left, const ExpressionNodePtr& middle, const ExpressionNodePtr& right);
ExpressionNodePtr distancetpointstbox(const ExpressionNodePtr& left, const ExpressionNodePtr& middle, const ExpressionNodePtr& right);
ExpressionNodePtr tedwithin(const ExpressionNodePtr& left, const ExpressionNodePtr& middle, const ExpressionNodePtr& right);
ExpressionNodePtr seintersects(const ExpressionNodePtr& one, const ExpressionNodePtr& two, const ExpressionNodePtr& three, const ExpressionNodePtr& four, const ExpressionNodePtr& five, const ExpressionNodePtr& six);
ExpressionNodePtr MOD(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr POWER(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr ABS(const ExpressionNodePtr& exp);
ExpressionNodePtr SQRT(const ExpressionNodePtr& exp);
ExpressionNodePtr EXP(const ExpressionNodePtr& exp);
ExpressionNodePtr LN(const ExpressionNodePtr& exp);
ExpressionNodePtr LOG2(const ExpressionNodePtr& exp);
ExpressionNodePtr LOG10(const ExpressionNodePtr& exp);
ExpressionNodePtr SIN(const ExpressionNodePtr& exp);
ExpressionNodePtr COS(const ExpressionNodePtr& exp);
ExpressionNodePtr RADIANS(const ExpressionNodePtr& exp);
ExpressionNodePtr ROUND(const ExpressionNodePtr& exp);
ExpressionNodePtr CEIL(const ExpressionNodePtr& exp);
ExpressionNodePtr FLOOR(const ExpressionNodePtr& exp);
ExpressionNodePtr operator++(ExpressionNodePtr exp);
ExpressionNodePtr operator--(ExpressionNodePtr exp);
ExpressionNodePtr operator++(ExpressionNodePtr exp, int value);
ExpressionNodePtr operator--(ExpressionNodePtr exp, int value);

/**
 * @brief Defines common binary arithmetical operations between a constant and an expression node.
 */
ExpressionNodePtr operator+(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator-(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator*(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator/(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator%(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr MOD(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr POWER(ExpressionItem leftExp, ExpressionNodePtr rightExp);

/**
 * @brief Defines common binary arithmetical operations between an expression node and a constant.
 */
ExpressionNodePtr operator+(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator-(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator*(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator/(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator%(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr MOD(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr POWER(ExpressionNodePtr leftExp, ExpressionItem rightExp);

/**
 * @brief Defines common binary arithmetical operations between two expression items.
 */
ExpressionNodePtr operator+(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator-(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator*(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator/(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator%(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr MOD(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr POWER(ExpressionItem leftExp, ExpressionItem rightExp);

/**
 * @brief Defines common unary arithmetical operations on an expression items.
 */
ExpressionNodePtr ABS(ExpressionItem exp);
ExpressionNodePtr SQRT(ExpressionItem exp);
ExpressionNodePtr EXP(ExpressionItem exp);
ExpressionNodePtr LN(ExpressionItem exp);
ExpressionNodePtr LOG2(ExpressionItem exp);
ExpressionNodePtr READ(ExpressionItem exp);
ExpressionNodePtr LOG10(ExpressionItem exp);
ExpressionNodePtr readT(ExpressionItem left, ExpressionItem middle, ExpressionItem right);
ExpressionNodePtr teintersects(ExpressionItem left, ExpressionItem middle, ExpressionItem right);
ExpressionNodePtr tpointatstbox(ExpressionItem left, ExpressionItem middle, ExpressionItem right);
ExpressionNodePtr distancetpointstbox(ExpressionItem left, ExpressionItem middle, ExpressionItem right);
ExpressionNodePtr tedwithin(ExpressionItem left, ExpressionItem middle, ExpressionItem right);
ExpressionNodePtr seintersects(ExpressionItem one, ExpressionItem two, ExpressionItem three, ExpressionItem four, ExpressionItem five, ExpressionItem six);
ExpressionNodePtr SIN(ExpressionItem exp);
ExpressionNodePtr COS(ExpressionItem exp);
ExpressionNodePtr RADIANS(ExpressionItem exp);
ExpressionNodePtr ROUND(ExpressionItem exp);
ExpressionNodePtr CEIL(ExpressionItem exp);
ExpressionNodePtr FLOOR(ExpressionItem exp);
ExpressionNodePtr operator++(ExpressionItem exp);
ExpressionNodePtr operator--(ExpressionItem exp);
ExpressionNodePtr operator++(ExpressionItem exp, int);
ExpressionNodePtr operator--(ExpressionItem exp, int);

}// namespace NES
#endif// NES_CLIENT_INCLUDE_API_EXPRESSIONS_ARITHMETICALEXPRESSIONS_HPP_
