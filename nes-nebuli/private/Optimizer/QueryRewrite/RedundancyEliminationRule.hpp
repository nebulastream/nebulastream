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
/*
#pragma once

#include <Functions/BinaryLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>

namespace NES
{
class Node;
class LogicalSelectionOperator;
}

namespace NES::Optimizer
{

class RedundancyEliminationRule;
using RedundancyEliminationRulePtr = std::shared_ptr<RedundancyEliminationRule>;

/// @note The rule is not currently used - since the compiler already does most of these optimizations -
///       but can applied and expanded in the future
/// @brief This class is responsible for reducing redundancies present in the predicates. Namely, three strategies can applied:
///  1) Constant_moving/folding
///  Example (folding): filter(Attribute("id") > 5+5) -> filter(Attribute("id") > 10)
///  Example (moving): filter(Attribute("id") + 10 > 10) -> filter(Attribute("id") > 0)
///  2) Arithmetic_simplification
///  Example: filter(Attribute("id") > Attribute("value") * 0) -> filter(Attribute("id") > 0)
///  3) Conjunction/Disjunction simplification
///  Example: filter(Attribute("id") > 0 && TRUE) -> filter(Attribute("id") > 0)
class RedundancyEliminationRule : public BaseRewriteRule
{
public:
    std::shared_ptr<QueryPlan> apply(std::shared_ptr<QueryPlan> queryPlan) override;

    static RedundancyEliminationRulePtr create();
    virtual ~RedundancyEliminationRule() = default;

private:
    explicit RedundancyEliminationRule();

    /// @brief Remove all possible redundancies by using constant folding, arithmetic simplification and conjunction/disjunction
    /// simplification
    /// @param predicate
    /// @return updated predicate
    static std::shared_ptr<LogicalFunction> eliminatePredicateRedundancy(const std::shared_ptr<LogicalFunction>& predicate);

    /// @note Currently not implemented
    /// @brief Move all constants to the same side of the expression (e.g. a-100 >= 300 becomes a >= 400)
    /// @param predicate
    /// @return updated predicate
    static std::shared_ptr<LogicalFunction> constantMoving(const std::shared_ptr<LogicalFunction>& predicate);

    /// @note Already done by the compiler
    /// @brief Functions involving only constants are folded into a single constant (e.g. 2 + 2 becomes 4, 2 = 2 becomes True)
    /// @param predicate
    /// @return updated predicate
    static std::shared_ptr<LogicalFunction> constantFolding(const std::shared_ptr<LogicalFunction>& predicate);

    /// @note Already done by the compiler
    /// @brief This rule applies arithmetic expressions to which the answer is known (e.g. a * 0 becomes 0, a + 0 becomes a)
    /// @param predicate
    /// @return updated predicate
    static std::shared_ptr<LogicalFunction> arithmeticSimplification(const std::shared_ptr<LogicalFunction>& predicate);

    /// @note Currently not implemented, since boolean values in the queries are not supported
    /// @brief FALSE in AND operation, the result of the expression is FALSE
    ///        TRUE in AND operation, expression can be omitted
    ///        FALSE in OR operation, expression can be omitted
    ///        TRUE in OR operation, the result of the expression is TRUE
    /// @param predicate
    /// @return updated predicate
    static std::shared_ptr<LogicalFunction> conjunctionDisjunctionSimplification(const std::shared_ptr<LogicalFunction>& predicate);
};

}
*/