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
#include <set>
#include <vector>
#include <Functions/LogicalFunction.hpp>
#include <Operators/LogicalOperators/LogicalSelectionOperator.hpp>
#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>

namespace NES
{
class LogicalSelectionOperator;
}

namespace NES::Optimizer
{

class FilterMergeRule;

/// @brief This rewrite rule identifies sequences of consecutive filters. The filters are combined together
/// into one filter where the predicate is a conjunction of the predicates of the original filters.
/// This reduces the number of operators in the query plan and should provide a performance benefit.
/// It is especially useful after splitting up and pushing down the filters.
/// Example:
///     |
///  Filter(a == 1)
///     |                             |
///  Filter(b > 0)          ===>   Filter((a == 1) && (b > 0) && (c < 2) && (a > 2)
///     |                             |
///  Filter(c < 2)
///     |
///  Filter(a > 2)
///     |
class FilterMergeRule : public BaseRewriteRule
{
public:
    static std::shared_ptr<FilterMergeRule> create();
    FilterMergeRule() = default;
    virtual ~FilterMergeRule() = default;

    /// @brief Apply Filter Merge rule on input query plan
    /// @param queryPlan: the original query plan
    /// @return updated logical query plan
    std::shared_ptr<QueryPlan> apply(std::shared_ptr<QueryPlan> queryPlan) override;

private:
    /// @brief Given a filter, retrieve all the consecutive filters (including the filter itself).
    /// @param firstFilter: the filter to check
    /// @return vector of filters
    static std::vector<std::shared_ptr<LogicalSelectionOperator>> getConsecutiveFilters(const std::shared_ptr<LogicalSelectionOperator>& firstFilter);
};
}
