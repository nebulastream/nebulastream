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
#include <Operators/LogicalOperators/LogicalSelectionOperator.hpp>
#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>

namespace NES::Optimizer
{


/**
 * @brief This class is responsible for altering the query plan to split up each filter operator into as small parts as possible.
 *  1.) A filter with an andFunction as a predicate can be split up into two separate filters.
 *  2.) A filter with a negated OrFunction can be reformulated deMorgans rules and can be split up afterwards.
 */
class FilterSplitUpRule : public BaseRewriteRule
{
public:
    std::shared_ptr<QueryPlan> apply(std::shared_ptr<QueryPlan> queryPlan) override;

    static std::shared_ptr<FilterSplitUpRule> create();
    virtual ~FilterSplitUpRule() = default;

private:
    explicit FilterSplitUpRule();

    /**
     * If it is possible this method splits up a filterOperator into multiple filterOperators.
     * If our query plan contains a parentOperaters->filter(expression1 && expression2)->childOperator.
     * This plan gets rewritten to parentOperaters->filter(expression1)->filter(expression2)->childOperator. We will call splitUpFilters()
     * on the new flters as well
     * If our query plan contains a parentOperaters->filter(!(expression1 || expression2))->childOperator, we use deMorgan to
     * reformulate the predicate to an andFunction and call splitUpFilter on the Filter.
     * @param filterOperator the filter operator node that we want to split up
     */
    void splitUpFilters(const std::shared_ptr<LogicalSelectionOperator>& filterOperator);
};

}
