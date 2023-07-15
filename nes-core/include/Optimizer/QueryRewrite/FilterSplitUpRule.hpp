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

#ifndef NES_FILTERSPLITUPRULE_HPP
#define NES_FILTERSPLITUPRULE_HPP

#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>

namespace NES {

class Node;
using NodePtr = std::shared_ptr<Node>;

class FilterLogicalOperatorNode;
using FilterLogicalOperatorNodePtr = std::shared_ptr<FilterLogicalOperatorNode>;
}// namespace NES

namespace NES::Optimizer {

class FilterSplitUpRule;
using FilterSplitUpRulePtr = std::shared_ptr<FilterSplitUpRule>;

/**
 * @brief This class is responsible for altering the query plan to push down the filter as much as possible.
 * Following are the exceptions:
 *  1.) The Leaf node in the query plan will always be source node. This means the filter can't be push below a source node.
 *  2.) Every operator below a filter has it's own set of rules that decide if and how the filter can be pushed below that operator
 */
class FilterSplitUpRule : public BaseRewriteRule {
  public:
    QueryPlanPtr apply(QueryPlanPtr queryPlan) override;

    static FilterSplitUpRulePtr create();
    virtual ~FilterSplitUpRule() = default;

  private:
    explicit FilterSplitUpRule();

    void splitUpFilters(FilterLogicalOperatorNodePtr filterOperator);
};

}// namespace NES::Optimizer

#endif//NES_FILTERSPLITUPRULE_HPP
