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

#ifndef NES_INCLUDE_OPTIMIZER_QUERYREWRITE_DISTRIBUTEJOINRULE_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERYREWRITE_DISTRIBUTEJOINRULE_HPP_

#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>

namespace NES {
class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class WindowOperatorNode;
using WindowOperatorNodePtr = std::shared_ptr<WindowOperatorNode>;

}// namespace NES

namespace NES::Optimizer {

class DistributeJoinRule;
using DistributeJoinRulePtr = std::shared_ptr<DistributeJoinRule>;

/**
 * @brief This rule currently only set the right number of join input edges
 */
class DistributeJoinRule : public BaseRewriteRule {
  public:
    static DistributeJoinRulePtr create();
    virtual ~DistributeJoinRule() = default;

    /**
     * @brief Apply Logical source expansion rule on input query plan
     * @param queryPlan: the original non-expanded query plan
     * @return expanded logical query plan
     */
    QueryPlanPtr apply(QueryPlanPtr queryPlan) override;

  private:
    explicit DistributeJoinRule();
};
}// namespace NES::Optimizer
#endif  // NES_INCLUDE_OPTIMIZER_QUERYREWRITE_DISTRIBUTEJOINRULE_HPP_
