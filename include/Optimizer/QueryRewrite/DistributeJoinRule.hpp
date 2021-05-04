/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_DistributeJoinRule_HPP
#define NES_DistributeJoinRule_HPP

#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>

namespace NES {
class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class DistributeJoinRule;
typedef std::shared_ptr<DistributeJoinRule> DistributeJoinRulePtr;

class WindowOperatorNode;
typedef std::shared_ptr<WindowOperatorNode> WindowOperatorNodePtr;

/**
 * @brief This rule currently only set the right number of join input edges

 */
class DistributeJoinRule : public BaseRewriteRule {
  public:
    static DistributeJoinRulePtr create();

    /**
     * @brief Apply Logical source expansion rule on input query plan
     * @param queryPlan: the original non-expanded query plan
     * @return expanded logical query plan
     */
    QueryPlanPtr apply(QueryPlanPtr queryPlan);

  private:
    explicit DistributeJoinRule();
};
}// namespace NES
#endif//NES_DistributeJoinRule_HPP
