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

#ifndef NES_INCLUDE_OPTIMIZER_QUERY_MERGER_BASE_QUERY_MERGER_RULE_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERY_MERGER_BASE_QUERY_MERGER_RULE_HPP_

#include <memory>

namespace NES {
class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;
}// namespace NES

namespace NES::Optimizer {
class BaseQueryMergerRule {

  public:
    /**
     * @brief apply the rule on Global Query Plan
     * @param globalQueryPlan: the global query plan
     */
    virtual bool apply(GlobalQueryPlanPtr globalQueryPlan) = 0;

    BaseQueryMergerRule() = default;

    BaseQueryMergerRule(const BaseQueryMergerRule&) = default;

    virtual ~BaseQueryMergerRule() = default;
};
using BaseQueryMergerRulePtr = std::shared_ptr<BaseQueryMergerRule>;
}// namespace NES::Optimizer
#endif  // NES_INCLUDE_OPTIMIZER_QUERY_MERGER_BASE_QUERY_MERGER_RULE_HPP_
