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

#ifndef NES_QUERYMERGERPHASE_HPP
#define NES_QUERYMERGERPHASE_HPP

#include <memory>

namespace NES {

class QueryMergerPhase;
typedef std::shared_ptr<QueryMergerPhase> QueryMergerPhasePtr;

class GlobalQueryPlan;
typedef std::shared_ptr<GlobalQueryPlan> GlobalQueryPlanPtr;

class L0QueryMergerRule;
typedef std::shared_ptr<L0QueryMergerRule> L0QueryMergerRulePtr;

class QueryMergerPhase {

  public:
    static QueryMergerPhasePtr create();

    /**
     * @brief execute method to apply different query merger rules on the global query plan.
     * @param globalQueryPlan: the global query plan
     * @return true if successful
     */
    bool execute(GlobalQueryPlanPtr globalQueryPlan);

  private:
    explicit QueryMergerPhase();
    L0QueryMergerRulePtr l0QueryMergerRule;
};
}// namespace NES
#endif//NES_QUERYMERGERPHASE_HPP
