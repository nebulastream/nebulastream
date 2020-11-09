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

#ifndef NES_GLOBALQUERYPLANUPDATEPHASE_HPP
#define NES_GLOBALQUERYPLANUPDATEPHASE_HPP

#include <memory>
#include <vector>

namespace NES {

class QueryCatalogEntry;

class GlobalQueryPlan;
typedef std::shared_ptr<GlobalQueryPlan> GlobalQueryPlanPtr;

class GlobalQueryPlanUpdatePhase;
typedef std::shared_ptr<GlobalQueryPlanUpdatePhase> GlobalQueryPlanUpdatePhasePtr;

/**
 * @brief This class is responsible for accepting a batch of query requests and then updating the Global Query Plan accordingly.
 */
class GlobalQueryPlanUpdatePhase {
  public:
    /**
     * @brief Create an instance of the GlobalQueryPlanUpdatePhase
     * @param globalQueryPlan: the input global query plan
     * @return Shared pointer for the GlobalQueryPlanUpdatePhase
     */
    static GlobalQueryPlanUpdatePhasePtr create(GlobalQueryPlanPtr globalQueryPlan);

    /**
     * @brief This method executes the Global Query Plan Update Phase on a batch of query requests
     * @param queryRequests: a batch of query requests (in the form of Query Catalog Entry) to be processed to update global query plan
     * @return Shared pointer to the Global Query Plan for further processing
     */
    GlobalQueryPlanPtr execute(const std::vector<QueryCatalogEntry> queryRequests);

  private:
    explicit GlobalQueryPlanUpdatePhase(GlobalQueryPlanPtr globalQueryPlan);

    GlobalQueryPlanPtr globalQueryPlan;
};
}// namespace NES

#endif//NES_GLOBALQUERYPLANUPDATEPHASE_HPP
