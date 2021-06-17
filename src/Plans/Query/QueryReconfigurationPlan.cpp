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

#include <Plans/Query/QueryReconfigurationPlan.hpp>
#include <algorithm>
#include <set>
#include <utility>

namespace NES {

QueryReconfigurationPlan::QueryReconfigurationPlan(std::vector<QuerySubPlanId> querySubPlansToStart,
                                                   std::map<QuerySubPlanId, QuerySubPlanId> querySubPlansIdToReplace,
                                                   std::vector<QuerySubPlanId> querySubPlansToStop)
    : querySubPlansToStart(querySubPlansToStart), querySubPlansIdToReplace(querySubPlansIdToReplace),
      querySubPlansToStop(querySubPlansToStop) {
    // nop
}

const std::vector<QuerySubPlanId> QueryReconfigurationPlan::getQuerySubPlansToStart() { return querySubPlansToStart; }
const std::map<QuerySubPlanId, QuerySubPlanId> QueryReconfigurationPlan::getQuerySubPlansIdToReplace() {
    return querySubPlansIdToReplace;
}

const std::vector<QuerySubPlanId> QueryReconfigurationPlan::getQuerySubPlansToStop() { return querySubPlansToStop; }

QueryReconfigurationPlanPtr
QueryReconfigurationPlan::create(Network::Messages::QueryReconfigurationMessage queryReconfigurationMessage) {
    return std::make_shared<QueryReconfigurationPlan>(queryReconfigurationMessage.getQuerySubPlansToStart(),
                                                      queryReconfigurationMessage.getQuerySubPlansIdToReplace(),
                                                      queryReconfigurationMessage.getQuerySubPlansToStop());
}
QueryReconfigurationPlanPtr QueryReconfigurationPlan::create(std::vector<QuerySubPlanId> querySubPlansToStart,
                                                             std::map<QuerySubPlanId, QuerySubPlanId> querySubPlansIdToReplace,
                                                             std::vector<QuerySubPlanId> querySubPlansToStop) {
    return std::make_shared<QueryReconfigurationPlan>(querySubPlansToStart, querySubPlansIdToReplace, querySubPlansToStop);
}
}// namespace NES