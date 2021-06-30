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
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <sstream>

namespace NES {

QueryReconfigurationPlan::QueryReconfigurationPlan(const QueryReconfigurationId reconfigurationId,
                                                   const QueryId queryId,
                                                   const QueryReconfigurationTypes reconfigurationType,
                                                   const QuerySubPlanId oldQuerySubPlanId,
                                                   const QuerySubPlanId newQuerySubPlanId)
    : id(reconfigurationId), queryId(queryId), reconfigurationType(reconfigurationType), oldQuerySubPlanId(oldQuerySubPlanId),
      newQuerySubPlanId(newQuerySubPlanId) {}

QueryReconfigurationPlanPtr QueryReconfigurationPlan::create(const QueryId queryId,
                                                             const QueryReconfigurationTypes reconfigurationType,
                                                             const QuerySubPlanId oldQuerySubPlanId,
                                                             const QuerySubPlanId newQuerySubPlanId) {
    return std::make_shared<QueryReconfigurationPlan>(PlanIdGenerator::getNextQueryReconfigurationPlanId(),
                                                      queryId,
                                                      reconfigurationType,
                                                      oldQuerySubPlanId,
                                                      newQuerySubPlanId);
}

QueryReconfigurationId QueryReconfigurationPlan::getId() const { return id; }

QueryId QueryReconfigurationPlan::getQueryId() const { return queryId; }

QueryReconfigurationTypes QueryReconfigurationPlan::getReconfigurationType() const { return reconfigurationType; }

QuerySubPlanId QueryReconfigurationPlan::getNewQuerySubPlanId() const { return newQuerySubPlanId; }

QuerySubPlanId QueryReconfigurationPlan::getOldQuerySubPlanId() const { return oldQuerySubPlanId; }

}// namespace NES