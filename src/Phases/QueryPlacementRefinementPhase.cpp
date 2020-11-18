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

#include <Phases/QueryPlacementRefinementPhase.hpp>
#include <Util/Logger.hpp>
namespace NES {

QueryPlacementRefinementPhasePtr QueryPlacementRefinementPhase::create(GlobalExecutionPlanPtr globalPlan) {
    return std::make_shared<QueryPlacementRefinementPhase>(QueryPlacementRefinementPhase(globalPlan));
}

QueryPlacementRefinementPhase::~QueryPlacementRefinementPhase() { NES_DEBUG("~QueryPlacementRefinementPhase()"); }
QueryPlacementRefinementPhase::QueryPlacementRefinementPhase(GlobalExecutionPlanPtr globalPlan) {
    NES_DEBUG("QueryPlacementRefinementPhase()");
    globalExecutionPlan = globalPlan;
}

bool QueryPlacementRefinementPhase::execute(QueryId queryId) {
    NES_DEBUG("QueryPlacementRefinementPhase() execute for query " << queryId);
    return true;
}

}// namespace NES