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

#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPAddQueryEvent.hpp>

namespace NES::RequestProcessor {

ISQPAddQueryEvent::ISQPAddQueryEvent(const QueryPlanPtr& queryPlan,
                                     Optimizer::PlacementStrategy placementStrategy,
                                     FaultToleranceType faultTolerance)
    : ISQPEvent(ISQP_ADD_QUERY_EVENT_PRIORITY), queryPlan(queryPlan), placementStrategy(placementStrategy),
      faultToleranceType(faultTolerance) {}

ISQPEventPtr ISQPAddQueryEvent::create(const NES::QueryPlanPtr& queryPlan,
                                       Optimizer::PlacementStrategy placementStrategy,
                                       FaultToleranceType faultTolerance) {
    return std::make_shared<ISQPAddQueryEvent>(queryPlan, placementStrategy, faultTolerance);
}

const QueryPlanPtr& ISQPAddQueryEvent::getQueryPlan() const { return queryPlan; }

Optimizer::PlacementStrategy ISQPAddQueryEvent::getPlacementStrategy() const { return placementStrategy; }

FaultToleranceType ISQPAddQueryEvent::getFaultTolerance() const { return faultToleranceType; }
}// namespace NES::RequestProcessor
