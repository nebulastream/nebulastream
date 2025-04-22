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

#ifndef NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_ISQP_ISQPEVENTS_ISQPADDQUERYEVENT_HPP_
#define NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_ISQP_ISQPEVENTS_ISQPADDQUERYEVENT_HPP_

#include <Identifiers/Identifiers.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPEvent.hpp>
#include <Util/CheckpointStorageType.hpp>
#include <Util/FaultToleranceType.hpp>
#include <Util/Placement/PlacementStrategy.hpp>

namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

namespace RequestProcessor {

/**
 * @brief the response indicating the id if the query after successful registration of the add query event
 */
struct ISQPAddQueryResponse : public ISQPResponse {
    explicit ISQPAddQueryResponse(QueryId queryId) : queryId(queryId){};
    QueryId queryId;
};
using ISQPAddQueryResponsePtr = std::shared_ptr<ISQPAddQueryResponse>;

class ISQPAddQueryEvent;
using ISQPAddQueryEventPtr = std::shared_ptr<ISQPAddQueryEvent>;

/**
 * @brief Representing the ISQP Add query event
 */
class ISQPAddQueryEvent : public ISQPEvent {

  public:
    static ISQPEventPtr create(const QueryPlanPtr& queryPlan,
                               Optimizer::PlacementStrategy placementStrategy,
                               FaultToleranceType faultTolerance = FaultToleranceType::NONE,
                               CheckpointStorageType checkpointStorage = CheckpointStorageType::NONE);

    ISQPAddQueryEvent(const QueryPlanPtr& queryPlan,
                      Optimizer::PlacementStrategy placementStrategy,
                      FaultToleranceType faultTolerance,
                      CheckpointStorageType checkpointStorage);

    const QueryPlanPtr& getQueryPlan() const;

    Optimizer::PlacementStrategy getPlacementStrategy() const;

    FaultToleranceType getFaultTolerance() const;

    CheckpointStorageType getCheckpointStorage() const;

  private:
    QueryPlanPtr queryPlan;
    Optimizer::PlacementStrategy placementStrategy;
    FaultToleranceType faultToleranceType;
    CheckpointStorageType checkpointStorageType;
};
}// namespace RequestProcessor
}// namespace NES
#endif// NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_ISQP_ISQPEVENTS_ISQPADDQUERYEVENT_HPP_
