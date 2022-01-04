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

#ifndef NES_INCLUDE_WORKQUEUES_REQUESTTYPES_RUNQUERYREQUEST_HPP_
#define NES_INCLUDE_WORKQUEUES_REQUESTTYPES_RUNQUERYREQUEST_HPP_

#include <WorkQueues/RequestTypes/Request.hpp>

namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class RunQueryRequest;
using RunQueryRequestPtr = std::shared_ptr<RunQueryRequest>;

/**
 * @brief This request is used for running a new query in NES cluster
 */
class RunQueryRequest : public Request {

  public:
    /**
     * @brief Create instance of RunQueryRequest
     * @param queryPlan : the query plan to be run
     * @param queryPlacementStrategy: the placement strategy name
     * @return shared pointer to the instance of Run query request
     */
    static RunQueryRequestPtr create(QueryPlanPtr queryPlan, std::string queryPlacementStrategy);

    /// Virtual destructor for inheritance
    virtual ~RunQueryRequest() = default;

    /**
     * @brief Get the query plan to run
     * @return pointer to the query plan to run
     */
    QueryPlanPtr getQueryPlan();

    /**
     * @brief Get query placement strategy
     * @return query placement strategy
     */
    std::string getQueryPlacementStrategy();

    std::string toString() override;

  private:
    explicit RunQueryRequest(const QueryPlanPtr& queryPlan, std::string queryPlacementStrategy);
    QueryPlanPtr queryPlan;
    std::string queryPlacementStrategy;
};
}// namespace NES

#endif  // NES_INCLUDE_WORKQUEUES_REQUESTTYPES_RUNQUERYREQUEST_HPP_
