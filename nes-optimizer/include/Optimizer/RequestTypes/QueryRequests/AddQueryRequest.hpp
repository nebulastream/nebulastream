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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_REQUESTTYPES_QUERYREQUESTS_ADDQUERYREQUEST_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_REQUESTTYPES_QUERYREQUESTS_ADDQUERYREQUEST_HPP_

#include <Util/Placement/PlacementStrategy.hpp>
#include  <Optimizer/RequestTypes/Request.hpp>
#include <future>

namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class AddQueryRequest;
using AddQueryRequestPtr = std::shared_ptr<AddQueryRequest>;

/**
 * @brief This request is used for running a new query in NES cluster
 */
class AddQueryRequest : public Request {
  public:
    /**
     * @brief Create instance of RunQueryRequest
     * @param queryPlan : the query plan to be run
     * @param queryPlacementStrategy: the placement strategy name
     * @return shared pointer to the instance of Run query request
     */
    static AddQueryRequestPtr create(QueryPlanPtr queryPlan, Optimizer::PlacementStrategy queryPlacementStrategy);

    /// Virtual destructor for inheritance
    /**
     * @brief Get the query plan to run
     * @return pointer to the query plan to run
     */
    QueryPlanPtr getQueryPlan();

    /**
     * @brief Get query placement strategy
     * @return query placement strategy
     */
    Optimizer::PlacementStrategy getQueryPlacementStrategy();

    uint64_t getQueryId();

    std::string toString() override;

    RequestType getRequestType() override;

  private:
    explicit AddQueryRequest(const QueryPlanPtr& queryPlan, Optimizer::PlacementStrategy queryPlacementStrategy);
    QueryPlanPtr queryPlan;
    Optimizer::PlacementStrategy queryPlacementStrategy;
};
}// namespace NES

#endif // NES_OPTIMIZER_INCLUDE_OPTIMIZER_REQUESTTYPES_QUERYREQUESTS_ADDQUERYREQUEST_HPP_
