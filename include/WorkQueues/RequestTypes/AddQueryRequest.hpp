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

#ifndef NES_ADDQUERYREQUEST_HPP
#define NES_ADDQUERYREQUEST_HPP

#include <WorkQueues/RequestTypes/NESRequest.hpp>

namespace NES {

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class AddQueryRequest;
typedef std::shared_ptr<AddQueryRequest> AddQueryRequestPtr;

/**
 * @brief This request is used for running a new query in NES cluster
 */
class AddQueryRequest : public NESRequest {

    static AddQueryRequestPtr create(QueryPlanPtr queryPlan);

  private:
    explicit AddQueryRequest(QueryPlanPtr queryPlan);
    QueryPlanPtr queryPlan;
};
}// namespace NES

#endif//NES_ADDQUERYREQUEST_HPP
