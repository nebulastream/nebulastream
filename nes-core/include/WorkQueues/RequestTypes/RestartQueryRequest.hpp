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

#ifndef NES_INCLUDE_WORKQUEUES_REQUESTTYPES_RESTARTQUERYREQUEST_HPP_
#define NES_INCLUDE_WORKQUEUES_REQUESTTYPES_RESTARTQUERYREQUEST_HPP_

#include <WorkQueues/RequestTypes/Request.hpp>

namespace NES {

class RestartQueryRequest;
using RestartQueryRequestPtr = std::shared_ptr<RestartQueryRequest>;

/**
 * @brief This request restarts a query with provided id
 */
class RestartQueryRequest : public Request {

  public:
    /**
     * @brief Create instance of  RestartQueryRequest
     * @param queryId : the id of query to be restarted
     * @return shared pointer to the instance of restart query request
     */
    static RestartQueryRequestPtr create(QueryId queryId);

    ~RestartQueryRequest() override = default;

    std::string toString() override;

  private:
    explicit RestartQueryRequest(QueryId queryId);
};
}// namespace NES
#endif  // NES_INCLUDE_WORKQUEUES_REQUESTTYPES_RESTARTQUERYREQUEST_HPP_
