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

#ifndef NES_INCLUDE_WORK_QUEUES_REQUEST_TYPES_STOP_QUERY_REQUEST_HPP_
#define NES_INCLUDE_WORK_QUEUES_REQUEST_TYPES_STOP_QUERY_REQUEST_HPP_

#include <WorkQueues/RequestTypes/NESRequest.hpp>

namespace NES {

class StopQueryRequest;
using StopQueryRequestPtr = std::shared_ptr<StopQueryRequest>;

/**
 * @brief This request is used for stopping a running query in NES cluster
 */
class StopQueryRequest : public NESRequest {

  public:
    /**
     * @brief Create instance of  StopQueryRequest
     * @param queryId : the id of query to be stopped
     * @return shared pointer to the instance of stop query request
     */
    static StopQueryRequestPtr create(QueryId queryId);

    std::string toString() override;

  private:
    explicit StopQueryRequest(QueryId queryId);
};
}// namespace NES

#endif// NES_INCLUDE_WORK_QUEUES_REQUEST_TYPES_STOP_QUERY_REQUEST_HPP_
