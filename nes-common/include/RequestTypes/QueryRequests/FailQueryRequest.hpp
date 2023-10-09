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

#ifndef NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_QUERYREQUESTS_FAILQUERYREQUEST_HPP_
#define NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_QUERYREQUESTS_FAILQUERYREQUEST_HPP_

#include <Common/Identifiers.hpp>
#include <RequestTypes/Request.hpp>
#include <memory>
#include <string>

namespace NES {

class FailQueryRequest;
using FailQueryRequestPtr = std::shared_ptr<FailQueryRequest>;
class FailQueryRequest : public Request {

  public:
    /**
     * @brief Create instance of  FailQueryRequest
     * @param sharedQueryId : the id of the shared query plan to fail
     * @param failureReason: reason for query failure
     * @return shared pointer to the instance of fail query request
     */
    static FailQueryRequestPtr create(SharedQueryId sharedQueryId, std::string failureReason);

    std::string getFailureReason();

    SharedQueryId getSharedQueryId() const;

    std::string toString() override;

    RequestType getRequestType() override;

  private:
    explicit FailQueryRequest(SharedQueryId sharedQueryId, std::string failureReason);

    SharedQueryId sharedQueryId;
    std::string failureReason;
};

}// namespace NES
#endif// NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_QUERYREQUESTS_FAILQUERYREQUEST_HPP_
