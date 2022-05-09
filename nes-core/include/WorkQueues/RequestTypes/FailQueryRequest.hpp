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

#ifndef NES_FAILQUERYREQUEST_HPP
#define NES_FAILQUERYREQUEST_HPP

#include <WorkQueues/RequestTypes/Request.hpp>
#include <memory>

namespace NES {

class FailQueryRequest;
using FailQueryRequestPtr = std::shared_ptr<FailQueryRequest>;
class FailQueryRequest : public Request {

  public:
    /**
     * @brief Create instance of  FailQueryRequest
     * @param queryId : the id of query to fail
     * @return shared pointer to the instance of fail query request
     */
    static FailQueryRequestPtr create(QueryId queryId);

    std::string toString() override;

  private:
    explicit FailQueryRequest(QueryId queryId);
};

}// namespace NES
#endif//NES_FAILQUERYREQUEST_HPP
