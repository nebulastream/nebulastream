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

#ifndef NES_INCLUDE_WORK_QUEUES_REQUEST_TYPES_NES_REQUEST_HPP_
#define NES_INCLUDE_WORK_QUEUES_REQUEST_TYPES_NES_REQUEST_HPP_

#include <Plans/Query/QueryId.hpp>
#include <exception>
#include <memory>
#include <stdexcept>

namespace NES {
/**
 * @brief This is the parent class for different type of requests handled by NES.
 */
class Request : public std::enable_shared_from_this<Request> {

  public:
    explicit Request(QueryId queryId);

    virtual ~Request() = default;

    /**
     * @brief Get the query id to stop
     * @return query id to stop
     */
    QueryId getQueryId() const;

    /**
     * @brief Checks if the current node is of type RequestType
     * @tparam RequestType
     * @return bool true if node is of RequestType
     */
    template<class RequestType>
    bool instanceOf() {
        if (dynamic_cast<RequestType*>(this)) {
            return true;
        };
        return false;
    };

    /**
     * @brief Dynamically casts the RequestType
     * @tparam RequestType
     * @return a shared pointer of the RequestType
     */
    template<class RequestType>
    std::shared_ptr<RequestType> as() {
        if (instanceOf<RequestType>()) {
            return std::dynamic_pointer_cast<RequestType>(this->shared_from_this());
        }
        throw std::logic_error("Request:: we performed an invalid cast of operator ");
    }

    virtual std::string toString() = 0;

  private:
    QueryId queryId;
};
}// namespace NES

#endif// NES_INCLUDE_WORK_QUEUES_REQUEST_TYPES_NES_REQUEST_HPP_
