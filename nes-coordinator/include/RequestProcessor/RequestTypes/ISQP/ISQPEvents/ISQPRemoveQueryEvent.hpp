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

#ifndef NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_ISQP_ISQPEVENTS_ISQPREMOVEQUERYEVENT_HPP_
#define NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_ISQP_ISQPEVENTS_ISQPREMOVEQUERYEVENT_HPP_

#include <Identifiers/Identifiers.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPEvent.hpp>

namespace NES::RequestProcessor {

/**
 * @brief the response indicating if the remove query was successfully applied
 */
struct ISQPRemoveQueryResponse : public ISQPResponse {
    explicit ISQPRemoveQueryResponse(bool success) : success(success){};
    bool success;
};
using ISQPRemoveQueryResponsePtr = std::shared_ptr<ISQPRemoveQueryResponse>;

class ISQPRemoveQueryEvent;
using ISQPRemoveQueryEventPtr = std::shared_ptr<ISQPRemoveQueryEvent>;

/**
 * @brief Representing the ISQP remove query event
 */
class ISQPRemoveQueryEvent : public ISQPEvent {

  public:
    static ISQPEventPtr create(const QueryId& queryId);

    ISQPRemoveQueryEvent(const QueryId& queryId);

    QueryId getQueryId() const;

  private:
    QueryId queryId;
};
}// namespace NES::RequestProcessor

#endif // NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_ISQP_ISQPEVENTS_ISQPREMOVEQUERYEVENT_HPP_
