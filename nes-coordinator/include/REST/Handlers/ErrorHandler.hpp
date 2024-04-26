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

#ifndef NES_COORDINATOR_INCLUDE_REST_HANDLERS_ERRORHANDLER_HPP_
#define NES_COORDINATOR_INCLUDE_REST_HANDLERS_ERRORHANDLER_HPP_

#include <oatpp/web/protocol/http/outgoing/ResponseFactory.hpp>
#include <oatpp/web/server/handler/ErrorHandler.hpp>

namespace NES {

class ErrorHandler : public oatpp::web::server::handler::ErrorHandler {
  private:
    typedef oatpp::web::protocol::http::outgoing::Response OutgoingResponse;
    typedef oatpp::web::protocol::http::Status Status;
    typedef oatpp::web::protocol::http::outgoing::ResponseFactory ResponseFactory;

  private:
    std::shared_ptr<oatpp::data::mapping::ObjectMapper> m_objectMapper;

  public:
    ErrorHandler(const std::shared_ptr<oatpp::data::mapping::ObjectMapper>& objectMapper);

    std::shared_ptr<OutgoingResponse>
    handleError(const Status& status, const oatpp::String& message, const Headers& headers = {}) override;
};
using ErrorHandlerPtr = std::shared_ptr<ErrorHandler>;
}// namespace NES
#endif // NES_COORDINATOR_INCLUDE_REST_HANDLERS_ERRORHANDLER_HPP_
