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

#include <REST/DTOs/ErrorResponse.hpp>
#include <REST/Handlers/ErrorHandler.hpp>

namespace NES {

ErrorHandler::ErrorHandler(const std::shared_ptr<oatpp::data::mapping::ObjectMapper>& objectMapper)
    : m_objectMapper(objectMapper) {}

std::shared_ptr<ErrorHandler::OutgoingResponse>
ErrorHandler::handleError(const Status& status, const oatpp::String& message, const Headers& headers) {
    auto error = REST::DTO::ErrorResponse::createShared();
    error->status = "ERROR";
    error->code = status.code;
    error->message = message;
    auto response = ResponseFactory::createResponse(status, error, m_objectMapper);
    for (const auto& pair : headers.getAll()) {
        response->putHeader(pair.first.toString(), pair.second.toString());
    }
    return response;
}

}// namespace NES
