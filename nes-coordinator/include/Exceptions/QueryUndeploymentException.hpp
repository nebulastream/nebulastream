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
#ifndef NES_CORE_INCLUDE_EXCEPTIONS_QUERYUNDEPLOYMENTEXCEPTION_HPP_
#define NES_CORE_INCLUDE_EXCEPTIONS_QUERYUNDEPLOYMENTEXCEPTION_HPP_

#include <Exceptions/RequestExecutionException.hpp>
#include <Identifiers.hpp>
#include <optional>
#include <stdexcept>
#include <string>

namespace NES::Exceptions {

/**
 * @brief this exception indicates an error during the undeployment of a query
 */
class QueryUndeploymentException : public RequestExecutionException {
  public:
    /**
     * @brief constructor
     * @param sharedQueryId the shared query id of the query that was being undeployed when the exception occurred
     * @param message message containing information about the error
     */
    QueryUndeploymentException(SharedQueryId sharedQueryId, const std::string& message);

    [[nodiscard]] const char* what() const noexcept override;
};
}// namespace NES::Exceptions
#endif// NES_CORE_INCLUDE_EXCEPTIONS_QUERYUNDEPLOYMENTEXCEPTION_HPP_
