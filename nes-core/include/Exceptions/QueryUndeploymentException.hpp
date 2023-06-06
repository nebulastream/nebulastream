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

#include <Common/Identifiers.hpp>
#include <Exceptions/RequestExecutionException.hpp>
#include <optional>
#include <stdexcept>
#include <string>

namespace NES {

/**
 * @brief this exception indicates an error during the undeployment of a reqeust
 */
class QueryUndeploymentException : public std::runtime_error, public RequestExecutionException {
  public:
    /**
     * @brief constructor
     * @param message message containing information about the error
     */
    explicit QueryUndeploymentException(const std::string& message);

    /**
     * @brief constructor
     * @param sharedQueryId the shared query id of the query that was being undeployed when the exception occurred
     * @param message message containing information about the error
     */
    QueryUndeploymentException(SharedQueryId sharedQueryId, const std::string& message);

    /**
     * @brief get the shared query id of the query that was to be undeployed, if it is known
     * @return: an optional containing the shared query id or nullopt if it was no supplies at request creation
     */
    std::optional<SharedQueryId> getSharedQueryId();

    const char* what() const noexcept override;

  private:
    std::optional<SharedQueryId> sharedQueryId;

};
}// namespace NES
#endif// NES_CORE_INCLUDE_EXCEPTIONS_QUERYUNDEPLOYMENTEXCEPTION_HPP_
