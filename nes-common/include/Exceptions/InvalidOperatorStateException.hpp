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

#ifndef NES_CORE_INCLUDE_EXCEPTIONS_INVALIDOPERATORSTATEEXCEPTION_HPP_
#define NES_CORE_INCLUDE_EXCEPTIONS_INVALIDOPERATORSTATEEXCEPTION_HPP_

#include <Common/Identifiers.hpp>
#include <Exceptions/RequestExecutionException.hpp>
#include <Util/OperatorState.hpp>
#include <stdexcept>
#include <vector>

namespace NES::Exceptions {
/**
 * @brief Exception is raised when the operator is in an Invalid status
 */
class InvalidOperatorStateException : public RequestExecutionException {
  public:
    explicit InvalidOperatorStateException(OperatorId operatorId,
                                           const std::vector<OperatorState>& expectedState,
                                           OperatorState actualState);
    [[nodiscard]] const char* what() const noexcept override;

  private:
    std::string message;
};
}// namespace NES::Exceptions
#endif// NES_CORE_INCLUDE_EXCEPTIONS_INVALIDOPERATORSTATEEXCEPTION_HPP_
