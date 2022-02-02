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

#ifndef NES_INCLUDE_EXCEPTIONS_UDFEXCEPTION_HPP_
#define NES_INCLUDE_EXCEPTIONS_UDFEXCEPTION_HPP_

#include <Exceptions/RuntimeException.hpp>

#include <string>

namespace NES {

/**
 * @brief This exception is thrown when an error occurs during UDF processing.
 */
class UdfException : public RuntimeException {
  public:
    /**
     * @brief Construct a UDF exception from a message and include the current stack trace.
     * @param message The exception message.
     */
    explicit UdfException(const std::string& message);
    /**
     * @brief Return the exception message without the stack trace.
     * @return The original exception message without the stack trace.
     *
     * RuntimeException automatically includes the stack trace at the time when the exception was constructed.
     * However, the error message is also returned to clients which submit UDFs over the REST API.
     * These clients should not receive the stack trace information.
     */
    [[nodiscard]] const std::string& getMessage() const { return message; }

  private:
    const std::string message;
};

}// namespace NES
#endif  // NES_INCLUDE_EXCEPTIONS_UDFEXCEPTION_HPP_
