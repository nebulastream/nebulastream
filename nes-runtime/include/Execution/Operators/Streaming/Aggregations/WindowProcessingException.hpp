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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_WINDOWPROCESSINGEXCEPTION_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_WINDOWPROCESSINGEXCEPTION_HPP_

#include <Exceptions/RuntimeException.hpp>
#include <string>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This exception is thrown when an error occurs during window processing.
 */
class WindowProcessingException : public Exceptions::RuntimeException {
  public:
    /**
     * @brief Construct a WindowProcessingException exception from a message.
     * @param message The exception message.
     * @param location The location of this message.
     */
    explicit WindowProcessingException(const std::string& message,
                                       const std::source_location location = std::source_location::current());

  private:
    const std::string message;
};

}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_WINDOWPROCESSINGEXCEPTION_HPP_
