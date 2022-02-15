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

#ifndef NES_INCLUDE_EXCEPTIONS_CONFIGURATIONEXCEPTION_HPP_
#define NES_INCLUDE_EXCEPTIONS_CONFIGURATIONEXCEPTION_HPP_

#include <Exceptions/NesRuntimeException.hpp>
#include <Util/Logger.hpp>
#include <string>
namespace NES {

/**
 * @brief This exception is thrown when an error occurs during configuration processing.
 */
class ConfigurationException : public NesRuntimeException {
  public:
    /**
     * @brief Construct a configuration exception from a message and include the current stack trace.
     * @param message The exception message.
     */
    explicit ConfigurationException(
        const std::string& message, std::string&& stacktrace= NES::Runtime::collectAndPrintStacktrace(), std::source_location location = std::source_location::current());
};

}// namespace NES
#endif// NES_INCLUDE_EXCEPTIONS_CONFIGURATIONEXCEPTION_HPP_
