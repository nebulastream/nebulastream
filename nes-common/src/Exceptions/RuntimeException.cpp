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

#include <Exceptions/RuntimeException.hpp>
#include <utility>
#include <Util/Logger.hpp>

namespace NES::Exceptions {

RuntimeException::RuntimeException(std::string msg, std::string&& stacktrace, const std::source_location location) : errorMessage(std::move(msg)) {
    auto spiLocation = spi::LocationInfo(location.file_name(), location.function_name(), location.line());
    NESLogger::getInstance()->error(errorMessage, spiLocation);
    errorMessage.append(":: callstack:\n");
    errorMessage.append(stacktrace);
}

RuntimeException::RuntimeException(std::string msg, const std::string& stacktrace) : errorMessage(std::move(msg)) {
    errorMessage.append(":: callstack:\n");
    errorMessage.append(stacktrace);
}

const char* RuntimeException::what() const noexcept { return errorMessage.c_str(); }

}// namespace NES