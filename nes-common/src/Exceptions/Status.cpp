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

#include <Exceptions/Status.hpp>

namespace NES::Exceptions {

Status::Status() {}

Status::Status(const RuntimeException exception)
    : exception(std::make_exception_ptr(exception)), message(exception.getMessage()), stacktrace(exception.getStacktrace()) {}

Status::Status(const std::exception exception) : exception(std::make_exception_ptr(exception)), message(exception.what()) {}

Status::Status(std::exception_ptr exception, std::string message)
    : exception(std::make_exception_ptr(exception)), message(message) {}

bool Status::isSuccessful() { return !exception.has_value(); }
const std::string& Status::getMessage() const { return message; }
const std::string& Status::getStacktrace() const { return stacktrace; }

}// namespace NES::Exceptions