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

#ifndef NES_NES_COMMON_INCLUDE_EXCEPTIONS_STATUS_HPP_
#define NES_NES_COMMON_INCLUDE_EXCEPTIONS_STATUS_HPP_
#include <Exceptions/RuntimeException.hpp>
#include <exception>
#include <optional>
namespace NES::Exceptions {

class Status {
  public:
    Status();
    Status(std::exception_ptr exception, std::string message = "Unknown error occurred!");
    Status(const std::exception exception);
    Status(const RuntimeException exception);
    bool isSuccessful();
    const std::string& getMessage() const;
    const std::string& getStacktrace() const;

  private:
    std::optional<std::exception_ptr> exception;
    const std::string message;
    const std::string stacktrace;
};

}// namespace NES::Exceptions

#endif//NES_NES_COMMON_INCLUDE_EXCEPTIONS_STATUS_HPP_
