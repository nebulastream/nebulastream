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
#ifndef NES_CORE_INCLUDE_EXCEPTIONS_REQUESTEXECUTIONEXCEPTION_HPP_
#define NES_CORE_INCLUDE_EXCEPTIONS_REQUESTEXECUTIONEXCEPTION_HPP_

#include <Common/Identifiers.hpp>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>

namespace NES::Exceptions {

/**
 * @brief This is the base class for exceptions thrown during the execution of coordinator side requests which
 * indicate an error that possibly require a rollback or other kinds of specific error handling
 */
class RequestExecutionException : public std::enable_shared_from_this<RequestExecutionException>, public std::runtime_error {
  public:
    explicit RequestExecutionException(const std::string& message);
    explicit RequestExecutionException(QueryId queryId, const std::string& message);

    /**
     * @brief Checks if this object is of type ExceptionType
     * @tparam ExceptionType: a subclass ob RequestExecutionException
     * @return bool true if object is of type ExceptionType
     */
    template<class ExceptionType>
    bool instanceOf() {
        if (dynamic_cast<ExceptionType*>(this)) {
            return true;
        }
        return false;
    };

    /**
    * @brief Dynamically casts the exception to the given type
    * @tparam ExceptionType: a subclass ob RequestExecutionException
    * @return returns a shared pointer of the given type
    */
    template<class ExceptionType>
    std::shared_ptr<ExceptionType> as() {
        if (instanceOf<ExceptionType>()) {
            return std::dynamic_pointer_cast<ExceptionType>(this->shared_from_this());
        }
        throw std::logic_error("Exception:: we performed an invalid cast of exception");
    }

    QueryId getQueryId() const;

  private:
    QueryId queryId;
};
}// namespace NES::Exceptions

#endif// NES_CORE_INCLUDE_EXCEPTIONS_REQUESTEXECUTIONEXCEPTION_HPP_
