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
#ifndef NES_BASEREQUESTEXECUTIONEXCEPTION_HPP
#define NES_BASEREQUESTEXECUTIONEXCEPTION_HPP

#include <exception>
#include <string>

namespace NES {

class BaseRequestExecutionException : public std::exception {

  public:
    /**
     * @brief Checks if this object is of type ExceptionType
     * @tparam ExceptionType: a subclass ob BaseRequestExecutionException
     * @return bool true if object is of type ExceptionType
     */
    template<class ExceptionType>
    bool instanceOf() {
        if (dynamic_cast<ExceptionType*>(this)) {
            return true;
        };
        return false;
    };

    //[[nodiscard]] const char* what() const noexcept override;
};

}

#endif//NES_BASEREQUESTEXECUTIONEXCEPTION_HPP
