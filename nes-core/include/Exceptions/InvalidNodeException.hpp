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

#ifndef NES_CORE_INCLUDE_EXCEPTIONS_INVALIDNODEEXCEPTION_HPP_
#define NES_CORE_INCLUDE_EXCEPTIONS_INVALIDNODEEXCEPTION_HPP_

#include <Exceptions/RequestExecutionException.hpp>
#include <stdexcept>
#include <string>

namespace NES::Exceptions {
/**
 * @brief This Exception is thrown if we obtain an invalid node, e.g. nullptr, or cannot find a node
 */
class InvalidNodeException : public RequestExecutionException {
  public:
    explicit InvalidNodeException(const std::string& message);
};
}// namespace NES

#endif// NES_CORE_INCLUDE_EXCEPTIONS_INVALIDQUERYEXCEPTION_HPP_
