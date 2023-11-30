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
#ifndef NES_COORDINATOR_INCLUDE_EXCEPTIONS_EXECUTIONNODENOTFOUNDEXCEPTION_HPP_
#define NES_COORDINATOR_INCLUDE_EXCEPTIONS_EXECUTIONNODENOTFOUNDEXCEPTION_HPP_

#include <Exceptions/RequestExecutionException.hpp>
#include <Identifiers.hpp>
namespace NES::Exceptions {

/**
 * @brief This exception indicates, that a lookup for an execution node in the global execution plan failed
 */
class ExecutionNodeNotFoundException : public RequestExecutionException {
  public:
    /**
     * @brief construct an exception containing a human readable message
     * @param message: A string to indicate to the user what caused the exception
     */
    explicit ExecutionNodeNotFoundException(const std::string& message);

    /**
     * @brief construct an exception containing a human readable message and a node id
     * @param message: A string to indicate to the user what caused the exception
     * @param id: the id of the execution node that was looked up but not found (execution node ids correspond to topology
     * node ids and therefore have the same type)
     */
    ExecutionNodeNotFoundException(const std::string& message, TopologyNodeId id);

    /**
     * @brief get the id of the execution node that could not be found
     */
    [[nodiscard]] NodeId getNodeId() const;

  private:
    TopologyNodeId id;
};
}// namespace NES::Exceptions
#endif  // NES_COORDINATOR_INCLUDE_EXCEPTIONS_EXECUTIONNODENOTFOUNDEXCEPTION_HPP_
