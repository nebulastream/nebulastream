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
#ifndef NES_EXECUTIONNODENOTFOUNDEXCEPTION_HPP
#define NES_EXECUTIONNODENOTFOUNDEXCEPTION_HPP

#include <Exceptions/QueryUndeploymentException.hpp>
#include <Common/Identifiers.hpp>
namespace NES {

/**
 * @brief This exception indicates, that a lookup for an execution node in the global execution plan failed
 */
class ExecutionNodeNotFoundException : public QueryUndeploymentException {
  public:
    explicit ExecutionNodeNotFoundException(std::string message);
    ExecutionNodeNotFoundException(std::string message, TopologyNodeId id);


    NodeId getNodeId();

  private:
    //execution node ids are equal to the topology node ids
    TopologyNodeId id;
};
}

#endif//NES_EXECUTIONNODENOTFOUNDEXCEPTION_HPP
