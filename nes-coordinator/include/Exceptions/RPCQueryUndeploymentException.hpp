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
#ifndef NES_COORDINATOR_INCLUDE_EXCEPTIONS_RPCQUERYUNDEPLOYMENTEXCEPTION_HPP_
#define NES_COORDINATOR_INCLUDE_EXCEPTIONS_RPCQUERYUNDEPLOYMENTEXCEPTION_HPP_
#include <Exceptions/RequestExecutionException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <vector>
namespace NES::Exceptions {

/**
 * @brief This exception indicates that an rpc to undeploy a query from a worker has failed
 */
//todo 3915: check if this class can be generalized to include also rpc failures during deployment
class RPCQueryUndeploymentException : public RequestExecutionException {
  public:
    /**
     * @brief construct an exception
     * @param message: a string containing a description of the error that occured
     * @param failedRpcExecutionNodeIds: the execution node ids of the workers that could not be reached vie rpc
     * @param mode: the mode indicating if the rpc was a register, unregister, start or stop operation
     */
    explicit RPCQueryUndeploymentException(const std::string& message,
                                           std::vector<WorkerId> failedRpcExecutionNodeIds,
                                           RpcClientMode mode);

    [[nodiscard]] const char* what() const noexcept override;

    /**
     * @brief get a list of the nodes that could not be reached
     * @return a vector of node ids
     */
    std::vector<WorkerId> getFailedExecutionNodeIds();

    /**
     * @brief get the mode of the failed operation
     * @return register, unregister, stop or start
     */
    RpcClientMode getMode();

  private:
    std::string message;
    std::vector<WorkerId> failedExecutionNodeIds;
    RpcClientMode mode;
};
}// namespace NES::Exceptions
#endif // NES_COORDINATOR_INCLUDE_EXCEPTIONS_RPCQUERYUNDEPLOYMENTEXCEPTION_HPP_
