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
#ifndef NES_COORDINATOR_INCLUDE_EXCEPTIONS_RPCEXCEPTION_HPP_
#define NES_COORDINATOR_INCLUDE_EXCEPTIONS_RPCEXCEPTION_HPP_
#include <Exceptions/RequestExecutionException.hpp>
#include <Exceptions/RuntimeException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <memory>

namespace NES {
using CompletionQueuePtr = std::shared_ptr<grpc::CompletionQueue>;

namespace Exceptions {
struct RpcFailureInformation {
    CompletionQueuePtr completionQueue;
    uint64_t count;
};

/**
 * @brief This exception indicates the failure of an rpc
 */
class RpcException : public RequestExecutionException {
  public:
    /**
     * @brief Constructor
     * @param message a human readable message describing the error
     * @param failedRpcs information about the failed rpcs containing pointers to the completion queues of the failed
     * calls and the count of performed calls
     * @param mode the mode of the rpc operation
     */
    explicit RpcException(const std::string& message, std::vector<RpcFailureInformation> failedRpcs, RpcClientModes mode);

    [[nodiscard]] const char* what() const noexcept override;

    /**
     * @brief get a list of the rpcs that failed
     * @return a list of structs containing a pointer to the completion queue and the count of operations performed in
     * that queue
     */
    std::vector<RpcFailureInformation> getFailedCalls();

    /**
     * @brief get the mode of the failed operation
     * @return register, unregister, stop or start
     */
    RpcClientModes getMode();

  private:
    std::string message;
    std::vector<RpcFailureInformation> failedRpcs;
    RpcClientModes mode;
};
}// namespace Exceptions
}// namespace NES
#endif// NES_COORDINATOR_INCLUDE_EXCEPTIONS_RPCEXCEPTION_HPP_
