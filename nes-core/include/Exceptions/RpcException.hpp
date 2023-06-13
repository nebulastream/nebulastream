#ifndef NES_RPCEXCEPTION_HPP
#define NES_RPCEXCEPTION_HPP
#include <Exceptions/RequestExecutionException.hpp>
#include <Exceptions/RuntimeException.hpp>
#include <memory>
#include <GRPC/WorkerRPCClient.hpp>

namespace NES {
using CompletionQueuePtr = std::shared_ptr<grpc::CompletionQueue>;

namespace  Exceptions {
        struct RpcFailureInformation {
            CompletionQueuePtr completionQueue;
            uint64_t count;
        };
        class RpcException : public RequestExecutionException {
          public:
            explicit RpcException(std::string message, std::vector<RpcFailureInformation> failedRpcs, RpcClientModes mode);

            [[nodiscard]] const char* what() const noexcept override;

            std::vector<RpcFailureInformation> getFailedRpcNodeIds();

            RpcClientModes getMode();

          private:
            std::string message;
            std::vector<RpcFailureInformation> failedRpcs;
            RpcClientModes mode;
        };
    }
}
#endif//NES_RPCEXCEPTION_HPP
