#ifndef NES_QUERYUNDEPLOYMENTRPCEXCEPTION_HPP
#define NES_QUERYUNDEPLOYMENTRPCEXCEPTION_HPP
#include <Exceptions/RequestExecutionException.hpp>
#include <vector>
#include <GRPC/WorkerRPCClient.hpp>
namespace NES::Exceptions {
class QueryUndeploymentRpcException : public RequestExecutionException {
  public:
    explicit QueryUndeploymentRpcException(std::string message, std::vector<uint64_t> failedRpcExecutionNodeIds, RpcClientModes mode);

    [[nodiscard]] const char * what() const noexcept override;

    std::vector<uint64_t> getFailedExecutionNodeIds();

    RpcClientModes getMode();

  private:
    std::string message;
    std::vector<uint64_t> failedExecutionNodeIds;
    RpcClientModes mode;
};
}// namespace NES::Exceptions
#endif
