#include <Exceptions/QueryUndeploymentSendRpcException.hpp>
NES::Exceptions::QueryUndeploymentSendRpcException::QueryUndeploymentSendRpcException(std::vector<uint64_t> completed,
                                                                      std::vector<uint64_t> failed, RpcClientModes mode) : completed(completed), failed(failed), mode(mode) {}
std::vector<uint64_t> NES::Exceptions::QueryUndeploymentSendRpcException::getCompletedExecutionNodeIds() {
    return completed;
}

std::vector<uint64_t> NES::Exceptions::QueryUndeploymentSendRpcException::getFailedExecutionNodeIds() { return failed; }

NES::RpcClientModes NES::Exceptions::QueryUndeploymentSendRpcException::getMode() { return mode; }
