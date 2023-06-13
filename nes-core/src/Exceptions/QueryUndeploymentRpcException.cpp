#include <Exceptions/QueryUndeploymentRpcException.hpp>
namespace NES::Exceptions {
QueryUndeploymentRpcException::QueryUndeploymentRpcException(std::string message, std::vector<uint64_t> failedRpcExecutionNodeIds, RpcClientModes mode) : message(message), failedExecutionNodeIds(failedRpcExecutionNodeIds), mode(mode) {}

const char* QueryUndeploymentRpcException::what() const noexcept { return message.c_str(); }

std::vector<uint64_t> QueryUndeploymentRpcException::getFailedExecutionNodeIds() { return failedExecutionNodeIds; }

RpcClientModes QueryUndeploymentRpcException::getMode() { return mode; }
}
