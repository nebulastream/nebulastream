#include <Exceptions/RpcException.hpp>
namespace NES::Exceptions {
RpcException::RpcException(std::string message, std::vector<RpcFailureInformation> failedRpcs, RpcClientModes mode) : message(message), failedRpcs(failedRpcs), mode(mode) {}

const char* RpcException::what() const noexcept { return message.c_str(); }
std::vector<RpcFailureInformation> RpcException::getFailedRpcNodeIds() { return failedRpcs; }
RpcClientModes RpcException::getMode() { return mode; }
};// namespace NES::Exceptions