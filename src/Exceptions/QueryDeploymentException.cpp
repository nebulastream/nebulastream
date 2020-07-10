#include <Exceptions/QueryDeploymentException.hpp>

namespace NES {
QueryDeploymentException::QueryDeploymentException(std::string message) : std::runtime_error(message) {}
}// namespace NES
