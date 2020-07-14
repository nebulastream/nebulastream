#include <Exceptions/QueryUndeploymentException.hpp>

namespace NES {
QueryUndeploymentException::QueryUndeploymentException(std::string message) : std::runtime_error(message) {}
}// namespace NES