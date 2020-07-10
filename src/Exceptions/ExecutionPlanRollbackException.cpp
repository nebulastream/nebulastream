#include "Exceptions/ExecutionPlanRollbackException.hpp"

namespace NES {
ExecutionPlanRollbackException::ExecutionPlanRollbackException(std::string message) : std::runtime_error(message) {}
}// namespace NES