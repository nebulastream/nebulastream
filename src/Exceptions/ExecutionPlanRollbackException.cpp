#include "Exceptions/ExecutionPlanRollbackException.hpp"

using namespace NES;

ExecutionPlanRollbackException::ExecutionPlanRollbackException(std::string message) : message(message) {}