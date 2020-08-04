#include <Exceptions/GlobalQueryPlanUpdateException.hpp>

namespace NES {
GlobalQueryPlanUpdateException::GlobalQueryPlanUpdateException(std::string message) : std::runtime_error(message) {}
}// namespace NES
