#include "Exceptions/InvalidQueryException.hpp"

namespace NES {
InvalidQueryException::InvalidQueryException(std::string message) : std::runtime_error(message) {}
}// namespace NES