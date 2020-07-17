#include "Exceptions/QueryPlacementException.hpp"

namespace NES {
QueryPlacementException::QueryPlacementException(std::string message) : std::runtime_error(message) {}
}// namespace NES
