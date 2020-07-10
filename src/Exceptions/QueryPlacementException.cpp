#include "Exceptions/QueryPlacementException.hpp"

namespace NES {

QueryPlacementException::QueryPlacementException(std::string message) : std::logic_error(message){}

}
