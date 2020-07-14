#include "Exceptions/QueryNotFoundException.hpp"

namespace NES {
QueryNotFoundException::QueryNotFoundException(std::string message) : std::runtime_error(message) {}
}
