#include <Exceptions/QueryMergerException.hpp>

namespace NES {
QueryMergerException::QueryMergerException(std::string message) : std::runtime_error(message) {}
}// namespace NES
