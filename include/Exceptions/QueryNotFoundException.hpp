#ifndef NES_QUERYNOTFOUNDEXCEPTION_HPP
#define NES_QUERYNOTFOUNDEXCEPTION_HPP

#include <stdexcept>

namespace NES {
/**
 * @brief This exception is raised when the query you are looking for is not found
 */
class QueryNotFoundException : public std::runtime_error {
  public:
    explicit QueryNotFoundException(std::string message);
};
}// namespace NES
#endif//NES_QUERYNOTFOUNDEXCEPTION_HPP
