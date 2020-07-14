#ifndef NES_INVALIDQUERYEXCEPTION_HPP
#define NES_INVALIDQUERYEXCEPTION_HPP

#include <stdexcept>

namespace NES {
/**
 * @brief This Exception is thrown if the query is found to have invalid format or has a logical error.
 */
class InvalidQueryException : public std::runtime_error {
  public:
    explicit InvalidQueryException(std::string message);
};
}// namespace NES

#endif//NES_INVALIDQUERYEXCEPTION_HPP
