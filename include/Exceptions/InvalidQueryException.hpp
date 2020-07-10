#ifndef NES_INVALIDQUERYEXCEPTION_HPP
#define NES_INVALIDQUERYEXCEPTION_HPP

#include <stdexcept>

namespace NES {
class InvalidQueryException : public std::runtime_error {
  public:
    explicit InvalidQueryException(std::string message);
};
}// namespace NES

#endif//NES_INVALIDQUERYEXCEPTION_HPP
