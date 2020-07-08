#ifndef NES_INVALIDQUERYEXCEPTION_HPP
#define NES_INVALIDQUERYEXCEPTION_HPP

#include <stdexcept>

namespace NES {
class InvalidQueryException : public std::exception {
  public:
    explicit InvalidQueryException(std::string message);

  private:
    std::string message;
};
}// namespace NES

#endif//NES_INVALIDQUERYEXCEPTION_HPP
