#ifndef NES_QUERYUNDEPLOYMENTEXCEPTION_HPP
#define NES_QUERYUNDEPLOYMENTEXCEPTION_HPP

#include <stdexcept>

namespace NES {
class QueryUndeploymentException : public std::runtime_error {
  public:
    explicit QueryUndeploymentException(std::string message);
};
}// namespace NES
#endif//NES_QUERYUNDEPLOYMENTEXCEPTION_HPP
