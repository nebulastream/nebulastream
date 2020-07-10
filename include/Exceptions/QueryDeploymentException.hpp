#ifndef NES_QUERYDEPLOYMENTEXCEPTION_HPP
#define NES_QUERYDEPLOYMENTEXCEPTION_HPP

#include <stdexcept>

namespace NES {

class QueryDeploymentException : public std::runtime_error {

  public:
    explicit QueryDeploymentException(std::string message);

};
}
#endif//NES_QUERYDEPLOYMENTEXCEPTION_HPP
