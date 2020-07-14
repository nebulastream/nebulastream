#ifndef NES_QUERYDEPLOYMENTEXCEPTION_HPP
#define NES_QUERYDEPLOYMENTEXCEPTION_HPP

#include <stdexcept>

namespace NES {

/**
 * @brief This exception is thrown if some error occurred while deploying the query
 */
class QueryDeploymentException : public std::runtime_error {

  public:
    explicit QueryDeploymentException(std::string message);

};
}
#endif//NES_QUERYDEPLOYMENTEXCEPTION_HPP
