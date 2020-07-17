#ifndef NES_INCLUDE_EXCEPTIONS_EXECUTIONPLANROLLBACKEXCEPTION_HPP_
#define NES_INCLUDE_EXCEPTIONS_EXECUTIONPLANROLLBACKEXCEPTION_HPP_

#include <stdexcept>

namespace NES {

/**
 * @brief This exception represents an error occurred while reverting the changes in the QEP plan
 */
class ExecutionPlanRollbackException : public std::runtime_error {
  public:
    explicit ExecutionPlanRollbackException(std::string message);
};
}// namespace NES

#endif//NES_INCLUDE_EXCEPTIONS_EXECUTIONPLANROLLBACKEXCEPTION_HPP_
