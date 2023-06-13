#ifndef NES_NOSUCHSQPINGLOBALQUERYPLANEXCEPTION_H
#define NES_NOSUCHSQPINGLOBALQUERYPLANEXCEPTION_H
#include <Exceptions/RequestExecutionException.hpp>
#include <Common/Identifiers.hpp>

namespace NES::Exceptions {

/**
 * @brief This exception indicates, that no shared query plan with the given id could be found
 */
class NoSuchSQPInGlobalQueryPlanException : public RequestExecutionException {
    NoSuchSQPInGlobalQueryPlanException(std::string message, SharedQueryId id);
  private:
    std::string message;
    SharedQueryId id;
};

}

#endif//NES_NOSUCHSQPINGLOBALQUERYPLANEXCEPTION_H
