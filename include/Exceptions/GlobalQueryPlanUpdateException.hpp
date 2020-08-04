#ifndef NES_GLOBALQUERYPLANUPDATEEXCEPTION_HPP
#define NES_GLOBALQUERYPLANUPDATEEXCEPTION_HPP

#include <stdexcept>

namespace NES {

/**
 * @brief This exception is thrown if some error occurred while performing update on Global query plan
 */
class GlobalQueryPlanUpdateException : public std::runtime_error {
  public:
    explicit GlobalQueryPlanUpdateException(std::string message);
};

}// namespace NES

#endif//NES_GLOBALQUERYPLANUPDATEEXCEPTION_HPP
