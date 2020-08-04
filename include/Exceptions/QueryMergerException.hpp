#ifndef NES_QUERYMERGEREXCEPTION_HPP
#define NES_QUERYMERGEREXCEPTION_HPP

#include <stdexcept>

namespace NES {

/**
 * @brief This exception is thrown if some error occurred while performing query merge operation
 */
class QueryMergerException : public std::runtime_error {
  public:
    explicit QueryMergerException(std::string message);
};

}// namespace NES

#endif//NES_QUERYMERGEREXCEPTION_HPP
