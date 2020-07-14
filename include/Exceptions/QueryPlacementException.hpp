#ifndef NES_QUERYPLACEMENTEXCEPTION_HPP
#define NES_QUERYPLACEMENTEXCEPTION_HPP

#include <stdexcept>

namespace NES {

/**
 * @brief Exception indicating problem during operator placement phase
 */
class QueryPlacementException : public std::runtime_error {

  public:
    explicit QueryPlacementException(std::string message);
};
}// namespace NES
#endif//NES_QUERYPLACEMENTEXCEPTION_HPP
