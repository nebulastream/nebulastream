#ifndef NES_INVALIDARGUMENTEXCEPTION_HPP
#define NES_INVALIDARGUMENTEXCEPTION_HPP

#include <stdexcept>

namespace NES {

/**
 * @brief This class is used for adding custom invalid argument exception.
 */
class InvalidArgumentException : public std::exception {
  public:
    explicit InvalidArgumentException(std::string message, std::string name, std::string value);

  private:
    std::string message;
    std::string name;
    std::string value;
};
}// namespace NES
#endif//NES_INVALIDARGUMENTEXCEPTION_HPP
