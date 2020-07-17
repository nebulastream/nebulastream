#include "Exceptions/InvalidArgumentException.hpp"
#include <cstring>

namespace NES {

InvalidArgumentException::InvalidArgumentException(std::string name, std::string value) {
    message = "Received invalid value " + value + " for input argument " + name;
}

const char* InvalidArgumentException::what() const noexcept {
    return message.c_str();
}
}// namespace NES