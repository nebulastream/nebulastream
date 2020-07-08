#include "Exceptions/InvalidArgumentException.hpp"

namespace NES {

InvalidArgumentException::InvalidArgumentException(std::string message, std::string name, std::string value)
    :message(message), name(name), value(value) {}

}