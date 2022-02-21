#include "Exceptions/InvalidCoordinateFormatException.h"

namespace NES {
const char* InvalidCoordinateFormatException::what() const noexcept {
    return "The provided string is not of the format \"<lat>, <lng>\"";
}
}// namespace NES

