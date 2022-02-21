#include "Exceptions/CoordinatesOutOfRangeException.h"

namespace NES {
const char* CoordinatesOutOfRangeException::what() const noexcept {
    return "Invalid latitude or longitude";
}
}// namespace NES
