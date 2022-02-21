#ifndef NES_INVALIDCOORDINATEFORMATEXCEPTION_H
#define NES_INVALIDCOORDINATEFORMATEXCEPTION_H
#include <exception>

namespace NES {

/**
 * @brief an exception indicating that the entered string is not of the format "<double>, <double>"
 */
class InvalidCoordinateFormatException : public std::exception {
    const char* what () const noexcept;
};

}// namespase NES
#endif//NES_INVALIDCOORDINATEFORMATEXCEPTION_H
