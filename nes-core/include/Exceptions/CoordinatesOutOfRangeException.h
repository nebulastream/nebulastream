//
// Created by x on 21.02.22.
//

#ifndef NES_COORDINATESOUTOFRANGEEXCEPTION_H
#define NES_COORDINATESOUTOFRANGEEXCEPTION_H

#include <exception>
namespace NES {

/**
 * @brief an exception indicating that the entered latitude is not in range [-90, 90] or the longitude is not in range [-180, 180]
 */
class CoordinatesOutOfRangeException : public std::exception {
    const char* what () const noexcept;
};
}

#endif//NES_COORDINATESOUTOFRANGEEXCEPTION_H
