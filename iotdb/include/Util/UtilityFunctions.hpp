#ifndef FUNCTIONS_HPP
#define FUNCTIONS_HPP

#include <cmath>
#include <fstream>
#include <optional>

#include "caf/all.hpp"

/**
 * @brief a collection of shared utility functions
 */
namespace iotdb {

// removes leading and trailing whitespaces
std::string trim(std::string s);

}

#endif /* FUNCTIONS_HPP */
