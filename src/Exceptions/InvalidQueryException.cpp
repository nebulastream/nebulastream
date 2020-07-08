#include "Exceptions/InvalidQueryException.hpp"

using namespace NES;

InvalidQueryException::InvalidQueryException(std::string message): message(message) {}
