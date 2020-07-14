#include "Exceptions/InvalidQueryStatusException.hpp"
#include <sstream>

namespace NES {

InvalidQueryStatusException::InvalidQueryStatusException(std::vector<QueryStatus> expectedStatuses, QueryStatus actualStatus)
    : std::runtime_error(""), expectedStatuses(expectedStatuses), actualStatus(actualStatus) {}

const char* InvalidQueryStatusException::what() const throw() {
    std::stringstream expectedStatus;
    for (QueryStatus status : expectedStatuses) {
        expectedStatus << queryStatusToStringMap[status] << " ";
    }
    return ("InvalidQueryStatusException: Expected query to be in [" + expectedStatus.str() + "] but found to be in " + queryStatusToStringMap[actualStatus]).c_str();
}

}// namespace NES