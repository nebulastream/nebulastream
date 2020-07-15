#include "Exceptions/InvalidQueryStatusException.hpp"
#include <sstream>

namespace NES {

InvalidQueryStatusException::InvalidQueryStatusException(std::vector<QueryStatus> expectedStatuses, QueryStatus actualStatus) {

    std::stringstream expectedStatus;
    for (QueryStatus status : expectedStatuses) {
        expectedStatus << queryStatusToStringMap[status] << " ";
    }
    message = "InvalidQueryStatusException: Expected query to be in [" + expectedStatus.str() + "] but found to be in " + queryStatusToStringMap[actualStatus];
}

const char* InvalidQueryStatusException::what() const throw() {
    return message.c_str();
}

}// namespace NES