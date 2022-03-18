/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include "Exceptions/InvalidQueryStatusException.hpp"
#include <sstream>

namespace NES {

InvalidQueryStatusException::InvalidQueryStatusException(const std::vector<QueryStatus::Value>& expectedStatuses,
                                                         QueryStatus::Value actualStatus) {

    std::stringstream expectedStatus;
    for (QueryStatus::Value status : expectedStatuses) {
        expectedStatus << QueryStatus::toString(status) << " ";
    }
    message = "InvalidQueryStatusException: Expected query to be in [" + expectedStatus.str() + "] but found to be in "
        + QueryStatus::toString(actualStatus);
}

const char* InvalidQueryStatusException::what() const noexcept { return message.c_str(); }

}// namespace NES