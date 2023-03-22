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

#include <Exceptions/InvalidArgumentException.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/SharedQueryPlanStatus.hpp>

namespace NES {

std::string SharedQueryPlanStatus::toString(const Value queryStatus) {
    switch (queryStatus) {
        case Value::Created: return "CREATED";
        case Value::Deployed: return "DEPLOYED";
        case Value::Updated: return "UPDATED";
        case Value::Stopped: return "STOPPED";
        case Value::Failed: return "FAILED";
    }
}

SharedQueryPlanStatus::Value SharedQueryPlanStatus::getFromString(const std::string queryStatus) {
    if (queryStatus == "CREATED") {
        return Value::Created;
    } else if (queryStatus == "DEPLOYED") {
        return Value::Deployed;
    } else if (queryStatus == "UPDATED") {
        return Value::Updated;
    } else if (queryStatus == "STOPPED") {
        return Value::Stopped;
    } else if (queryStatus == "FAILED") {
        return Value::Failed;
    } else {
        NES_ERROR2("No valid query status to parse");
        throw InvalidArgumentException("status", queryStatus);
    }
}

}// namespace NES
