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
#include <Util/QueryStatus.hpp>

namespace NES {

std::string QueryStatus::toString(const Value queryStatus) {
    switch (queryStatus) {
        case Value::Registered: return "REGISTERED";
        case Value::Optimizing: return "OPTIMIZING";
        case Value::Deployed: return "DEPLOYED";
        case Value::Running: return "RUNNING";
        case Value::Stopped: return "STOPPED";
        case Value::MarkedForFailure: return "MARKED-FOR-FAILURE";
        case Value::Failed: return "FAILED";
        case Value::Restarting: return "RESTARTING";
        case Value::Migrating: return "MIGRATING";
        case Value::MarkedForHardStop: return "MARKED-FOR-HARD-STOP";
        case Value::MarkedForSoftStop: return "MARKED-FOR-SOFT-STOP";
        case Value::SoftStopTriggered: return "SOFT-STOP-TRIGGERED";
        case Value::SoftStopCompleted: return "SOFT-STOP-COMPLETED";
    }
}

QueryStatus::Value QueryStatus::getFromString(const std::string queryStatus) {
    if (queryStatus == "REGISTERED") {
        return Value::Registered;
    } else if (queryStatus == "OPTIMIZING") {
        return Value::Optimizing;
    } else if (queryStatus == "DEPLOYED") {
        return Value::Deployed;
    } else if (queryStatus == "RUNNING") {
        return Value::Running;
    } else if (queryStatus == "MARKED-FOR-HARD-STOP") {
        return Value::MarkedForHardStop;
    } else if (queryStatus == "STOPPED") {
        return Value::Stopped;
    } else if (queryStatus == "FAILED") {
        return Value::Failed;
    } else if (queryStatus == "MIGRATING") {
        return Value::Migrating;
    } else if (queryStatus == "SOFT-STOP-COMPLETED") {
        return Value::SoftStopCompleted;
    } else if (queryStatus == "SOFT-STOP-TRIGGERED") {
        return Value::SoftStopTriggered;
    } else if (queryStatus == "MARKED-FOR-SOFT-STOP") {
        return Value::MarkedForSoftStop;
    } else if (queryStatus == "MARKED-FOR-FAILURE") {
        return Value::MarkedForFailure;
    } else {
        NES_ERROR2("No valid query status to parse");
        throw InvalidArgumentException("status", queryStatus);
    }
}

}// namespace NES
