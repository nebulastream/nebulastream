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

#include <Util/FaultToleranceType.hpp>
#include <string>

namespace NES {
FaultToleranceType stringToFaultToleranceTypeMap(const std::string faultToleranceMode) {
    if (faultToleranceMode == "NONE") {
        return FaultToleranceType::NONE;
    } else if (faultToleranceMode == "AT_MOST_ONCE") {
        return FaultToleranceType::AT_MOST_ONCE;
    } else if (faultToleranceMode == "AT_LEAST_ONCE") {
        return FaultToleranceType::AT_LEAST_ONCE;
    } else if (faultToleranceMode == "EXACTLY_ONCE") {
        return FaultToleranceType::EXACTLY_ONCE;
    } else if (faultToleranceMode == "ACTIVE_STANDBY"){
        return FaultToleranceType::ACTIVE_STANDBY;
    } else if (faultToleranceMode == "CHECKPOINTING"){
        return FaultToleranceType::CHECKPOINTING;
    } else if (faultToleranceMode == "UPSTREAM_BACKUP"){
        return FaultToleranceType::UPSTREAM_BACKUP;
    } else {
        return FaultToleranceType::INVALID;
    }
}

std::string toString(const FaultToleranceType faultToleranceMode) {
    switch (faultToleranceMode) {
        case FaultToleranceType::NONE: return "NONE";
        case FaultToleranceType::AT_MOST_ONCE: return "AT_MOST_ONCE";
        case FaultToleranceType::AT_LEAST_ONCE: return "AT_LEAST_ONCE";
        case FaultToleranceType::EXACTLY_ONCE: return "EXACTLY_ONCE";
        case FaultToleranceType::ACTIVE_STANDBY: return "ACTIVE_STANDBY";
        case FaultToleranceType::CHECKPOINTING: return "CHECKPOINTING";
        case FaultToleranceType::UPSTREAM_BACKUP: return "UPSTREAM_BACKUP";
        case FaultToleranceType::AMNESIA: return "AMNESIA";
        case FaultToleranceType::INVALID: return "INVALID";
    }
}
}// namespace NES