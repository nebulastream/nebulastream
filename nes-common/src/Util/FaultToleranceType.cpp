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
    } else if (faultToleranceMode == "CH") {
        return FaultToleranceType::CH;
    } else if (faultToleranceMode == "UB") {
        return FaultToleranceType::UB;
    } else if (faultToleranceMode == "AS") {
        return FaultToleranceType::AS;
    } else if (faultToleranceMode == "M") {
        return FaultToleranceType::M;
    } else {
        return FaultToleranceType::INVALID;
    }
}

std::string toString(const FaultToleranceType faultToleranceMode) {
    switch (faultToleranceMode) {
        case FaultToleranceType::NONE: return "NONE";
        case FaultToleranceType::CH: return "CH";
        case FaultToleranceType::UB: return "UB";
        case FaultToleranceType::AS: return "AS";
        case FaultToleranceType::M: return "M";
        case FaultToleranceType::INVALID: return "INVALID";
    }
}
}// namespace NES
