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
#include <Util/FaultTolerancePlacement.hpp>
#include <Util/Logger/Logger.hpp>
#include <string>

namespace NES {
    FaultTolerancePlacement::Value FaultTolerancePlacement::getFromString(const std::string faultToleranceMode) {
        if (faultToleranceMode == "NONE") {
            return FaultTolerancePlacement::NONE;
        } else if (faultToleranceMode == "NAIVE") {
            return FaultTolerancePlacement::NAIVE;
        } else if (faultToleranceMode == "MFTP") {
            return FaultTolerancePlacement::MFTP;
        } else if (faultToleranceMode == "MFTPH") {
            return FaultTolerancePlacement::MFTPH;
        } else {
            NES_THROW_RUNTIME_ERROR("FaultTolerancePlacement not supported " + faultToleranceMode);
        }
    }

    std::string FaultTolerancePlacement::toString(const Value faultToleranceMode) {
        switch (faultToleranceMode) {
            case FaultTolerancePlacement::NONE: return "NONE";
            case FaultTolerancePlacement::NAIVE: return "NAIVE";
            case FaultTolerancePlacement::MFTP: return "MFTP";
            case FaultTolerancePlacement::MFTPH: return "MFTPH";
            case FaultTolerancePlacement::FLINK: return "FLINK";
            case FaultTolerancePlacement::FRONTIER: return "FRONTIER";
        }
    }
}// namespace NES