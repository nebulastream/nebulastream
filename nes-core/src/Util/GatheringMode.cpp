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

#include <Util/GatheringMode.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES {

std::string GatheringMode::toString(const Value gatheringMode) {
    switch (gatheringMode) {
        case GatheringMode::FREQUENCY_MODE: return "frequency";
        case GatheringMode::INGESTION_RATE_MODE: return "ingestionrate";
        case GatheringMode::ADAPTIVE_MODE: return "adaptive";
    }
}

GatheringMode::Value GatheringMode::getFromString(const std::string gatheringMode) {
    if (gatheringMode == "frequency") {
        return GatheringMode::FREQUENCY_MODE;
    } else if (gatheringMode == "ingestionrate") {
        return GatheringMode::INGESTION_RATE_MODE;
    }   else if (gatheringMode == "adaptive") {
        return GatheringMode::ADAPTIVE_MODE;
    } else {
        NES_THROW_RUNTIME_ERROR("gatheringMode not supported " << gatheringMode);
    }
}
}// namespace NES