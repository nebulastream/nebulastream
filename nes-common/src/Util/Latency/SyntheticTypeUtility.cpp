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

#include <Util/Logger/Logger.hpp>
#include <Util/Latency/SyntheticType.hpp>
#include <Util/Latency/SyntheticTypeUtility.hpp>

namespace NES::Synthetic::Util {

Experimental::SyntheticType SyntheticTypeUtility::stringToNodeType(const std::string syntheticTypeString) {
    if (syntheticTypeString == "NC_DISABLED") {
        return Experimental::SyntheticType::NC_DISABLED;
    } else if (syntheticTypeString == "NC_ENABLED") {
        return Experimental::SyntheticType::NC_ENABLED;
    }
    return Experimental::SyntheticType::INVALID;
}

Experimental::SyntheticType SyntheticTypeUtility::protobufEnumToNodeType(NES::Synthetic::Protobuf::SyntheticType syntheticType) {
    switch (syntheticType) {
        case NES::Synthetic::Protobuf::SyntheticType::NC_DISABLED: return Experimental::SyntheticType::NC_DISABLED;
        case NES::Synthetic::Protobuf::SyntheticType::NC_ENABLED: return Experimental::SyntheticType::NC_ENABLED;
        case NES::Synthetic::Protobuf::SyntheticType_INT_MIN_SENTINEL_DO_NOT_USE_: return Experimental::SyntheticType::INVALID;
        case NES::Synthetic::Protobuf::SyntheticType_INT_MAX_SENTINEL_DO_NOT_USE_: return Experimental::SyntheticType::INVALID;
    }
    return Experimental::SyntheticType::INVALID;
}

std::string SyntheticTypeUtility::toString(const Experimental::SyntheticType syntheticType) {
    switch (syntheticType) {
        case Experimental::SyntheticType::NC_DISABLED: return "NC_DISABLED";
        case Experimental::SyntheticType::NC_ENABLED: return "NC_ENABLED";
        case Experimental::SyntheticType::INVALID: return "INVALID";
    }
}

NES::Synthetic::Protobuf::SyntheticType SyntheticTypeUtility::toProtobufEnum(Experimental::SyntheticType syntheticType) {
    switch (syntheticType) {
        case Experimental::SyntheticType::NC_DISABLED: return NES::Synthetic::Protobuf::SyntheticType::NC_DISABLED;
        case Experimental::SyntheticType::NC_ENABLED: return NES::Synthetic::Protobuf::SyntheticType::NC_ENABLED;
        case Experimental::SyntheticType::INVALID:
            NES_FATAL_ERROR("cannot construct protobuf enum from invalid spatial type, exiting");
            exit(EXIT_FAILURE);
    }
}
}// namespace NES::Synthetic::Util