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
#include "Util/Logger/Logger.hpp"
#include <Util/Experimental/WorkerSpatialType.hpp>
#include <Util/Experimental/WorkerSpatialTypeUtilities.hpp>
namespace NES::Spatial::Util {

Index::Experimental::WorkerSpatialType WorkerSpatialTypeUtilities::stringToWorkerSpatialType(const std::string spatialTypeString) {
    if (spatialTypeString == "NO_LOCATION") {
        return Index::Experimental::WorkerSpatialType::NO_LOCATION;
    } else if (spatialTypeString == "FIELD_NODE") {
        return Index::Experimental::WorkerSpatialType::FIELD_NODE;
    } else if (spatialTypeString == "MOBILE_NODE") {
        return Index::Experimental::WorkerSpatialType::MOBILE_NODE;
    }
    return Index::Experimental::WorkerSpatialType::INVALID;
}

Index::Experimental::WorkerSpatialType WorkerSpatialTypeUtilities::protobufEnumToWorkerSpatialType(SpatialType spatialType) {
    switch (spatialType) {
        case SpatialType::NO_LOCATION:
            return Index::Experimental::WorkerSpatialType::NO_LOCATION;
        case SpatialType::FIELD_NODE:
            return Index::Experimental::WorkerSpatialType::FIELD_NODE;
        case SpatialType::MOBILE_NODE:
            return Index::Experimental::WorkerSpatialType::MOBILE_NODE;
        case SpatialType_INT_MIN_SENTINEL_DO_NOT_USE_:
            return Index::Experimental::WorkerSpatialType::INVALID;
        case SpatialType_INT_MAX_SENTINEL_DO_NOT_USE_:
            return Index::Experimental::WorkerSpatialType::INVALID;
    }
    return Index::Experimental::WorkerSpatialType::INVALID;
}

std::string WorkerSpatialTypeUtilities::toString(const Index::Experimental::WorkerSpatialType spatialType) {
    switch (spatialType) {
        case Index::Experimental::WorkerSpatialType::NO_LOCATION: return "NO_LOCATION";
        case Index::Experimental::WorkerSpatialType::FIELD_NODE: return "FIELD_NODE";
        case Index::Experimental::WorkerSpatialType::MOBILE_NODE: return "MOBILE_NODE";
        case Index::Experimental::WorkerSpatialType::INVALID: return "INVALID";
    }
}

SpatialType WorkerSpatialTypeUtilities::toProtobufEnum(Index::Experimental::WorkerSpatialType spatialType) {
    switch (spatialType) {
        case Index::Experimental::WorkerSpatialType::NO_LOCATION:
            return SpatialType::NO_LOCATION;
        case Index::Experimental::WorkerSpatialType::FIELD_NODE:
            return SpatialType::FIELD_NODE;
        case Index::Experimental::WorkerSpatialType::MOBILE_NODE:
            return SpatialType::MOBILE_NODE;
        case Index::Experimental::WorkerSpatialType::INVALID:
            NES_FATAL_ERROR("cannot construct protobuf enum from invalid spatial type, exiting")
            exit(EXIT_FAILURE);
    }
}
}// namespace NES