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
namespace NES::Spatial::Index::Experimental {
WorkerSpatialType stringToWorkerSpatialType(const std::string spatialTypeString) {
    if (spatialTypeString == "NO_LOCATION") {
        return WorkerSpatialType::NO_LOCATION;
    } else if (spatialTypeString == "FIELD_NODE") {
        return WorkerSpatialType::FIELD_NODE;
    } else if (spatialTypeString == "MOBILE_NODE") {
        return WorkerSpatialType::MOBILE_NODE;
    }
    return WorkerSpatialType::INVALID;
}

WorkerSpatialType protobufEnumToWorkerSpatialType(SpatialType spatialType) {
    if (spatialType == SpatialType::NO_LOCATION) {
        return WorkerSpatialType::NO_LOCATION;
    }
    if (spatialType == SpatialType::FIELD_NODE ) {
        return WorkerSpatialType::FIELD_NODE;
    }
    if (spatialType == SpatialType::MOBILE_NODE) {
        return WorkerSpatialType::MOBILE_NODE;
    }
    return WorkerSpatialType::INVALID;
}

std::string toString(const WorkerSpatialType spatialType) {
    switch (spatialType) {
        case WorkerSpatialType::NO_LOCATION: return "NO_LOCATION";
        case WorkerSpatialType::FIELD_NODE: return "FIELD_NODE";
        case WorkerSpatialType::MOBILE_NODE: return "MOBILE_NODE";
        case WorkerSpatialType::INVALID: return "INVALID";
            }
        }

SpatialType toProtobufEnum(WorkerSpatialType spatialType) {
    switch (spatialType) {
        case WorkerSpatialType::NO_LOCATION:
            return SpatialType::NO_LOCATION;
        case WorkerSpatialType::FIELD_NODE:
            return SpatialType::FIELD_NODE;
        case WorkerSpatialType::MOBILE_NODE:
            return SpatialType::MOBILE_NODE;
        case WorkerSpatialType::INVALID:
            NES_FATAL_ERROR("cannot construct protobuf enum from invalid spatial type, exiting")
            exit(EXIT_FAILURE);
    }
}
}// namespace NES::Spatial::Mobility::Experimental
