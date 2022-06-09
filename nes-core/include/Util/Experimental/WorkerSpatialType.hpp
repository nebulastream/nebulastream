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

#ifndef NES_UTIL_EXPERIMENTL_WORKERSPATIALTYPE_HPP
#define NES_UTIL_EXPERIMENTL_WORKERSPATIALTYPE_HPP
#include <string>
#include <WorkerLocation.grpc.pb.h>

namespace NES::Spatial::Index::Experimental {
enum class WorkerSpatialType {
    NO_LOCATION = 0, //the worker does not have a known location
    FIELD_NODE = 1, //the worker has a known fixed location that will not change after its creation
    MOBILE_NODE = 2, //the worker runs on a mobile device which might change its location anytime
    INVALID = 3 //no valid worker type
};

WorkerSpatialType stringToWorkerSpatialType(std::string spatialTypeString);

WorkerSpatialType protobufEnumToWorkerSpatialType(SpatialType spatielType);

std::string toString(WorkerSpatialType spatialType);

SpatialType toProtobufEnum(WorkerSpatialType spatialType);


}

#endif//NES_UTIL_EXPERIMENTL_WORKERSPATIALTYPE_HPP
