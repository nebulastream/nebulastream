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

#ifndef NES_COMMON_INCLUDE_UTIL_MOBILITY_SPATIALTYPEUTILITY_HPP_
#define NES_COMMON_INCLUDE_UTIL_MOBILITY_SPATIALTYPEUTILITY_HPP_

#include <Util/Mobility/SpatialType.hpp>
#include <WorkerLocation.grpc.pb.h>

namespace NES::Spatial::Util {

/**
 * @brief this class contains functions to convert a spatial type enum to its equivalent protobuf type and vice versa
 * as well as functions to convert the node type enum to/from string
 */
class SpatialTypeUtility {
  public:
    static Experimental::SpatialType stringToNodeType(const std::string spatialTypeString);

    static Experimental::SpatialType protobufEnumToNodeType(NES::Spatial::Protobuf::SpatialType spatialType);

    static std::string toString(Experimental::SpatialType spatialType);

    static NES::Spatial::Protobuf::SpatialType toProtobufEnum(Experimental::SpatialType spatialType);
};

}// namespace NES::Spatial::Util

#endif // NES_COMMON_INCLUDE_UTIL_MOBILITY_SPATIALTYPEUTILITY_HPP_
