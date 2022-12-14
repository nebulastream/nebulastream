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

#ifndef NES_CORE_INCLUDE_UTIL_EXPERIMENTAL_NODETYPEUTILITIES_HPP_
#define NES_CORE_INCLUDE_UTIL_EXPERIMENTAL_NODETYPEUTILITIES_HPP_

#include <Util/Experimental/SpatialType.hpp>

namespace NES::Spatial::Util {

/**
 * @brief this class contains functions to convert a spatial type enum to its equivalent protobuf type and vice versa
 * as well as functions to convert the node type enum to/from string
 */
class NodeTypeUtilities {
  public:
    static Index::Experimental::SpatialType stringToNodeType(const std::string nodeTypeString);

    static Index::Experimental::SpatialType protobufEnumToNodeType(NES::Spatial::Protobuf::SpatialType nodeType);

    static std::string toString(Index::Experimental::SpatialType nodeType);

    static NES::Spatial::Protobuf::SpatialType toProtobufEnum(Index::Experimental::SpatialType nodeType);
};

}// namespace NES::Spatial::Util

#endif// NES_CORE_INCLUDE_UTIL_EXPERIMENTAL_NODETYPEUTILITIES_HPP_
